package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeout
import okhttp3.Call
import okhttp3.Callback
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit
import okhttp3.ConnectionPool
import okhttp3.Dispatcher
import okhttp3.Protocol
import okhttp3.Response
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import java.io.IOException
import java.util.concurrent.CompletableFuture
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.RejectedExecutionHandler
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.ThreadPoolExecutor


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {
    private val incomingRegCounted: Counter = Counter.builder("incoming started")
        .description("incoming started request")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val incomingFinishedReqCounted: Counter = Counter.builder("incoming finished")
        .description("incoming finished request")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val outgoingReqCounted: Counter = Counter.builder("outgoing started")
        .description("outgoing started request")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val outgoingFinishedReqCounted: Counter = Counter.builder("outgoing finished")
        .description("outgoing finished request")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val retryCounter: Counter = Counter.builder("outgoing_request_retries")
        .description("Number of retries for outgoing requests")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val outgoingRequestProcessingTime = DistributionSummary
        .builder("outgoing_request_processing_time")
        .description("Outgoing request latency")
        .publishPercentiles(0.5, 0.75, 0.9, 0.95, 0.99)
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val rejectedTasksCounter = Counter.builder("executor tasks rejected")
        .description("Number of tasks rejected due to queue overflow")
        .tag("account", properties.accountName)
        .register(meterRegistry)

    private val rejectedCountingPolicy = RejectedExecutionHandler { r, executor ->
        rejectedTasksCounter.increment()
        if (!executor.isShutdown) {
            r.run()
        }
    }

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRIES: Int = 3
        const val HEDGE_DELAY_MS: Long = 120
    }

    private val hedgeExecutor = ScheduledThreadPoolExecutor(4)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val paymentExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        NamedThreadFactory("payment-submission-executor"),
        rejectedCountingPolicy
    ).apply {
        allowCoreThreadTimeOut(false)
    }

    private val okHttpExecutor = ThreadPoolExecutor(
        parallelRequests,
        parallelRequests,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(parallelRequests * 2),
        NamedThreadFactory("okhttp-dispatcher-executor"),
        rejectedCountingPolicy
    )

    // IF the request is not completed in 1.5 seconds, then we throw the IOException
    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis(1000L))
        .dispatcher(Dispatcher(okHttpExecutor).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()

    init {
        meterRegistry.gauge("payment.executor.queue.size", paymentExecutor.queue) { it.size.toDouble() }
        meterRegistry.gauge("okhttp.dispatcher.queue.size", okHttpExecutor.queue) { it.size.toDouble() }
    }

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val retryAfterMillis: Long =
        (((requestAverageProcessingTime.toMillis().toDouble()) / 2.0) * (rateLimitPerSec / 7.0)).toLong()

    fun recordRetry() = retryCounter.increment()
    fun recordOutgoingRequest(processingTime: Long) = outgoingRequestProcessingTime.record(processingTime.toDouble())

    private val circuitBreaker = CircuitBreakerRegistry.of(
        CircuitBreakerConfig.custom()
            .failureRateThreshold(50f)
            .slowCallRateThreshold(50f)
            .slowCallDurationThreshold(Duration.ofMillis(500))
            .minimumNumberOfCalls(10)
            .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.COUNT_BASED)
            .slidingWindowSize(20)
            .waitDurationInOpenState(Duration.ofSeconds(2))
            .permittedNumberOfCallsInHalfOpenState(3)
            .build()
    ).circuitBreaker(properties.accountName)

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        return performPaymentAsyncWithRetry(paymentId, amount, paymentStartedAt, deadline, 1)
    }

    private fun performPaymentAsyncWithRetry(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long,
        attempt: Int
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId, attempt $attempt")
        incomingRegCounted.increment()

        val future = CompletableFuture<Boolean>()
        val transactionId = UUID.randomUUID()

        val protectedFuture = executeWithHedging(paymentId, amount, transactionId)

        protectedFuture.whenComplete { result, throwable ->
            val now = now()
            if (throwable != null) {
                logger.error("[$accountName] Payment attempt $attempt failed for $paymentId", throwable)
                if (now < deadline && attempt < MAX_RETRIES) {
                    val delayMs = calculateBackoff(attempt)
                    logger.info("[$accountName] Scheduling retry $attempt for $paymentId in ${delayMs}ms")
                    hedgeExecutor.schedule({
                        val retryFuture = performPaymentAsyncWithRetry(
                            paymentId,
                            amount,
                            paymentStartedAt,
                            deadline,
                            attempt + 1
                        )
                        retryFuture.whenComplete { retryResult, retryThrowable ->
                            if (retryThrowable != null) {
                                future.completeExceptionally(retryThrowable)
                            } else {
                                future.complete(retryResult)
                            }
                        }
                    }, delayMs, TimeUnit.MILLISECONDS)
                } else {
                    future.completeExceptionally(throwable)
                }
            } else {
                future.complete(result)
            }
            incomingFinishedReqCounted.increment()
        }

        return future
    }

    private fun calculateBackoff(attempt: Int): Long {
        val base = 100L
        return (base * Math.pow(2.0, (attempt - 1).toDouble())).toLong()
    }

    private fun executeWithHedging(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID
    ): CompletableFuture<Boolean> {
        val future = CompletableFuture<Boolean>()
        val startTime = now()
        val request = createRequest(paymentId, amount, transactionId)

        val firstFuture = sendRequestProtected(request, paymentId, transactionId, startTime)
        firstFuture.whenComplete { result, throwable ->
            if (!future.isDone) {
                if (throwable != null) {
                    future.completeExceptionally(throwable)
                } else {
                    future.complete(result)
                }
            }
        }

        hedgeExecutor.schedule({
            if (!future.isDone) {
                retryCounter.increment()
                logger.warn("[$accountName] Hedged request for payment $paymentId")
                val hedgedFuture = sendRequestProtected(request, paymentId, transactionId, startTime)
                hedgedFuture.whenComplete { result, throwable ->
                    if (!future.isDone) {
                        if (throwable != null) {
                            future.completeExceptionally(throwable)
                        } else {
                            future.complete(result)
                        }
                    }
                }
            }
        }, HEDGE_DELAY_MS, TimeUnit.MILLISECONDS)

        return future
    }

    private fun createRequest(paymentId: UUID, amount: Int, transactionId: UUID): Request {
        return Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()
    }

    private fun sendRequestProtected(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
        startTime: Long
    ): CompletableFuture<Boolean> {
        return circuitBreaker.executeCompletionStage {
            sendRequestInternal(request, paymentId, transactionId, startTime)
        }.toCompletableFuture()
    }

    private fun sendRequestInternal(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
        startTime: Long
    ): CompletableFuture<Boolean> {
        val future = CompletableFuture<Boolean>()

        try {
            semaphore.acquire()
            rateLimiter.tickBlocking()
        } catch (e: InterruptedException) {
            future.completeExceptionally(e)
            return future
        }

        outgoingReqCounted.increment()
        logger.info("[$accountName] Sending request payment=$paymentId txId=$transactionId")

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    response.use { resp ->
                        val body = mapper.readValue(
                            resp.body?.string(),
                            ExternalSysResponse::class.java
                        )
                        logger.info(
                            "[$accountName] Response received payment=$paymentId txId=$transactionId success=${body.result}"
                        )
                        recordOutgoingRequest(now() - startTime)
                        future.complete(body.result)
                    }
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] Error processing response payment=$paymentId txId=$transactionId",
                        e
                    )
                    future.completeExceptionally(e)
                } finally {
                    semaphore.release()
                    outgoingFinishedReqCounted.increment()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error(
                    "[$accountName] Request failed payment=$paymentId txId=$transactionId",
                    e
                )
                future.completeExceptionally(e)
                semaphore.release()
                outgoingFinishedReqCounted.increment()
            }
        })

        return future
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()