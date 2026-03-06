package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
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

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()

        incomingRegCounted.increment()

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        CompletableFuture.runAsync({
            try {
                performPaymentSingleAttempt(paymentId, amount, transactionId, future)
            } catch (e: Exception) {
                future.complete(false)
                outgoingFinishedReqCounted.increment()
                incomingFinishedReqCounted.increment()
            }
        }, paymentExecutor)

        return future
    }

    private fun performPaymentSingleAttempt(
        paymentId: UUID,
        amount: Int,
        transactionId: UUID,
        future: CompletableFuture<Boolean>
    ) {

        val startTime = now()

        val request = Request.Builder()
            .url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            .post(emptyBody)
            .build()

        sendRequest(request, paymentId, transactionId, startTime, future)

        hedgeExecutor.schedule({

            if (!future.isDone) {
                recordRetry()
                logger.warn("[$accountName] Hedged request for payment $paymentId")

                sendRequest(request, paymentId, transactionId, startTime, future)
            }

        }, 120, TimeUnit.MILLISECONDS)
    }

    private fun sendRequest(
        request: Request,
        paymentId: UUID,
        transactionId: UUID,
        startTime: Long,
        future: CompletableFuture<Boolean>
    ) {
        logger.info("[$accountName] Sending request payment=$paymentId txId=$transactionId")
        outgoingReqCounted.increment()
        semaphore.acquire()
        rateLimiter.tickBlocking()
        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    if (future.isDone) return
                    response.use { resp ->
                        val body = mapper.readValue(
                            resp.body?.string(),
                            ExternalSysResponse::class.java
                        )
                        logger.info(
                            "[$accountName] Response received payment=$paymentId txId=$transactionId success=${body.result}"
                        )
                        val processingTime = now() - startTime
                        recordOutgoingRequest(processingTime)

                        future.complete(body.result)
                    }
                } catch (e: Exception) {
                    logger.error(
                        "[$accountName] Error processing response payment=$paymentId txId=$transactionId",
                        e
                    )
                    if (!future.isDone) future.complete(false)
                } finally {
                    semaphore.release()
                    outgoingFinishedReqCounted.increment()
                    incomingFinishedReqCounted.increment()
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                logger.error(
                    "[$accountName] Request failed payment=$paymentId txId=$transactionId",
                    e
                )
                if (!future.isDone) {
                    future.complete(false)
                }
                semaphore.release()
                outgoingFinishedReqCounted.increment()
                incomingFinishedReqCounted.increment()
            }
        })
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()