package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
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
import java.util.concurrent.ScheduledThreadPoolExecutor


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

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()

        const val MAX_RETRIES: Int = 3
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val paymentExecutor = object : ScheduledThreadPoolExecutor(
        5000,
        NamedThreadFactory("payment-submission-executor")
    ) {
        init {
            setMaximumPoolSize(5000)
            setKeepAliveTime(0L, TimeUnit.MILLISECONDS)
            setRejectedExecutionHandler(CallerBlockingRejectedExecutionHandler())
            removeOnCancelPolicy = true
        }
    }

    // IF the request is not completed in 1.5 seconds, then we throw the IOException
    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofSeconds(30))
        .dispatcher( Dispatcher(Executors.newFixedThreadPool(10000)).apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()

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

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

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

        outgoingReqCounted.increment()
        semaphore.acquire()
        rateLimiter.tickBlocking()

        val request = Request.Builder().run {
            url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
            post(emptyBody)
        }.build()

        try {
            client.newCall(request).enqueue(object : Callback {
                override fun onResponse(call: Call, response: Response) {
                    try {
                        response.use { resp ->
                            val body = try {
                                mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                            } catch (e: Exception) {
                                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${resp.code}, reason: ${resp.body?.string()}")
                                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                            }

                            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                            paymentESService.update(paymentId) {
                                it.logProcessing(body.result, now(), transactionId, reason = body.message)
                            }

                            val processingTime = now() - startTime
                            recordOutgoingRequest(processingTime)

                            future.complete(body.result)
                        }
                    } catch (e: Exception) {
                        logger.error("[$accountName] Error processing response for payment $paymentId", e)
                        future.complete(false)
                    } finally {
                        semaphore.release()
                        outgoingFinishedReqCounted.increment()
                        incomingFinishedReqCounted.increment()
                    }
                }

                override fun onFailure(call: Call, e: IOException) {
                    try {
                        when (e) {
                            is SocketTimeoutException -> {
                                logger.error("[$accountName] Payment socket timeout for txId: $transactionId, payment: $paymentId", e)
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Socket timeout.")
                                }
                            }

                            is java.io.InterruptedIOException -> {
                                logger.error("[$accountName] Payment interrupted (timeout/cancel) for txId: $transactionId, payment: $paymentId", e)
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "Interrupted I/O (timeout or cancel).")
                                }
                            }

                            else -> {
                                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = e.message)
                                }
                            }
                        }

                        future.complete(false)
                    } catch (ex: Exception) {
                        logger.error("[$accountName] Error in onFailure for payment $paymentId", ex)
                        future.complete(false)
                    } finally {
                        semaphore.release()
                        outgoingFinishedReqCounted.increment()
                        incomingFinishedReqCounted.increment()
                    }
                }
            })
        } catch (e: Exception) {
            logger.error("[$accountName] Error preparing request for payment $paymentId", e)
            future.complete(false)
            semaphore.release()
            outgoingFinishedReqCounted.increment()
            incomingFinishedReqCounted.increment()
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()