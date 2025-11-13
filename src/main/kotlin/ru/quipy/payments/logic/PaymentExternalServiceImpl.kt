package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
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
import kotlin.math.ceil


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

    // IF the request is not completed in 1.5 seconds, then we throw the IOException
    private val client = OkHttpClient.Builder()
        //.callTimeout(1500, TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val semaphore = Semaphore(parallelRequests)
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val retryAfterMillis: Long =
        (((requestAverageProcessingTime.toMillis().toDouble()) / 2.0) * (rateLimitPerSec / 7.0)).toLong()

    fun recordRetry() = retryCounter.increment()
    fun recordOutgoingRequest(processingTime: Long) = outgoingRequestProcessingTime.record(processingTime.toDouble())

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        incomingRegCounted.increment()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        semaphore.acquire()

        var attempt = 1

        while (attempt <= MAX_RETRIES) {
            val startTime = now()
            try {
                outgoingReqCounted.increment()
                val request = Request.Builder().run {
                    url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                rateLimiter.tickBlocking()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }

                    val processingTime = now() - startTime
                    recordOutgoingRequest(processingTime)

                    if (body.result) {
                        return
                    } else if (attempt < MAX_RETRIES) {
                        logger.warn("[$accountName] Retry #$attempt for payment $paymentId after ${retryAfterMillis}ms")
                        recordRetry()
                        Thread.sleep(retryAfterMillis)
                    }
                }
            } catch (e: Exception) {
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

                if (attempt < MAX_RETRIES) {
                    logger.warn("[$accountName] Retry #$attempt for payment $paymentId after ${retryAfterMillis}ms")
                    recordRetry()
                    Thread.sleep(retryAfterMillis)
                }

                attempt++
            } finally {
                semaphore.release()
                outgoingFinishedReqCounted.increment()
                incomingFinishedReqCounted.increment()
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

fun now() = System.currentTimeMillis()