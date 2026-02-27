package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.*
import okhttp3.RequestBody.Companion.toRequestBody
import org.slf4j.LoggerFactory
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentProviderHostPort: String,
    private val token: String,
    meterRegistry: MeterRegistry
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)
        val emptyBody = ByteArray(0).toRequestBody(null)
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val parallelRequests = properties.parallelRequests
    private val inFlightLimiter = Semaphore(parallelRequests)

    private val incomingStarted = Counter.builder("incoming.started")
        .tag("account", accountName)
        .register(meterRegistry)

    private val incomingFinished = Counter.builder("incoming.finished")
        .tag("account", accountName)
        .register(meterRegistry)

    private val outgoingStarted = Counter.builder("outgoing.started")
        .tag("account", accountName)
        .register(meterRegistry)

    private val outgoingFinished = Counter.builder("outgoing.finished")
        .tag("account", accountName)
        .register(meterRegistry)

    private val outgoingLatency = DistributionSummary
        .builder("outgoing.latency")
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val client = OkHttpClient.Builder()
        .readTimeout(Duration.ofMillis(1000))
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectionPool(ConnectionPool(parallelRequests, 20, TimeUnit.SECONDS))
        .build()

    override fun performPaymentAsync(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        deadline: Long
    ): CompletableFuture<Boolean> {

        incomingStarted.increment()

        if (!inFlightLimiter.tryAcquire()) {
            incomingFinished.increment()
            logger.warn("[$accountName] Too many in-flight requests, rejecting payment $paymentId")
            return CompletableFuture.completedFuture(false)
        }

        val transactionId = UUID.randomUUID()
        val future = CompletableFuture<Boolean>()

        val request = Request.Builder()
            .url(
                "http://$paymentProviderHostPort/external/process" +
                        "?serviceName=$serviceName" +
                        "&token=$token" +
                        "&accountName=$accountName" +
                        "&transactionId=$transactionId" +
                        "&paymentId=$paymentId" +
                        "&amount=$amount"
            )
            .post(emptyBody)
            .build()

        val startTime = now()
        outgoingStarted.increment()

        client.newCall(request).enqueue(object : Callback {
            override fun onResponse(call: Call, response: Response) {
                try {
                    response.use { resp ->
                        val body = try {
                            mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: Exception) {
                            logger.error("[$accountName] Invalid response for payment $paymentId", e)
                            ExternalSysResponse(
                                transactionId.toString(),
                                paymentId.toString(),
                                false,
                                e.message
                            )
                        }

                        future.complete(body.result)
                    }
                } catch (e: Exception) {
                    logger.error("[$accountName] Error processing response for payment $paymentId", e)
                    future.complete(false)
                } finally {
                    finish(startTime)
                }
            }

            override fun onFailure(call: Call, e: IOException) {
                try {
                    future.complete(false)
                } catch (ex: Exception) {
                    logger.error("[$accountName] Failure handling error for payment $paymentId", ex)
                    future.complete(false)
                } finally {
                    finish(startTime)
                }
            }

            private fun finish(startTime: Long) {
                inFlightLimiter.release()
                outgoingFinished.increment()
                incomingFinished.increment()
                outgoingLatency.record((now() - startTime).toDouble())
            }
        })

        return future
    }

    override fun price() = properties.price
    override fun isEnabled() = properties.enabled
    override fun name() = accountName
}

fun now() = System.currentTimeMillis()
