package ru.quipy.payments.logic

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.TokenBucketRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import org.springframework.http.HttpHeaders
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.RejectedExecutionException

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
        private const val QUEUE_SIZE_MULTIPLIER = 11
        private const val MAX_SCHEDULED_TASKS = 4000
        private const val CORE_POOL_SIZE = 100
        private const val MAX_POOL_SIZE = 100
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val queueSize = CORE_POOL_SIZE * QUEUE_SIZE_MULTIPLIER

    private val rejectedCountingPolicy = RejectedExecutionHandler { r, executor ->
        throw RejectedExecutionException("Task rejected from $executor")
    }

    private val immediateExecutor = ThreadPoolExecutor(
        CORE_POOL_SIZE, MAX_POOL_SIZE,
        0L, TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(queueSize),
        NamedThreadFactory("order-immediate-executor"),
        rejectedCountingPolicy
    )

    private val scheduledExecutor = ScheduledThreadPoolExecutor(
        10,
        NamedThreadFactory("order-scheduled-executor")
    ).apply {
        removeOnCancelPolicy = true
        rejectedExecutionHandler = RejectedExecutionHandler { r, executor ->
            logger.error("Scheduled task rejected: $r")
        }
    }

    private val scheduledTasksSemaphore = Semaphore(MAX_SCHEDULED_TASKS)

    val rateLimiter = TokenBucketRateLimiter(4000, 5000, 1, TimeUnit.SECONDS)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (!rateLimiter.tick()) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor can't acquire a token",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        if (immediateExecutor.queue.size >= queueSize) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        val createdAt = System.currentTimeMillis()

        try {
            immediateExecutor.submit {
                val createdEvent = paymentESService.create {
                    it.create(paymentId, orderId, amount)
                }
                logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

                retryAsync(paymentId, amount, createdAt, deadline, attempt = 1)
            }
        } catch (e: RejectedExecutionException) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor overloaded, please retry later",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        return createdAt
    }

    private fun retryAsync(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            logger.warn("Payment $paymentId attempt #$attempt aborted: deadline exceeded")
            return
        }

        val paymentRequest = paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        val start = System.currentTimeMillis()

        paymentRequest
            .orTimeout(timeLeft, TimeUnit.MILLISECONDS)
            .whenCompleteAsync({ success, error ->
                val elapsed = System.currentTimeMillis() - start
                when {
                    error != null -> {
                        logger.warn(
                            "Payment $paymentId attempt #$attempt failed: ${error.message}, " +
                                    "timeLeft=${deadline - System.currentTimeMillis()}ms, elapsed=${elapsed}ms"
                        )
                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }

                    success == true -> {
                        logger.info("Payment $paymentId attempt #$attempt succeeded, elapsed=${elapsed}ms")
                    }

                    else -> {
                        logger.info("Payment $paymentId attempt #$attempt returned failure, elapsed=${elapsed}ms")
                        scheduleRetry(paymentId, amount, createdAt, deadline, attempt)
                    }
                }
            }, immediateExecutor)
    }

    private fun scheduleRetry(
        paymentId: UUID,
        amount: Int,
        createdAt: Long,
        deadline: Long,
        attempt: Int
    ) {
        val now = System.currentTimeMillis()
        val timeLeft = deadline - now

        if (timeLeft <= 0) {
            return
        }

        if (!scheduledTasksSemaphore.tryAcquire()) {
            logger.error("Too many scheduled retries for payment $paymentId, dropping retry #${attempt + 1}")
            return
        }

        try {
            scheduledExecutor.schedule(
                {
                    try {
                        retryAsync(paymentId, amount, createdAt, deadline, attempt + 1)
                    } finally {
                        scheduledTasksSemaphore.release()
                    }
                },
                100L,
                TimeUnit.MILLISECONDS
            )
        } catch (e: RejectedExecutionException) {
            scheduledTasksSemaphore.release()
            logger.error("Failed to schedule retry for payment $paymentId attempt #${attempt + 1}: queue full")
        }
    }
}
