package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.HttpStatus
import org.springframework.stereotype.Service
import org.springframework.web.client.HttpClientErrorException
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import org.springframework.http.HttpHeaders
import ru.quipy.common.utils.CompositeRateLimiter
import ru.quipy.common.utils.LeakingBucketRateLimiter
import ru.quipy.common.utils.TokenBucketRateLimiter
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit

@Service
class OrderPayer {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentExecutor = ThreadPoolExecutor(
        5000,
        5000,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(5000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    )

    val rateLimiter = TokenBucketRateLimiter(6, 11, 1, TimeUnit.SECONDS)

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        if (paymentExecutor.queue.remainingCapacity() == 0) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor queue is full",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        if (!rateLimiter.tick()) {
            throw HttpClientErrorException.create(
                HttpStatus.TOO_MANY_REQUESTS,
                "Payment executor can't acquire a token",
                HttpHeaders.EMPTY,
                ByteArray(0),
                null
            )
        }

        val createdAt = System.currentTimeMillis()

        paymentExecutor.submit {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment ${createdEvent.paymentId} for order $orderId created.")

            paymentService.submitPaymentRequest(paymentId, amount, createdAt, deadline)
        }
        return createdAt
    }
}