package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.LinkedBlockingQueue
import ru.quipy.common.utils.LeakingBucketRateLimiter

@Service
class PaymentSystemImpl(
    private val paymentAccounts: List<PaymentExternalSystemAdapter>,
    private val rateLimiter: LeakingBucketRateLimiter
) : PaymentService {
    companion object {
        val logger = LoggerFactory.getLogger(PaymentSystemImpl::class.java)
    }

    private val paymentQueue = LinkedBlockingQueue<PaymentRequest>()
    private val paymentExecutor = Executors.newFixedThreadPool(
        30,  // Максимум параллельных запросов
        NamedThreadFactory("payment-processor")
    )

    init {
        // Запускаем обработчик очереди
        startQueueProcessor()
    }

    private fun startQueueProcessor() {
        paymentExecutor.submit {
            while (true) {
                try {
                    val request = paymentQueue.take()
                    processPaymentRequest(request)
                } catch (e: Exception) {
                    logger.error("Error processing payment request", e)
                }
            }
        }
    }

    private fun processPaymentRequest(request: PaymentRequest) {
        val startTime = System.currentTimeMillis()
        
        // Ждем разрешения от rate limiter
        while (!rateLimiter.tick()) {
            Thread.sleep(10)
        }

        // Проверяем, не истек ли дедлайн
        if (System.currentTimeMillis() >= request.deadline) {
            logger.warn("Payment request ${request.paymentId} expired")
            return
        }

        // Отправляем запрос через первый доступный аккаунт
        paymentAccounts.firstOrNull()?.let { account ->
            account.performPaymentAsync(
                request.paymentId,
                request.amount,
                request.paymentStartedAt,
                request.deadline
            )
        }
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val request = PaymentRequest(paymentId, amount, paymentStartedAt, deadline)
        paymentQueue.offer(request)
    }

    data class PaymentRequest(
        val paymentId: UUID,
        val amount: Int,
        val paymentStartedAt: Long,
        val deadline: Long
    )
}
