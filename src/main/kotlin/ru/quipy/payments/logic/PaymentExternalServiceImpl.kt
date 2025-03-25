package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()

        private const val MAX_RETRIES = 3
        private const val RETRY_DELAY_MS = 1000L
        private const val MAX_RETRY_DELAY_MS = 5000L
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val client = OkHttpClient.Builder().build()
    private val ongoingWindow = NonBlockingOngoingWindow(parallelRequests)
    private val rateLimiter = SlidingWindowRateLimiter(
        rateLimitPerSec.toLong(), Duration.ofSeconds(1L)
    )

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()
        logger.info("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        var retryCount = 0
        var lastError: Exception? = null
        var lastResponse: ExternalSysResponse? = null
        var shouldContinue = true

        while (retryCount <= MAX_RETRIES && shouldContinue) {
            try {
                if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                    return
                }

                while(ongoingWindow.putIntoWindow() !is NonBlockingOngoingWindow.WindowResponse.Success) {
                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        return
                    }
                }

                while(!rateLimiter.tick()) {
                    if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                        }
                        return
                    }
                }

                val request = Request.Builder().run {
                    url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                client.newCall(request).execute().use { response ->
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                    }

                    lastResponse = body
                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    if (body.result) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(true, now(), transactionId, reason = body.message)
                        }
                        shouldContinue = false
                    } else {
                        lastError = Exception(body.message ?: "Payment failed")
                        retryCount++
                        if (retryCount <= MAX_RETRIES) {
                            val delay = calculateRetryDelay(retryCount)
                            logger.info("[$accountName] Retrying payment $paymentId (attempt ${retryCount + 1}/$MAX_RETRIES) after ${delay}ms")
                            Thread.sleep(delay)
                        } else {
                            shouldContinue = false
                        }
                    }
                }
            } catch (e: Exception) {
                lastError = e
                retryCount++
                if (retryCount <= MAX_RETRIES) {
                    val delay = calculateRetryDelay(retryCount)
                    logger.info("[$accountName] Retrying payment $paymentId (attempt ${retryCount + 1}/$MAX_RETRIES) after ${delay}ms due to: ${e.message}")
                    Thread.sleep(delay)
                } else {
                    shouldContinue = false
                }
            } finally {
                ongoingWindow.releaseWindow()
            }
        }

        // Если все попытки исчерпаны, регистрируем неудачу
        val finalReason = when (lastError) {
            is SocketTimeoutException -> "Request timeout after $MAX_RETRIES attempts"
            else -> "Payment failed after $MAX_RETRIES attempts: ${lastError?.message}"
        }

        paymentESService.update(paymentId) {
            it.logProcessing(false, now(), transactionId, reason = finalReason)
        }
    }

    private fun calculateRetryDelay(retryCount: Int): Long {
        // Экспоненциальная задержка с случайным отклонением
        val baseDelay = RETRY_DELAY_MS * (1L shl (retryCount - 1))
        val maxDelay = minOf(baseDelay, MAX_RETRY_DELAY_MS.toLong())
        val randomFactor = 0.5 + Math.random() // случайное число от 0.5 до 1.5
        return (maxDelay * randomFactor).toLong()
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()