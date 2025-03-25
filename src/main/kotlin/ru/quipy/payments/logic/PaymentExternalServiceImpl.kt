package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests
    private var waitTime: Long = 1000
    private val threadCount = 50 // ratePerSecond / (1 / averageProcessingTime) = 100 / (1 / 0.5)

    private val httpClient = OkHttpClient.Builder()
        .callTimeout((requestAverageProcessingTime.toMillis() * 1.5).toLong(), TimeUnit.MILLISECONDS)
        .build()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong() - 1, Duration.ofSeconds(1))
    private val window = OngoingWindow(parallelRequests)
    private val pool = ThreadPoolExecutor(
        threadCount,
        threadCount,
        //20000.toLong(),
        0.toLong(),
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(5000)
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
        window.acquire()

        pool.submit {
            try {
                val req = Request.Builder().run {
                    url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                    post(emptyBody)
                }.build()

                // пробуем отправить платеж пока не закончится время
                while (now() < deadline) {
                    try {
                        rateLimiter.tickBlocking()
                        if (now() + requestAverageProcessingTime.toMillis() >= deadline) {
                            logger.warn("[$accountName] No time left for: $paymentId")
                            break
                        }

                        val resp = httpClient.newCall(req).execute()
                        resp.use { response ->
                            if (response.isSuccessful) {
                                val responseString = response.body?.string() ?: ""
                                val result = try {
                                    mapper.readValue(responseString, ExternalSysResponse::class.java)
                                } catch (e: Exception) {
                                    ExternalSysResponse(
                                        transactionId.toString(),
                                        paymentId.toString(),
                                        false,
                                        e.message
                                    )
                                }
                                logger.info("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, result: ${result.result}, message: ${result.message}")
                                paymentESService.update(paymentId) {
                                    it.logProcessing(result.result, now(), transactionId, reason = result.message)
                                }
                                return@submit
                            } else if (response.code == 429 || response.code == 503) {
                                val retryHeader = response.headers["Retry-After"]
                                if (!retryHeader.isNullOrEmpty()) {
                                    waitTime = retryHeader.toLong() * 1000
                                }
                            } else {
                                logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId, code: ${response.code}")
                                paymentESService.update(paymentId) {
                                    it.logProcessing(false, now(), transactionId, reason = "HTTP ${response.code}")
                                }
                                return@submit
                            }
                        }
                    } catch (e: SocketTimeoutException) {
                        logger.error("[$accountName] Timeout: tx=$transactionId, payment=$paymentId", e)
                    } catch (e: Exception) {
                        logger.error("[$accountName] Unexpected error: tx=$transactionId, payment=$paymentId", e)
                    }

                    if (now() + waitTime >= deadline) {
                        break
                    }

                    Thread.sleep(waitTime)
                }
                logger.error("[$accountName] Can't squeeze more retries into the deadline for txId: $transactionId, payment: $paymentId")
                paymentESService.update(paymentId) {
                    it.logProcessing(
                        false,
                        now(),
                        transactionId,
                        reason = "Can't squeeze more retries into the deadline"
                    )
                }
            } finally {
                window.release()
            }
        }

    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()