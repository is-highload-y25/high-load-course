package ru.quipy.payments.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import ru.quipy.common.utils.LeakingBucketRateLimiter
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*

@Configuration
class PaymentAccountsConfig {
    companion object {
        private val logger = LoggerFactory.getLogger(PaymentAccountsConfig::class.java)
        private val PAYMENT_PROVIDER_HOST_PORT: String = "localhost:1234"
        private val javaClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .build()
        private val mapper = ObjectMapper().registerKotlinModule().registerModules(JavaTimeModule())
    }

    private val allowedAccounts = setOf("acc-3")

    @Bean
    fun accountAdapters(paymentService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>): List<PaymentExternalSystemAdapter> {
        return try {
            val request = HttpRequest.newBuilder()
                .uri(URI("http://${PAYMENT_PROVIDER_HOST_PORT}/external/accounts?serviceName=onlineStore"))
                .GET()
                .build()

            val resp = javaClient.send(request, HttpResponse.BodyHandlers.ofString())

            logger.info("\nPayment accounts list:")
            mapper.readValue<List<PaymentAccountProperties>>(
                resp.body(),
                mapper.typeFactory.constructCollectionType(List::class.java, PaymentAccountProperties::class.java)
            )
                .filter {
                    it.accountName in allowedAccounts
                }.onEach { logger.info(it.toString()) }
                .map { PaymentExternalSystemAdapterImpl(it, paymentService) }
        } catch (e: Exception) {
            logger.warn("Failed to fetch accounts from external service, using default configuration", e)
            // Создаем дефолтную конфигурацию для acc-3
            listOf(
                PaymentExternalSystemAdapterImpl(
                    PaymentAccountProperties(
                        serviceName = "onlineStore",
                        accountName = "acc-3",
                        parallelRequests = 30,
                        rateLimitPerSec = 10,
                        price = 30,
                        averageProcessingTime = Duration.ofSeconds(1),
                        enabled = true
                    ),
                    paymentService
                )
            )
        }
    }

    @Bean
    fun rateLimiter(): LeakingBucketRateLimiter {
        return LeakingBucketRateLimiter(
            rate = 10,  // 10 запросов в секунду
            window = Duration.ofSeconds(1),
            bucketSize = 30  // Максимум параллельных запросов
        )
    }
}