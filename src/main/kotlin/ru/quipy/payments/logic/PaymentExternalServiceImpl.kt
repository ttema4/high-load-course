package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.DistributionSummary
import io.micrometer.core.instrument.MeterRegistry
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.InterruptedIOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val meterRegistry: MeterRegistry
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
    private val processingReserveMillis = maxOf(100L, requestAverageProcessingTime.toMillis() / 2)
    private val processingTimeOneCall = (1_150L + rateLimitPerSec - 1) / rateLimitPerSec
    private val maxAttempts = 4
    private val attemptTimeoutMillis = 1500L
    private val rateLimiter = SlidingWindowRateLimiter(rate = 1, window = Duration.ofMillis(processingTimeOneCall))
    // Ограничитель количества параллельных запросов к внешнему платежному движку
    private val inFlightRequests = Semaphore(parallelRequests, true)
    private val retriesCounter = meterRegistry.counter("shop.payment.retries", "account", accountName, "service", serviceName)
    private val httpLatencySummary = DistributionSummary.builder("shop.payment.http.latency.ms")
        .description("Duration of outgoing payment HTTP requests in milliseconds")
        .tag("account", accountName)
        .tag("service", serviceName)
        .publishPercentiles(0.50, 0.85, 0.90, 0.95, 0.99)
        .register(meterRegistry)
    private val client = OkHttpClient.Builder().build()

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val remainingMillis = deadline - now() - processingReserveMillis
        if (remainingMillis <= 0 || !inFlightRequests.tryAcquire(remainingMillis, TimeUnit.MILLISECONDS)) {
            paymentESService.update(paymentId) {
                it.logProcessing(success = false, processedAt = now(), reason = "Deadline unreachable")
            }
            return
        }

        try {
            if (!rateLimiter.tickBlocking(deadlineMillis = deadline - processingReserveMillis)) {
                paymentESService.update(paymentId) {
                    it.logProcessing(success = false, processedAt = now(), reason = "Deadline unreachable")
                }
                return
            }
            performPayment(paymentId, amount, paymentStartedAt, deadline)
        } finally {
            inFlightRequests.release()
        }
    }

    private fun performPayment(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        try {
            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                header("X-Request-Deadline", deadline.toString())
                post(emptyBody)
            }.build()

            var attempt = 1
            while (true) {
                val callTimeoutMillis = deadline - now() - processingReserveMillis
                if (callTimeoutMillis <= 0) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Deadline unreachable")
                    }
                    return
                }

                val call = client.newCall(request)
                call.timeout().timeout(minOf(callTimeoutMillis, attemptTimeoutMillis), TimeUnit.MILLISECONDS)
                if (attempt > 1)
                    retriesCounter.increment()

                val startedAt = System.nanoTime()
                val body = try {
                    call.execute().use { response ->
                        try {
                            mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                        } catch (e: InterruptedIOException) {
                            throw e
                        } catch (e: Exception) {
                            logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}", e)
                            ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                        }
                    }
                } catch (e: InterruptedIOException) {
                    logger.warn("[$accountName] Payment attempt timeout for txId: $transactionId, payment: $paymentId, attempt: $attempt", e)
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, "Request timeout.")
                } finally {
                    httpLatencySummary.record((System.nanoTime() - startedAt) / 1_000_000.0)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}, attempt: $attempt")

                if (body.result || (body.message != "Temporary error" && body.message != "Request timeout.") || attempt >= maxAttempts) {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                    return
                }

                attempt++
                val retryDeadline = deadline - processingReserveMillis
                while (!rateLimiter.tick()) {
                    if (System.currentTimeMillis() > retryDeadline) {
                        paymentESService.update(paymentId) {
                            it.logProcessing(false, now(), transactionId, reason = "Deadline unreachable")
                        }
                        return
                    }
                    Thread.sleep(1)
                }
            }
        } catch (e: Exception) {
            when (e) {
                is InterruptedIOException -> {
                    logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
