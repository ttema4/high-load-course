package ru.quipy.payments.subscribers

import io.micrometer.core.instrument.MeterRegistry
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.api.PaymentProcessedEvent
import ru.quipy.streams.AggregateSubscriptionsManager
import ru.quipy.streams.annotation.RetryConf
import ru.quipy.streams.annotation.RetryFailedStrategy
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.CopyOnWriteArrayList
import jakarta.annotation.PostConstruct

@Service
class PaymentTransactionsSubscriber {

    val logger: Logger = LoggerFactory.getLogger(PaymentTransactionsSubscriber::class.java)

    val paymentLog: MutableMap<UUID, MutableList<PaymentLogRecord>> = ConcurrentHashMap()

    @Autowired
    lateinit var subscriptionsManager: AggregateSubscriptionsManager

    @Autowired
    lateinit var meterRegistry: MeterRegistry

    @PostConstruct
    fun init() {
        subscriptionsManager.createSubscriber(
            PaymentAggregate::class,
            "payments:payment-processings-subscriber",
            retryConf = RetryConf(1, RetryFailedStrategy.SKIP_EVENT)
        ) {
            `when`(PaymentProcessedEvent::class) { event ->
                meterRegistry.counter("shop_payments_finished_total", "outcome", if (event.success) "success" else "fail").increment()
                paymentLog.computeIfAbsent(event.orderId) {
                    CopyOnWriteArrayList()
                }.add(
                    PaymentLogRecord(
                        event.processedAt,
                        status = if (event.success) PaymentStatus.SUCCESS else PaymentStatus.FAILED,
                        event.amount,
                        event.paymentId,
                    )
                )
            }
        }
    }

    class PaymentLogRecord(
        val timestamp: Long,
        val status: PaymentStatus,
        val amount: Int,
        val transactionId: UUID,
    )

    enum class PaymentStatus {
        FAILED,
        SUCCESS
    }
}