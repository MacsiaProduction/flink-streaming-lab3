package lab3.producer

import java.time.Instant
import java.util.Properties
import java.util.UUID
import kotlin.random.Random
import lab3.JsonSerde
import lab3.model.Event
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer

enum class ProducerMode {
    NORMAL,
    OUT_OF_ORDER,
    LATE_EVENTS,
}

class EventProducer(
    private val bootstrapServers: String,
    private val topic: String,
) {
    fun run(
        mode: ProducerMode,
        count: Int,
        intervalMs: Long,
    ) {
        val props = producerProperties()
        KafkaProducer<String, String>(props).use { producer ->
            when (mode) {
                ProducerMode.NORMAL -> sendInOrder(producer, count, intervalMs)
                ProducerMode.OUT_OF_ORDER -> sendWithReorderBuffer(producer, count, intervalMs)
                ProducerMode.LATE_EVENTS -> sendWithDeferredLateBatch(producer, count, intervalMs)
            }
            producer.flush()
        }
    }

    private fun producerProperties(): Properties =
        Properties().apply {
            put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
            put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
            put(ProducerConfig.ACKS_CONFIG, "all")
            put(ProducerConfig.LINGER_MS_CONFIG, "0")
        }

    private fun sendInOrder(
        producer: KafkaProducer<String, String>,
        count: Int,
        intervalMs: Long,
    ) {
        repeat(count) { i ->
            sendEvent(producer, syntheticEvent(i))
            sleep(intervalMs)
        }
    }

    private fun sendWithReorderBuffer(
        producer: KafkaProducer<String, String>,
        count: Int,
        intervalMs: Long,
    ) {
        val buffer = ArrayDeque<Event>()
        var produced = 0
        var sent = 0
        while (sent < count) {
            while (buffer.size < OUT_OF_ORDER_BUFFER_SIZE && produced < count) {
                buffer.addLast(syntheticEvent(produced++))
            }
            if (buffer.isEmpty()) {
                break
            }
            val shouldReorder = Random.nextDouble() < OUT_OF_ORDER_PROBABILITY && buffer.size >= 2
            val toSend =
                if (shouldReorder) {
                    buffer.removeAt(Random.nextInt(buffer.size))
                } else {
                    buffer.removeFirst()
                }
            sendEvent(producer, toSend)
            sent++
            sleep(intervalMs)
        }
    }

    private fun sendWithDeferredLateBatch(
        producer: KafkaProducer<String, String>,
        count: Int,
        intervalMs: Long,
    ) {
        val deferred = mutableListOf<Event>()
        repeat(count) { i ->
            val event = syntheticEvent(i)
            if (shouldDeferForLateDelivery(i)) {
                deferred.add(event)
            } else {
                sendEvent(producer, event)
                sleep(intervalMs)
            }
        }
        if (deferred.isNotEmpty()) {
            Thread.sleep(LATE_BATCH_DELAY_MS)
            deferred.forEach { e ->
                sendEvent(producer, e)
                sleep(intervalMs)
            }
        }
    }

    private fun shouldDeferForLateDelivery(index: Int): Boolean =
        index % LATE_DETERMINISTIC_MODULO == LATE_DETERMINISTIC_INDEX ||
            Random.nextDouble() < LATE_RANDOM_PROBABILITY

    private fun syntheticEvent(index: Int): Event {
        val base = Instant.now().toEpochMilli()
        val skewMs = (index % EVENT_TIME_SKEW_CYCLE) * EVENT_TIME_SKEW_STEP_MS
        return Event(
            eventId = UUID.randomUUID().toString(),
            userId = "user-${index % USER_ID_MODULO}",
            eventType = if (index % 2 == 0) "click" else "view",
            eventTime = Instant.ofEpochMilli(base - skewMs),
        )
    }

    private fun sendEvent(
        producer: KafkaProducer<String, String>,
        event: Event,
    ) {
        val json = JsonSerde.toJson(event)
        producer.send(ProducerRecord(topic, event.eventId, json))
    }

    private fun sleep(intervalMs: Long) {
        if (intervalMs > 0) {
            Thread.sleep(intervalMs)
        }
    }

    private companion object {
        const val OUT_OF_ORDER_BUFFER_SIZE = 8
        const val OUT_OF_ORDER_PROBABILITY = 0.25
        const val LATE_RANDOM_PROBABILITY = 0.12
        const val LATE_BATCH_DELAY_MS = 15_000L
        const val LATE_DETERMINISTIC_MODULO = 8
        const val LATE_DETERMINISTIC_INDEX = 7
        const val EVENT_TIME_SKEW_CYCLE = 7
        const val EVENT_TIME_SKEW_STEP_MS = 200L
        const val USER_ID_MODULO = 5
    }
}
