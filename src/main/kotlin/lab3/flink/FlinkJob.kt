package lab3.flink

import java.time.Duration
import java.util.UUID
import lab3.model.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

/**
 * Tumbling event-time window count job (TODO.md baseline).
 *
 * Pipeline: Kafka(JSON) -> map(JSON to Event) -> assignTimestampsAndWatermarks
 *           -> windowAll(tumbling event-time) with allowed lateness -> log sink.
 */
object FlinkJob {
    private const val JOB_NAME = "lab3-event-time-tumbling-windows"

    fun run(
        bootstrapServers: String,
        topic: String,
        windowSeconds: Long,
        latenessSeconds: Long,
        watermarkMaxOutOfOrderSeconds: Long,
    ) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 1

        val rawJson = readRawJson(env, bootstrapServers, topic)
        val events = parseAndAssignWatermarks(rawJson, watermarkMaxOutOfOrderSeconds)
        countAndLog(events, windowSeconds, latenessSeconds)

        env.execute(JOB_NAME)
    }

    private fun readRawJson(
        env: StreamExecutionEnvironment,
        bootstrapServers: String,
        topic: String,
    ): DataStream<String> {
        val source =
            KafkaSource.builder<String>()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-lab3-${UUID.randomUUID()}")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(SimpleStringSchema())
                .build()
        return env.fromSource(source, WatermarkStrategy.noWatermarks(), "kafka-source")
            .uid("kafka-source")
    }

    private fun parseAndAssignWatermarks(
        rawJson: DataStream<String>,
        watermarkMaxOutOfOrderSeconds: Long,
    ): DataStream<Event> {
        val parsed =
            rawJson.map(JsonToEventMapFunction())
                .name("json-to-event")
                .uid("json-to-event")

        val watermarks =
            WatermarkStrategy.forBoundedOutOfOrderness<Event>(
                    Duration.ofSeconds(watermarkMaxOutOfOrderSeconds),
                )
                .withTimestampAssigner { event, _ -> event.eventTime.toEpochMilli() }

        return parsed.assignTimestampsAndWatermarks(watermarks)
            .name("assign-timestamps-and-watermarks")
            .uid("assign-timestamps-and-watermarks")
    }

    private fun countAndLog(
        events: DataStream<Event>,
        windowSeconds: Long,
        latenessSeconds: Long,
    ) {
        val windowCounts =
            events
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSeconds)))
                .allowedLateness(Duration.ofSeconds(latenessSeconds))
                .process(TumblingWindowCounter())
                .name("tumbling-window-event-counts")
                .uid("tumbling-window-event-counts")

        windowCounts
            .map { it.formatForLog() }
            .name("format-window-summary")
            .uid("format-window-summary")
            .addSink(WindowSummaryLogSink())
            .name("log-window-summaries")
            .uid("log-window-summaries")
    }
}
