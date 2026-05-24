package lab3.flink.sliding

import java.time.Duration
import java.util.UUID
import lab3.flink.JsonToEventMapFunction
import lab3.flink.WindowSummaryLogSink
import lab3.model.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

/**
 * EXTRA.md job: "true" sliding window emitting on every event, with state
 * holding exactly the last [windowSeconds] of events. The Kafka source and
 * watermark setup mirror [lab3.flink.FlinkJob] so the only difference visible
 * to a reviewer is the windowing operator itself.
 *
 * Pipeline: Kafka(JSON) -> map(JSON to Event) -> assignTimestampsAndWatermarks
 *           -> keyBy(constant) -> TrueSlidingWindowFunction -> log sink.
 */
object TrueSlidingWindowJob {
    private const val JOB_NAME = "lab3-true-sliding-window"
    private const val CONSTANT_KEY = 0

    fun run(
        bootstrapServers: String,
        topic: String,
        windowSeconds: Long,
        watermarkMaxOutOfOrderSeconds: Long,
    ) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 1

        val rawJson = readRawJson(env, bootstrapServers, topic)
        val events = parseAndAssignWatermarks(rawJson, watermarkMaxOutOfOrderSeconds)
        slideAndLog(events, windowSeconds)

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
                .setGroupId("flink-lab3-truewindow-${UUID.randomUUID()}")
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

    private fun slideAndLog(
        events: DataStream<Event>,
        windowSeconds: Long,
    ) {
        val windowSizeMillis = Duration.ofSeconds(windowSeconds).toMillis()

        val perEventCounts =
            events
                .keyBy { CONSTANT_KEY }
                .process(TrueSlidingWindowFunction(windowSizeMillis))
                .name("true-sliding-window-counts")
                .uid("true-sliding-window-counts")

        perEventCounts
            .map { it.formatForLog() }
            .name("format-window-summary")
            .uid("format-window-summary")
            .addSink(WindowSummaryLogSink())
            .name("log-window-summaries")
            .uid("log-window-summaries")
    }
}
