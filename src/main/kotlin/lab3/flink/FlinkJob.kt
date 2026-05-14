package lab3.flink

import java.time.Duration
import java.util.UUID
import lab3.model.Event
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows

object FlinkJob {
    fun run(
        bootstrapServers: String,
        topic: String,
        windowSeconds: Long,
        latenessSeconds: Long,
        watermarkMaxOutOfOrderSeconds: Long,
    ) {
        val env = StreamExecutionEnvironment.getExecutionEnvironment()
        env.parallelism = 1

        val kafkaSource =
            KafkaSource.builder<String>()
                .setBootstrapServers(bootstrapServers)
                .setTopics(topic)
                .setGroupId("flink-lab3-${UUID.randomUUID()}")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(SimpleStringSchema())
                .build()

        val raw =
            env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source",
            )

        val events =
            raw.map(JsonToEventMapFunction())
                .assignTimestampsAndWatermarks(
                    WatermarkStrategy.forBoundedOutOfOrderness<Event>(
                            Duration.ofSeconds(watermarkMaxOutOfOrderSeconds),
                        )
                        .withTimestampAssigner { event, _ -> event.eventTime.toEpochMilli() },
                )

        val windowCounts =
            events
                .windowAll(TumblingEventTimeWindows.of(Duration.ofSeconds(windowSeconds)))
                .allowedLateness(Duration.ofSeconds(latenessSeconds))
                .process(TumblingWindowCounter())
                .name("tumbling-window-event-counts")

        windowCounts
            .map { it.formatForLog() }
            .name("window-summary-lines")
            .addSink(WindowSummaryLogSink())
            .name("log-window-summaries")

        env.execute("lab3-event-time-tumbling-windows")
    }
}
