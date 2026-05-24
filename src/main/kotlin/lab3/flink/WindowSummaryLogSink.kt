package lab3.flink

import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction
import org.slf4j.LoggerFactory

/**
 * Emits each window summary line at INFO so it follows the configured Log4j2 console appender
 * (line-buffered compared to Flink `.print()` when stdout is redirected).
 */
class WindowSummaryLogSink : SinkFunction<String> {
    override fun invoke(
        value: String,
        context: SinkFunction.Context,
    ) {
        log.info(value)
    }

    companion object {
        private val log = LoggerFactory.getLogger("lab3.flink.WindowResults")
    }
}
