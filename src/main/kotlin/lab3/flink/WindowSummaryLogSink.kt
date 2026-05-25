package lab3.flink

import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction
import org.slf4j.LoggerFactory

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
