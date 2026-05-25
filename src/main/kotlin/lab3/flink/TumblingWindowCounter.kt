package lab3.flink

import java.time.Instant
import lab3.model.Event
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

class TumblingWindowCounter : ProcessAllWindowFunction<Event, WindowCount, TimeWindow>() {
    override fun process(
        context: Context,
        elements: Iterable<Event>,
        out: Collector<WindowCount>,
    ) {
        val w = context.window()
        out.collect(
            WindowCount(
                windowStart = Instant.ofEpochMilli(w.start),
                windowEnd = Instant.ofEpochMilli(w.end),
                eventCount = elements.count(),
            ),
        )
    }
}
