package lab3.flink.sliding

import java.time.Instant
import lab3.flink.WindowCount
import lab3.model.Event
import org.apache.flink.api.common.functions.OpenContext
import org.apache.flink.api.common.state.ListState
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

/**
 * "True" sliding window over the last [windowSizeMillis] of event time.
 *
 * `KeyedProcessFunction` + `ListState` + event-time timers.
 */
class TrueSlidingWindowFunction(
    private val windowSizeMillis: Long,
) : KeyedProcessFunction<Int, Event, WindowCount>() {
    @Transient private lateinit var events: ListState<Event>

    override fun open(openContext: OpenContext) {
        events =
            runtimeContext.getListState(
                ListStateDescriptor("events-in-window", Event::class.java),
            )
    }

    override fun processElement(
        value: Event,
        ctx: Context,
        out: Collector<WindowCount>,
    ) {
        val incomingMs = value.eventTime.toEpochMilli()
        val nowMs = maxOf(ctx.timerService().currentWatermark(), incomingMs)
        val windowStartMs = nowMs - windowSizeMillis

        val kept =
            events.get()
                .filter { it.eventTime.toEpochMilli() >= windowStartMs }
                .toMutableList()
        if (incomingMs >= windowStartMs) {
            kept.add(value)
        }
        events.update(kept)

        out.collect(
            WindowCount(
                windowStart = Instant.ofEpochMilli(windowStartMs),
                windowEnd = Instant.ofEpochMilli(nowMs),
                eventCount = kept.size,
            ),
        )

        // +1 so the timer fires strictly after the event has expired in event time.
        ctx.timerService().registerEventTimeTimer(incomingMs + windowSizeMillis + 1)
    }

    override fun onTimer(
        timestamp: Long,
        ctx: OnTimerContext,
        out: Collector<WindowCount>,
    ) {
        val windowStartMs = timestamp - windowSizeMillis
        val kept = events.get().filter { it.eventTime.toEpochMilli() >= windowStartMs }
        events.update(kept)
    }
}
