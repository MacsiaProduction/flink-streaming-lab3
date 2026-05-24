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
 * Built only out of Flink primitives: `KeyedProcessFunction` + `ListState` +
 * event-time timers. Compared to a sliding tumbling window:
 *  - emits on **every** incoming event, not on slide boundaries;
 *  - state holds **exactly** the events with `event_time` in
 *    `[now - windowSizeMillis, now]` at any moment;
 *  - cleanup is incremental — both on element and on per-event timers — so
 *    nothing lingers in state beyond the window.
 *
 * "now" is defined as `max(currentWatermark, incoming.eventTime)`. This keeps
 * the window from rolling backwards when a late event arrives: late events
 * still trigger an emit, but they don't pull old already-expired events back
 * into state.
 *
 * Scaling note: a `ListState<Event>` is O(N) per element. For higher
 * throughput, replace this with a `MapState<bucketMs, Long>` (bucket counts
 * over second-resolution buckets) — O(buckets) per element.
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
        // Watermark has advanced to >= timestamp; drop anything whose event_time
        // is already outside the [watermark - W, watermark] window. No emit here
        // because the spec asks for output only on event arrival.
        val windowStartMs = timestamp - windowSizeMillis
        val kept = events.get().filter { it.eventTime.toEpochMilli() >= windowStartMs }
        events.update(kept)
    }
}
