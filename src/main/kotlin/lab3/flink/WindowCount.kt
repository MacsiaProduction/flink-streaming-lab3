package lab3.flink

import java.time.Instant

/**
 * Result of counting events in one tumbling event-time window.
 */
data class WindowCount(
    val windowStart: Instant,
    val windowEnd: Instant,
    val eventCount: Int,
) {
    fun formatForLog(): String = "Window[$windowStart - $windowEnd]: count=$eventCount"
}
