package lab3.flink

import java.time.Instant

data class WindowCount(
    val windowStart: Instant,
    val windowEnd: Instant,
    val eventCount: Int,
) {
    fun formatForLog(): String = "Window[$windowStart - $windowEnd]: count=$eventCount"
}
