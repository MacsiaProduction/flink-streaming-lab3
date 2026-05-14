package lab3.model

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable
import java.time.Instant

data class Event(
    @JsonProperty("event_id") val eventId: String,
    @JsonProperty("user_id") val userId: String,
    @JsonProperty("event_type") val eventType: String,
    @JsonProperty("event_time") val eventTime: Instant,
) : Serializable {
    companion object {
        const val serialVersionUID: Long = 1L
    }
}
