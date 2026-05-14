package lab3.flink

import lab3.JsonSerde
import lab3.model.Event
import org.apache.flink.api.common.functions.RichMapFunction

class JsonToEventMapFunction : RichMapFunction<String, Event>() {
    override fun map(value: String): Event = JsonSerde.fromJson(value)
}
