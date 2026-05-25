package lab3

import kotlin.system.exitProcess
import lab3.flink.FlinkJob
import lab3.producer.EventProducer
import lab3.producer.ProducerMode

private object Defaults {
    const val KAFKA = "localhost:9094"
    const val TOPIC = "events"
    const val WINDOW_SEC = 10L
    const val LATENESS_SEC = 5L
    const val WATERMARK_SEC = 5L
    const val PRODUCER_COUNT = 40
    const val PRODUCER_INTERVAL_MS = 400L
}

private val USAGE =
    """
    Usage:
      java -jar ... flink    [--kafka host:port] [--topic name] [--window sec] [--lateness sec] [--wm sec] [--rest-port port]
      java -jar ... producer [--kafka host:port] [--topic name] [--mode NORMAL|OUT_OF_ORDER|LATE_EVENTS]
                             [--count N] [--interval ms]
      java -jar ... help

    Defaults: --kafka ${Defaults.KAFKA}, --topic ${Defaults.TOPIC}.
    """.trimIndent()

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        exitWithUsage(0)
    }
    when (args[0].lowercase()) {
        "flink" -> runFlinkCli(args.drop(1).toTypedArray())
        "producer" -> runProducerCli(args.drop(1).toTypedArray())
        "help", "-h", "--help" -> exitWithUsage(0)
        else -> {
            System.err.println("Unknown command: ${args[0]}")
            exitWithUsage(1)
        }
    }
}

private fun runFlinkCli(args: Array<String>) {
    var kafka = Defaults.KAFKA
    var topic = Defaults.TOPIC
    var windowSec = Defaults.WINDOW_SEC
    var latenessSec = Defaults.LATENESS_SEC
    var wmSec = Defaults.WATERMARK_SEC
    var restPort = 0

    var i = 0
    while (i < args.size) {
        when (val arg = args[i]) {
            "--kafka" -> kafka = nextValue(args, ++i, arg)
            "--topic" -> topic = nextValue(args, ++i, arg)
            "--window" -> windowSec = nextValue(args, ++i, arg).toLong()
            "--lateness" -> latenessSec = nextValue(args, ++i, arg).toLong()
            "--wm" -> wmSec = nextValue(args, ++i, arg).toLong()
            "--rest-port" -> restPort = nextValue(args, ++i, arg).toInt()
            else -> unknownArg("flink", arg)
        }
        i++
    }

    FlinkJob.run(
        bootstrapServers = kafka,
        topic = topic,
        windowSeconds = windowSec,
        latenessSeconds = latenessSec,
        watermarkMaxOutOfOrderSeconds = wmSec,
        restPort = restPort,
    )
}

private fun runProducerCli(args: Array<String>) {
    var kafka = Defaults.KAFKA
    var topic = Defaults.TOPIC
    var mode = ProducerMode.NORMAL
    var count = Defaults.PRODUCER_COUNT
    var intervalMs = Defaults.PRODUCER_INTERVAL_MS

    var i = 0
    while (i < args.size) {
        when (val arg = args[i]) {
            "--kafka" -> kafka = nextValue(args, ++i, arg)
            "--topic" -> topic = nextValue(args, ++i, arg)
            "--mode" -> mode = parseProducerMode(nextValue(args, ++i, arg))
            "--count" -> count = nextValue(args, ++i, arg).toInt()
            "--interval" -> intervalMs = nextValue(args, ++i, arg).toLong()
            else -> unknownArg("producer", arg)
        }
        i++
    }

    EventProducer(kafka, topic).run(mode, count, intervalMs)
}

private fun parseProducerMode(raw: String): ProducerMode =
    when (raw.uppercase()) {
        "NORMAL" -> ProducerMode.NORMAL
        "OUT_OF_ORDER" -> ProducerMode.OUT_OF_ORDER
        "LATE_EVENTS" -> ProducerMode.LATE_EVENTS
        else -> {
            System.err.println("Unknown mode '$raw'. Use NORMAL, OUT_OF_ORDER, or LATE_EVENTS.")
            exitProcess(1)
        }
    }

private fun nextValue(
    args: Array<String>,
    index: Int,
    flagName: String,
): String {
    if (index >= args.size) {
        System.err.println("Missing value for $flagName")
        exitProcess(1)
    }
    return args[index]
}

private fun unknownArg(
    section: String,
    name: String,
): Nothing {
    System.err.println("Unknown $section arg: $name")
    exitWithUsage(1)
}

private fun exitWithUsage(code: Int): Nothing {
    val stream = if (code == 0) System.out else System.err
    stream.println(USAGE)
    exitProcess(code)
}
