package lab3

import kotlin.system.exitProcess
import lab3.flink.FlinkJob
import lab3.producer.EventProducer
import lab3.producer.ProducerMode

fun main(args: Array<String>) {
    if (args.isEmpty()) {
        printUsage()
        exitProcess(1)
    }
    when (args[0].lowercase()) {
        "flink" -> runFlinkCli(args.drop(1).toTypedArray())
        "producer" -> runProducerCli(args.drop(1).toTypedArray())
        else -> {
            System.err.println("Unknown command: ${args[0]}")
            printUsage()
            exitProcess(1)
        }
    }
}

private fun runFlinkCli(args: Array<String>) {
    var kafka = "localhost:9094"
    var topic = "events"
    var windowSec = 10L
    var latenessSec = 5L
    var wmSec = 5L
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--kafka" -> kafka = args[++i]
            "--topic" -> topic = args[++i]
            "--window" -> windowSec = args[++i].toLong()
            "--lateness" -> latenessSec = args[++i].toLong()
            "--wm" -> wmSec = args[++i].toLong()
            else -> {
                System.err.println("Unknown flink arg: ${args[i]}")
                printUsage()
                exitProcess(1)
            }
        }
        i++
    }
    FlinkJob.run(
        bootstrapServers = kafka,
        topic = topic,
        windowSeconds = windowSec,
        latenessSeconds = latenessSec,
        watermarkMaxOutOfOrderSeconds = wmSec,
    )
}

private fun runProducerCli(args: Array<String>) {
    var kafka = "localhost:9094"
    var topic = "events"
    var mode = ProducerMode.NORMAL
    var count = 40
    var intervalMs = 400L
    var i = 0
    while (i < args.size) {
        when (args[i]) {
            "--kafka" -> kafka = args[++i]
            "--topic" -> topic = args[++i]
            "--mode" -> {
                val m = args[++i].uppercase()
                mode =
                    when (m) {
                        "NORMAL" -> ProducerMode.NORMAL
                        "OUT_OF_ORDER" -> ProducerMode.OUT_OF_ORDER
                        "LATE_EVENTS" -> ProducerMode.LATE_EVENTS
                        else -> {
                            System.err.println("Unknown mode. Use NORMAL, OUT_OF_ORDER, or LATE_EVENTS")
                            exitProcess(1)
                        }
                    }
            }
            "--count" -> count = args[++i].toInt()
            "--interval" -> intervalMs = args[++i].toLong()
            else -> {
                System.err.println("Unknown producer arg: ${args[i]}")
                printUsage()
                exitProcess(1)
            }
        }
        i++
    }
    EventProducer(kafka, topic).run(mode, count, intervalMs)
}

private fun printUsage() {
    System.err.println(
        """
        Usage:
          java -jar ... flink [--kafka host:port] [--topic name] [--window sec] [--lateness sec] [--wm sec]
          java -jar ... producer [--kafka host:port] [--topic name] [--mode NORMAL|OUT_OF_ORDER|LATE_EVENTS] [--count N] [--interval ms]
        """.trimIndent(),
    )
}
