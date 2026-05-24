# flink-streaming-lab3

End-to-end Kotlin + Apache Flink 2.0 + Kafka lab that implements
[TODO.md](TODO.md): a producer that feeds events into Kafka in three controlled
patterns, and a Flink job that consumes them, applies event-time tumbling
windows with watermarks and allowed lateness, and prints window counts to the
console.

```
+----------+   JSON   +-------+   bytes   +-------------------------+
| Producer | -------> | Kafka | --------> |  Flink streaming job    |
| (3 modes)|          |       |           |  event-time + watermarks|
+----------+          +-------+           |  TumblingEventTimeWindow|
                                          |  allowedLateness        |
                                          +-----------+-------------+
                                                      |
                                                      v
                                        Window[start - end]: count=N
                                        (Log4j INFO, lab3.flink.WindowResults)
```

## Web UIs and endpoints

| Service           | URL / address              | Notes                            |
| ----------------- | -------------------------- | -------------------------------- |
| Flink Web UI      | <http://localhost:8081>    | job graph, watermarks, metrics   |
| Kafka (external)  | `localhost:9094`           | used by the host-side producer   |
| Kafka (internal)  | `kafka:9092`               | used by Flink inside Docker      |

The Flink dashboard is the easiest place to confirm the pipeline matches what
TODO.md asks for - each operator carries an explicit `.name()` / `.uid()` so the
graph in <http://localhost:8081> reads as:

```
kafka-source -> json-to-event -> assign-timestamps-and-watermarks
              -> tumbling-window-event-counts
              -> format-window-summary -> log-window-summaries
```

## Quickstart

Requirements: Docker (Compose v2), JDK 21 (only needed if you want to run the
producer from the host without Docker).

```bash
# Full end-to-end demo: Kafka + Flink in Docker, job submitted to the cluster,
# all three producer modes run from the host, asserts that window output appeared.
./scripts/test_full.sh

# Fast iteration: only Kafka in Docker, Flink job runs via gradle on host JVM.
./scripts/test_light.sh
```

While `test_full.sh` is running, open <http://localhost:8081> to watch the job.
Window results land in the Flink TaskManager log (the script also dumps the
last 40 lines after each round). You can also tail them live:

```bash
docker compose logs -f taskmanager | grep 'Window\['
```

## Manual demo (matches TODO.md scenarios)

```bash
# 1. Start the cluster
docker compose up -d

# 2. Build the fat jar (uses gradle:8.12-jdk21 in Docker)
./scripts/gradle_java21.sh --quiet shadowJar

# 3. Submit the Flink job to the cluster
docker compose cp build/libs/flink-streaming-lab3.jar jobmanager:/tmp/job.jar
docker compose exec -T jobmanager /opt/flink/bin/flink run -d \
  -c lab3.MainKt /tmp/job.jar \
  flink --kafka kafka:9092 --topic events --window 10 --lateness 5 --wm 5

# 4. Run the three producer modes one after another (each from the host)
JAR=build/libs/flink-streaming-lab3.jar
java -jar "$JAR" producer --mode NORMAL       --count 40 --interval 400
java -jar "$JAR" producer --mode OUT_OF_ORDER --count 40 --interval 400
java -jar "$JAR" producer --mode LATE_EVENTS  --count 40 --interval 400

# 5. Observe windows firing in the TaskManager log
docker compose logs --tail=80 taskmanager | grep 'Window\['
```

Expected log lines (one per fired window):

```
... INFO  lab3.flink.WindowResults - Window[2026-05-24T18:34:10Z - 2026-05-24T18:34:20Z]: count=12
... INFO  lab3.flink.WindowResults - Window[2026-05-24T18:34:20Z - 2026-05-24T18:34:30Z]: count=15
```

In `LATE_EVENTS` mode you should see a window get re-emitted after the
deferred batch arrives, because `allowedLateness` is set to 5 seconds.

## How TODO.md is covered

| TODO.md requirement                                              | Where it lives                                                                                              |
| ---------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| Event has `event_id`, `user_id`, `event_type`, `event_time`      | `src/main/kotlin/lab3/model/Event.kt`                                                                       |
| Producer writes events to Kafka                                  | `lab3.producer.EventProducer` (`run` -> `KafkaProducer.send`)                                               |
| Mode 1: in-order sending                                         | `EventProducer.sendInOrder` (`--mode NORMAL`)                                                               |
| Mode 2: out-of-order via buffer (5-10 events, 20-30% reorder)    | `EventProducer.sendWithReorderBuffer` + `OUT_OF_ORDER_BUFFER_SIZE=8`, `OUT_OF_ORDER_PROBABILITY=0.25`       |
| Mode 3: late events (10-15% delayed)                             | `EventProducer.sendWithDeferredLateBatch` + `LATE_RANDOM_PROBABILITY=0.12` and deterministic 1-in-8         |
| Flink reads from Kafka                                           | `FlinkJob.readRawJson` (`KafkaSource`)                                                                      |
| Extracts `event_time`                                            | `FlinkJob.parseAndAssignWatermarks` -> `withTimestampAssigner { e, _ -> e.eventTime.toEpochMilli() }`       |
| Watermarks                                                       | `WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(--wm))`                                      |
| Allowed lateness                                                 | `.allowedLateness(Duration.ofSeconds(--lateness))`                                                          |
| Counts events in tumbling windows only                           | `TumblingEventTimeWindows.of(...)` + `TumblingWindowCounter`                                                |
| Window results to the console                                    | `WindowSummaryLogSink` (`lab3.flink.WindowResults` Log4j logger)                                            |

## CLI reference

`java -jar build/libs/flink-streaming-lab3.jar <command> [options]`

| Command    | Option         | Default            | Meaning                                                    |
| ---------- | -------------- | ------------------ | ---------------------------------------------------------- |
| `flink`    | `--kafka`      | `localhost:9094`   | Kafka bootstrap servers                                    |
| `flink`    | `--topic`      | `events`           | Kafka topic to consume                                     |
| `flink`    | `--window`     | `10`               | Tumbling window size in seconds                            |
| `flink`    | `--lateness`   | `5`                | Allowed lateness (seconds)                                 |
| `flink`    | `--wm`         | `5`                | Watermark max out-of-orderness (seconds)                   |
| `producer` | `--kafka`      | `localhost:9094`   | Kafka bootstrap servers                                    |
| `producer` | `--topic`      | `events`           | Kafka topic to produce to                                  |
| `producer` | `--mode`       | `NORMAL`           | `NORMAL` / `OUT_OF_ORDER` / `LATE_EVENTS`                  |
| `producer` | `--count`      | `40`               | Number of events to produce                                |
| `producer` | `--interval`   | `400`              | Sleep between sends (ms)                                   |
| `help`     |                |                    | Print usage                                                |

When `flink` is submitted **to the cluster** (as in `test_full.sh`) use
`--kafka kafka:9092`. When running the job from the **host** JVM (as in
`test_light.sh`) use `--kafka localhost:9094`.

## Project layout

```
build.gradle                       Kotlin/JVM + Flink + shadow jar setup
docker compose.yml                 Kafka (KRaft) + Flink JM/TM
scripts/
  gradle_java21.sh                 run gradle inside gradle:8.12-jdk21 image
  test_full.sh                     full end-to-end demo (Kafka + Flink + 3 modes)
  test_light.sh                    fast iteration (host JVM Flink + Docker Kafka)
src/main/kotlin/lab3/
  Main.kt                          CLI entry point: `flink` and `producer` subcommands
  JsonSerde.kt                     Jackson mapper with JavaTimeModule
  model/Event.kt                   Event data class (TODO.md fields)
  producer/EventProducer.kt        Kafka producer with three TODO.md modes
  flink/
    FlinkJob.kt                    Flink pipeline assembly (source -> window -> sink)
    JsonToEventMapFunction.kt      JSON -> Event MapFunction
    TumblingWindowCounter.kt       ProcessAllWindowFunction emitting WindowCount
    WindowCount.kt                 Window result type with `formatForLog()`
    WindowSummaryLogSink.kt        Log4j INFO sink for WindowResults
src/main/resources/
  log4j2.properties                Log4j2 layout used by the Flink job
  simplelogger.properties          slf4j-simple config used by `java -jar ... producer`
```

## Tech stack

- Kotlin 2.1.20 on JDK 21
- Apache Flink 2.0.1 (streaming-java, clients, connector-base)
- Flink Kafka connector 4.0.1-2.0
- Apache Kafka 3.9.2 (KRaft, single broker)
- Jackson 2.18.3 (with `jackson-module-kotlin` and `jsr310`)
- Log4j2 2.24.3 for the Flink job, slf4j-simple for the standalone producer
- Gradle 8.12, com.github.johnrengelman.shadow 7.1.2

## EXTRA: true sliding window ([EXTRA.md](EXTRA.md))

Branch goal: emit an aggregation on **every** incoming event over the events
whose `event_time` falls in `[now - 30min, now]`, where state is incrementally
cleaned (no event lingers in state beyond the window).

Design rationale and the rejected alternatives are recorded in
[`src/main/kotlin/lab3/flink/sliding/TrueSlidingWindowFunction.kt`](src/main/kotlin/lab3/flink/sliding/TrueSlidingWindowFunction.kt).
Summary: `KeyedProcessFunction` + `ListState<Event>` + per-event event-time
timers. Built-in Flink primitives only. Per-element O(N) — acceptable for the
lab, see in-code note on bucketed `MapState` upgrade for scale.

| Piece                                  | Where it lives                                                                                                |
| -------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| Pipeline assembly (Kafka -> sliding)   | `src/main/kotlin/lab3/flink/sliding/TrueSlidingWindowJob.kt`                                                  |
| State + per-event cleanup timers       | `src/main/kotlin/lab3/flink/sliding/TrueSlidingWindowFunction.kt`                                             |
| CLI entry point (`truewindow`)         | `Main.runTrueWindowCli` in `src/main/kotlin/lab3/Main.kt`                                                     |
| Demo / smoke test                      | `scripts/test_extra.sh`                                                                                       |

### Run the EXTRA demo

```bash
# Full end-to-end demo: Kafka + Flink + truewindow job + NORMAL producer.
# Uses a 30s window (vs 30min in spec) so behaviour is visible in seconds.
./scripts/test_extra.sh
```

Or manually:

```bash
docker compose up -d
./scripts/gradle_java21.sh --quiet shadowJar

docker compose cp build/libs/flink-streaming-lab3.jar jobmanager:/tmp/job.jar
docker compose exec -T jobmanager /opt/flink/bin/flink run -d \
  -c lab3.MainKt /tmp/job.jar \
  truewindow --kafka kafka:9092 --topic events --window 30 --wm 5

java -jar build/libs/flink-streaming-lab3.jar producer \
  --mode NORMAL --count 40 --interval 400

docker compose logs --tail=200 taskmanager | grep 'Window\['
```

Expected shape: one `Window[start - end]: count=N` line **per produced event**,
with `count` rising from 1 as state fills, then plateauing / falling as old
events expire from the rolling window.
