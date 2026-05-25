#!/usr/bin/env bash
# EXTRA.md demo + smoke test.
#
# Stack:  Kafka + Flink JM/TM in Docker; the "truewindow" Flink job submitted
#         to the cluster (the new KeyedProcessFunction with ListState + timers);
#         producer in NORMAL mode from the host.
# Output: one "Window[start - end]: count=N" line per produced event in the
#         taskmanager logs - this script asserts that count > 1 lines exist
#         (i.e. multiple events accumulate in the sliding state).
# Window: 30 seconds (vs 30 minutes in spec) so the rolling behaviour is
#         visible during a short demo run.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose)
JAR="${ROOT}/build/libs/flink-streaming-lab3.jar"

readonly WINDOW_LINE_PATTERN='Window\['
readonly DEMO_WINDOW_SEC=30
readonly DEMO_WATERMARK_SEC=5
readonly DEMO_EVENT_COUNT=40
readonly DEMO_EVENT_INTERVAL_MS=400

taskmanager_logs() {
  "${COMPOSE[@]}" logs --no-color taskmanager 2>&1 || true
}

java21() {
  docker run --rm \
    --network host \
    --add-host=localhost.localdomain:127.0.0.1 \
    -v "${ROOT}:/work" \
    eclipse-temurin:21-jre \
    java "$@"
}

dump_recent_window_lines() {
  echo "--- Window lines (last 50) ---"
  taskmanager_logs | grep "${WINDOW_LINE_PATTERN}" | tail -n 50 || true
}

assert_truewindow_accumulates() {
  if ! taskmanager_logs | grep -q "${WINDOW_LINE_PATTERN}"; then
    echo "[extra] FAIL: no Window[ in taskmanager logs" >&2
    taskmanager_logs | tail -n 120 >&2 || true
    exit 1
  fi
  # truewindow emits once per event; with ~40 events spaced 400ms in a 30s
  # window we should see count climb well past 1.
  if ! taskmanager_logs | grep "${WINDOW_LINE_PATTERN}" | grep -Eq 'count=([2-9]|[1-9][0-9]+)\b'; then
    echo "[extra] FAIL: per-event window counts never exceeded 1" >&2
    dump_recent_window_lines >&2
    exit 1
  fi
}

echo "[extra] Building shadow JAR (JDK 21 in Docker if needed)..."
"${ROOT}/scripts/gradle_java21.sh" --quiet shadowJar

echo "[extra] Starting Kafka + Flink..."
"${COMPOSE[@]}" up -d

cleanup() {
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup ERR INT

KAFKA_TOPICS=(/opt/kafka/bin/kafka-topics.sh)

echo "[extra] Waiting for Kafka..."
for _ in $(seq 1 60); do
  if "${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[extra] Waiting for Flink Web UI..."
for _ in $(seq 1 60); do
  if curl -fsS "http://localhost:8081/overview" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[extra] Creating topic events (if missing)..."
"${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 1 \
  --replication-factor 1

echo "[extra] Copying JAR to jobmanager..."
"${COMPOSE[@]}" cp "${JAR}" jobmanager:/tmp/flink-streaming-lab3.jar

echo "[extra] Submitting truewindow Flink job (detached)..."
"${COMPOSE[@]}" exec -T jobmanager /opt/flink/bin/flink run -d \
  -c lab3.MainKt /tmp/flink-streaming-lab3.jar \
  truewindow --kafka kafka:9092 --topic events \
  --window "${DEMO_WINDOW_SEC}" --wm "${DEMO_WATERMARK_SEC}"

echo "[extra] Waiting for job to start reading..."
sleep 15

echo
echo "==================================================================="
echo "[extra] Producing ${DEMO_EVENT_COUNT} events (NORMAL, ${DEMO_EVENT_INTERVAL_MS}ms)"
echo "        Expect: one Window[...] line per event, count climbing."
echo "==================================================================="
java21 -jar /work/build/libs/flink-streaming-lab3.jar producer \
  --kafka localhost:9094 \
  --topic events \
  --mode NORMAL \
  --count "${DEMO_EVENT_COUNT}" \
  --interval "${DEMO_EVENT_INTERVAL_MS}"

echo "[extra] Waiting for late emits / cleanup timers to settle..."
sleep $((DEMO_WINDOW_SEC + 10))

dump_recent_window_lines
assert_truewindow_accumulates

echo
echo "[extra] OK - truewindow emitted per-event counts that grew past 1,"
echo "       meaning the sliding state actually accumulates events."
echo "[extra] Cluster left running. Web UI: http://localhost:8081"
echo "To stop: docker compose down -v"
