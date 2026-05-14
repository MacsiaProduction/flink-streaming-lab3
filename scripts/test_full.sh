#!/usr/bin/env bash
# Full stack: Kafka + Flink in Docker; job on cluster; three producer modes from host (localhost:9094).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose)
JAR="${ROOT}/build/libs/flink-streaming-lab3.jar"

readonly WINDOW_LINE_PATTERN='Window\['

taskmanager_logs() {
  "${COMPOSE[@]}" logs --no-color taskmanager 2>&1
}

dump_recent_window_lines() {
  echo "--- Window lines (last 40) ---"
  taskmanager_logs | grep "${WINDOW_LINE_PATTERN}" | tail -n 40 || true
}

assert_taskmanager_has_window_line() {
  local round="$1"
  if ! taskmanager_logs | grep -q "${WINDOW_LINE_PATTERN}"; then
    echo "[full] FAIL (${round}): no Window[ in taskmanager logs yet" >&2
    taskmanager_logs | tail -n 120 >&2 || true
    exit 1
  fi
}

echo "[full] Building shadow JAR (JDK 21 in Docker if needed)..."
"${ROOT}/scripts/gradle_java21.sh" --quiet shadowJar

echo "[full] Starting Kafka + Flink..."
"${COMPOSE[@]}" up -d

cleanup() {
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

KAFKA_TOPICS=(/opt/kafka/bin/kafka-topics.sh)

echo "[full] Waiting for Kafka..."
for _ in $(seq 1 60); do
  if "${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[full] Waiting for Flink Web UI..."
for _ in $(seq 1 60); do
  if curl -fsS "http://localhost:8081/overview" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[full] Creating topic events (if missing)..."
"${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 1 \
  --replication-factor 1

echo "[full] Copying JAR to jobmanager..."
"${COMPOSE[@]}" cp "${JAR}" jobmanager:/tmp/flink-streaming-lab3.jar

echo "[full] Submitting Flink job (detached)..."
"${COMPOSE[@]}" exec -T jobmanager /opt/flink/bin/flink run -d \
  -c lab3.MainKt /tmp/flink-streaming-lab3.jar \
  flink --kafka kafka:9092 --topic events --window 10 --lateness 5 --wm 5

echo "[full] Waiting for job to start reading..."
sleep 15

echo "[full] Round 1: NORMAL (40 events, 400ms)"
java -jar "${JAR}" producer \
  --kafka localhost:9094 \
  --topic events \
  --mode NORMAL \
  --count 40 \
  --interval 400

sleep 28
dump_recent_window_lines
assert_taskmanager_has_window_line "NORMAL"

echo "[full] Round 2: OUT_OF_ORDER (40 events, 400ms)"
java -jar "${JAR}" producer \
  --kafka localhost:9094 \
  --topic events \
  --mode OUT_OF_ORDER \
  --count 40 \
  --interval 400

sleep 28
dump_recent_window_lines
assert_taskmanager_has_window_line "OUT_OF_ORDER"

echo "[full] Round 3: LATE_EVENTS (40 events; producer delays a subset ~15s)"
java -jar "${JAR}" producer \
  --kafka localhost:9094 \
  --topic events \
  --mode LATE_EVENTS \
  --count 40 \
  --interval 400

sleep 45
dump_recent_window_lines
assert_taskmanager_has_window_line "LATE_EVENTS"

echo "[full] OK"
