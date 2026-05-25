#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose)
JAR="${ROOT}/build/libs/flink-streaming-lab3.jar"

readonly WINDOW_LINE_PATTERN='Window\['

taskmanager_logs() {
  "${COMPOSE[@]}" logs --no-color taskmanager 2>&1 || true
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

java21() {
  docker run --rm \
    --network host \
    --add-host=localhost.localdomain:127.0.0.1 \
    -v "${ROOT}:/work" \
    eclipse-temurin:21-jre \
    java "$@"
}

echo "[full] Starting Kafka + Flink..."
"${COMPOSE[@]}" up -d

cleanup() {
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup ERR INT

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

echo
echo "==================================================================="
echo "[full] Round 1/3: NORMAL  (in-order send; 50 events, baseline window counts)"
echo "==================================================================="
java21 -jar /work/build/libs/flink-streaming-lab3.jar producer \
  --kafka localhost:9094 \
  --topic events \
  --mode NORMAL \
  --count 50 \
  --interval 400

sleep 28
dump_recent_window_lines
assert_taskmanager_has_window_line "NORMAL"

echo
echo "==================================================================="
echo "[full] Round 2/3: OUT_OF_ORDER (buffered reorder; watermark covers"
echo "                  late arrivals within --wm 5s)"
echo "==================================================================="
java21 -jar /work/build/libs/flink-streaming-lab3.jar producer \
  --kafka localhost:9094 \
  --topic events \
  --mode OUT_OF_ORDER \
  --count 50 \
  --interval 400

sleep 28
dump_recent_window_lines
assert_taskmanager_has_window_line "OUT_OF_ORDER"

echo
echo "==================================================================="
echo "[full] Round 3/3: LATE_EVENTS (~12% delayed by ~15s; expect already"
echo "                  fired windows to be re-emitted via allowedLateness)"
echo "==================================================================="
java21 -jar /work/build/libs/flink-streaming-lab3.jar producer \
  --kafka localhost:9094 \
  --topic events \
  --mode LATE_EVENTS \
  --count 50 \
  --interval 400

sleep 45
dump_recent_window_lines
assert_taskmanager_has_window_line "LATE_EVENTS"

echo
echo "[full] OK - all three TODO.md producer modes produced window output."
echo "[full] Cluster left running. Web UI: http://localhost:8081"
echo "[full] To stop: docker compose down -v"
