#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

COMPOSE=(docker compose)
JAR="${ROOT}/build/libs/flink-streaming-lab3.jar"
LOG="${TMPDIR:-/tmp}/flink-streaming-lab3-light.log"

readonly WINDOW_LINE_PATTERN='Window\['

wait_for_substring_in_file() {
  local file="$1" needle="$2" max_attempts="${3:-90}" sleep_s="${4:-2}"
  local attempt=0
  while (( attempt < max_attempts )); do
    if grep -q "${needle}" "${file}" 2>/dev/null; then
      return 0
    fi
    sleep "${sleep_s}"
    ((++attempt))
  done
  return 1
}

assert_file_contains_window_line() {
  local file="$1"
  if ! grep -q "${WINDOW_LINE_PATTERN}" "${file}"; then
    echo "[light] FAIL: no window output in ${file}" >&2
    tail -n 80 "${file}" >&2 || true
    exit 1
  fi
}

echo "[light] Building shadow JAR (JDK 21 in Docker if needed)..."
"${ROOT}/scripts/gradle_java21.sh" --quiet shadowJar

echo "[light] Starting Kafka..."
"${COMPOSE[@]}" up -d kafka

cleanup() {
  set +e
  if [[ -n "${FLINK_PID:-}" ]] && kill -0 "${FLINK_PID}" 2>/dev/null; then
    kill "${FLINK_PID}" 2>/dev/null || true
    wait "${FLINK_PID}" 2>/dev/null || true
  fi
  "${COMPOSE[@]}" down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

KAFKA_TOPICS=(/opt/kafka/bin/kafka-topics.sh)

echo "[light] Waiting for Kafka to accept connections..."
for _ in $(seq 1 60); do
  if "${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" --bootstrap-server localhost:9092 --list >/dev/null 2>&1; then
    break
  fi
  sleep 2
done

echo "[light] Creating topic events (if missing)..."
"${COMPOSE[@]}" exec -T kafka "${KAFKA_TOPICS[@]}" \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic events \
  --partitions 1 \
  --replication-factor 1

rm -f "${LOG}"
echo "[light] Starting Flink job (gradle:8.12-jdk21, no wrapper download) -> ${LOG}"
echo "[light] Flink Web UI will be at http://localhost:8081"
docker run --rm \
  --network flink-streaming-lab3 \
  -p 8081:8081 \
  -e GRADLE_USER_HOME=/work/.gradle \
  -v "${ROOT}:/work" -w /work \
  gradle:8.12-jdk21 \
  gradle --no-daemon \
  run \
  --args="flink --kafka kafka:9092 --topic events --window 10 --lateness 5 --wm 5 --rest-port 8081" \
  >"${LOG}" 2>&1 &
FLINK_PID=$!

echo "[light] Waiting until Kafka source is running (up to 180s)..."
if ! wait_for_substring_in_file "${LOG}" 'Starting split fetcher 0' 90 2; then
  echo "[light] FAIL: Flink did not start reading Kafka in time. Log tail:" >&2
  tail -n 60 "${LOG}" >&2 || true
  exit 1
fi
sleep 3

echo "[light] Producing 60 events (NORMAL, 300ms interval)..."
docker run --rm \
  --network host \
  --add-host=localhost.localdomain:127.0.0.1 \
  -v "${ROOT}:/work" \
  eclipse-temurin:21-jre \
  java -jar /work/build/libs/flink-streaming-lab3.jar producer \
  --kafka localhost:9094 \
  --topic events \
  --mode NORMAL \
  --count 60 \
  --interval 300

echo "[light] Waiting for windows to fire..."
sleep 40

assert_file_contains_window_line "${LOG}"

echo "[light] Window lines (sample):"
grep "${WINDOW_LINE_PATTERN}" "${LOG}" | head -n 20 || true

echo "[light] OK"
