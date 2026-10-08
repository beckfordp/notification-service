#!/usr/bin/env bash
set -euo pipefail

# Verifies US-7.1 (track consume-order-status_20261003) end-to-end: brings up
# Kafka + the real service, publishes a synthetic order.status-changed event
# per status directly onto the topic (no live order-service needed, per the
# stub/fan-out pattern), and confirms the real consumer logs the matching
# email send for each status while /health/ready stays 200 throughout - not
# just that the unit/integration test suite passes (those Kafka tests stay
# .ignore'd in this dev environment - see OrderStatusChangedConsumerSuite).
#
# Usage: ./scripts/verify-order-status-changed.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""
LOG_FILE="/tmp/notification-service-verify.log"

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: notification-service did not become ready in time." >&2
  return 1
}

check() {
  local label="$1" expected="$2" actual="$3"
  if [ "$actual" = "$expected" ]; then
    echo "   OK: ${label} (got ${actual})"
  else
    echo "   FAIL: ${label} - expected ${expected}, got ${actual}" >&2
    FAILED=1
  fi
}

produce_one() {
  local topic="$1" json="$2"
  echo "$json" | docker compose exec -T kafka /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic" >/dev/null 2>&1
}

wait_for_log_line() {
  local pattern="$1"
  for _ in $(seq 1 30); do
    grep -qF "$pattern" "$LOG_FILE" && return 0
    sleep 1
  done
  return 1
}

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: kafka starting"

echo
echo "2. Pre-create order.status-changed (avoids a cold-subscription race: the"
echo "   consumer subscribes at startup, and auto-created-by-producer topics"
echo "   aren't always picked up by an already-subscribed consumer in time)..."
for _ in $(seq 1 30); do
  docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server localhost:9092 --create --if-not-exists \
    --topic order.status-changed --partitions 1 --replication-factor 1 \
    >/dev/null 2>&1 && break
  sleep 1
done
echo "   OK: order.status-changed topic created"

echo
echo "3. Starting notification-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >"$LOG_FILE" 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: notification-service ready on :8080"

echo
echo "4. Publish one synthetic event per status and confirm the matching email..."
for status in reservation_failed confirmed payment_failed; do
  order_id="verify-${status}-$(date +%s)"
  produce_one order.status-changed \
    "{\"orderId\":\"${order_id}\",\"customerId\":\"cust-verify\",\"status\":\"${status}\",\"timestamp\":\"2026-01-01T00:00:00Z\"}"
  if wait_for_log_line "order_id=${order_id}, status=${status}"; then
    check "email sent for status '${status}'" "0" "0"
  else
    check "email sent for status '${status}'" "0" "1"
  fi

  ready="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready)"
  check "/health/ready still 200 after '${status}'" "200" "$ready"
done

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
