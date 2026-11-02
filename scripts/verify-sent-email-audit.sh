#!/usr/bin/env bash
# Live verification for track sent-email-audit_20261102 (ADM-2).
#
# Starts throwaway Postgres and Kafka containers plus the service itself (host
# JVM), publishes one synthetic event on each consumed topic, then asserts:
#   - every event produced exactly one sent_email row (pending -> sent);
#   - GET /emails?orderId= returns the order + payment + fulfilment emails;
#   - GET /emails?customerId= returns the order email;
#   - recipient types and the envelope eventId survive into the audit rows.
#
# No Kubernetes/Helm is involved: those values, the compose Postgres service
# and readiness gating are ADM-6.
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SERVICE_PORT="${SERVICE_PORT:-18080}"
METRICS_PORT="${METRICS_PORT:-19090}"
PG_PORT="${PG_PORT:-55432}"
KAFKA_PORT="${KAFKA_PORT:-59092}"
PG_CONTAINER="notification-verify-postgres-$$"
KAFKA_CONTAINER="notification-verify-kafka-$$"
SERVICE_LOG="$(mktemp -t notification-verify-log.XXXXXX)"
SBT_PID=""

cleanup() {
  if [ -n "$SBT_PID" ]; then
    kill "$SBT_PID" 2>/dev/null || true
    wait "$SBT_PID" 2>/dev/null || true
  fi
  pkill -f 'notificationservice.Main' 2>/dev/null || true
  docker rm -f "$PG_CONTAINER" "$KAFKA_CONTAINER" >/dev/null 2>&1 || true
  rm -f "$SERVICE_LOG"
}
trap cleanup EXIT

uuid() { python3 -c 'import uuid; print(uuid.uuid4())'; }

echo "== ADM-2 sent-email audit live verification =="

echo "-- starting throwaway Postgres and Kafka --"
docker run -d --rm --name "$PG_CONTAINER" \
  -p "$PG_PORT:5432" \
  -e POSTGRES_DB=notification \
  -e POSTGRES_USER=notification \
  -e POSTGRES_PASSWORD=notification \
  postgres:16-alpine >/dev/null

docker run -d --rm --name "$KAFKA_CONTAINER" \
  -p "$KAFKA_PORT:29092" \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9092,CONTROLLER://:9093,EXTERNAL://:29092 \
  -e KAFKA_ADVERTISED_LISTENERS="PLAINTEXT://localhost:9092,EXTERNAL://localhost:$KAFKA_PORT" \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e CLUSTER_ID=MkU3OEVBNTcwNTJENDM2Qk \
  apache/kafka:3.8.0 >/dev/null

for _ in $(seq 1 60); do
  docker exec "$PG_CONTAINER" pg_isready -U notification >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$PG_CONTAINER" pg_isready -U notification >/dev/null || {
  echo "FAIL: Postgres never became ready" >&2
  exit 1
}

for _ in $(seq 1 60); do
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh \
    --bootstrap-server localhost:9092 >/dev/null 2>&1 && break
  sleep 1
done
docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-broker-api-versions.sh \
  --bootstrap-server localhost:9092 >/dev/null || {
  echo "FAIL: Kafka never became ready" >&2
  exit 1
}

echo "-- starting notification-service (host JVM) --"
POSTGRES_HOST=localhost \
  POSTGRES_PORT="$PG_PORT" \
  POSTGRES_DB=notification \
  POSTGRES_USER=notification \
  POSTGRES_PASSWORD=notification \
  KAFKA_BOOTSTRAP_SERVERS="localhost:$KAFKA_PORT" \
  NOTIFICATION_SERVICE_PORT="$SERVICE_PORT" \
  NOTIFICATION_SERVICE_METRICS_PORT="$METRICS_PORT" \
  sbt -batch run >"$SERVICE_LOG" 2>&1 </dev/null &
SBT_PID=$!

for _ in $(seq 1 120); do
  curl -sf "http://localhost:$SERVICE_PORT/health/ready" >/dev/null 2>&1 && break
  if ! kill -0 "$SBT_PID" 2>/dev/null; then
    echo "FAIL: the service exited during startup" >&2
    tail -40 "$SERVICE_LOG" >&2
    exit 1
  fi
  sleep 1
done
curl -sf "http://localhost:$SERVICE_PORT/health/ready" >/dev/null || {
  echo "FAIL: notification-service never became ready" >&2
  tail -40 "$SERVICE_LOG" >&2
  exit 1
}

ORDER_ID="verify-order-$(uuid)"
CUSTOMER_ID="verify-customer-$(uuid)"
USER_ID="verify-user-$(uuid)"
EMAIL="$USER_ID@example.com"
EVENT_IDS=()
TOPIC_PAYLOADS=()

add_event() {
  local topic="$1" event_type="$2" payload="$3" event_id
  event_id="$(uuid)"
  EVENT_IDS+=("$event_id")
  TOPIC_PAYLOADS+=(
    "$topic|{\"eventId\":\"$event_id\",\"eventType\":\"$event_type\",\"timestamp\":\"2026-11-02T10:00:00Z\",\"source\":\"verify-sent-email-audit\",\"correlationId\":\"$event_id\",\"payload\":$payload}"
  )
}

add_event order-events OrderConfirmed \
  "{\"orderId\":\"$ORDER_ID\",\"customerId\":\"$CUSTOMER_ID\",\"timestamp\":\"2026-11-02T10:00:00Z\"}"
add_event payment-events PaymentCaptured \
  "{\"orderId\":\"$ORDER_ID\",\"amountCents\":4999,\"timestamp\":\"2026-11-02T10:00:00Z\"}"
add_event fulfilment-events DispatchConfirmed \
  "{\"orderId\":\"$ORDER_ID\",\"trackingNo\":\"TRK-VERIFY\",\"items\":[]}"
add_event user-events UserRegistered \
  "{\"userId\":\"$USER_ID\",\"email\":\"$EMAIL\"}"

echo "-- publishing one synthetic event on each topic --"
for entry in "${TOPIC_PAYLOADS[@]}"; do
  topic="${entry%%|*}"
  payload="${entry#*|}"
  printf '%s\n' "$payload" | docker exec -i "$KAFKA_CONTAINER" \
    /opt/kafka/bin/kafka-console-producer.sh \
    --bootstrap-server localhost:9092 --topic "$topic" >/dev/null
done

echo "-- waiting for the four audit rows --"
fetch_emails() {
  curl -sf "http://localhost:$SERVICE_PORT/emails?$1" || true
}

for _ in $(seq 1 60); do
  order_rows="$(fetch_emails "orderId=$ORDER_ID")"
  customer_rows="$(fetch_emails "customerId=$CUSTOMER_ID")"
  user_rows="$(fetch_emails "customerId=$USER_ID")"
  printf '%s' "$order_rows" | grep -q "\"sourceEventId\":\"${EVENT_IDS[0]}\"" &&
    printf '%s' "$order_rows" | grep -q "\"sourceEventId\":\"${EVENT_IDS[1]}\"" &&
    printf '%s' "$order_rows" | grep -q "\"sourceEventId\":\"${EVENT_IDS[2]}\"" &&
    printf '%s' "$user_rows" | grep -q "\"sourceEventId\":\"${EVENT_IDS[3]}\"" &&
    break
  sleep 1
done

echo "-- asserting the audit rows --"
assert_contains() {
  if ! printf '%s' "$2" | grep -q "$3"; then
    echo "FAIL: $1 is missing '$3'" >&2
    echo "$2" >&2
    echo "-- all audit rows --" >&2
    fetch_emails "" >&2 || true
    echo "-- service log tail --" >&2
    tail -60 "$SERVICE_LOG" >&2 || true
    exit 1
  fi
  echo "   OK: $1"
}

assert_contains "order email by orderId" "$order_rows" "\"sourceEventId\":\"${EVENT_IDS[0]}\""
assert_contains "payment email by orderId" "$order_rows" "\"sourceEventId\":\"${EVENT_IDS[1]}\""
assert_contains "fulfilment email by orderId" "$order_rows" "\"sourceEventId\":\"${EVENT_IDS[2]}\""
assert_contains "order email by customerId" "$customer_rows" "\"sourceEventId\":\"${EVENT_IDS[0]}\""
assert_contains "welcome email by customerId" "$user_rows" "\"sourceEventId\":\"${EVENT_IDS[3]}\""

assert_contains "order recipient is the customer" "$customer_rows" '"recipientType":"customer"'
assert_contains "order recipient key" "$customer_rows" "\"recipientKey\":\"$CUSTOMER_ID\""
assert_contains "payment recipient is the order" "$order_rows" '"recipientType":"order"'
assert_contains "welcome recipient is the email" "$user_rows" "\"recipientKey\":\"$EMAIL\""
assert_contains "welcome customer attribution" "$user_rows" "\"customerId\":\"$USER_ID\""
assert_contains "all four rows are sent" "$order_rows" '"status":"sent"'
assert_contains "welcome row is sent" "$user_rows" '"status":"sent"'
assert_contains "stub transport logged the send" "$(cat "$SERVICE_LOG")" "Recorded sent email"

echo
echo "All ADM-2 live checks passed."
