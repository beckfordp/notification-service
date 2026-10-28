#!/usr/bin/env bash
# Live verification for track welcome-email_20261028 (US-16.4).
# 1) publishes a synthetic UserRegistered envelope to user-events and asserts
#    notification-service logs the welcome email;
# 2) signs up a real user through user-service and asserts the same welcome
#    email is sent end to end.
# Cleans up its test account afterwards.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
USER_URL="${USER_URL:-http://localhost:18083}"
PIDS=()
TMP_DIR="$(mktemp -d)"
cleanup() {
  for pid in "${PIDS[@]}"; do
    kill "$pid" 2>/dev/null || true
    wait "$pid" 2>/dev/null || true
  done
  rm -rf "$TMP_DIR"
}
trap cleanup EXIT

psql_user() {
  kubectl exec -n "$NAMESPACE" deploy/user-postgres -- \
    psql -U user -d user -tAc "$1" | tr -d '[:space:]'
}

notification_logs() {
  kubectl logs -n "$NAMESPACE" deploy/notification-service --since=30m 2>/dev/null || true
}

wait_for_log() {
  local needle="$1"
  for _ in $(seq 1 30); do
    if notification_logs | grep -q "$needle"; then
      return 0
    fi
    sleep 1
  done
  echo "FAIL: notification-service never logged: $needle" >&2
  notification_logs | tail -20 >&2
  return 1
}

echo "== US-16.4 live verification =="
kubectl -n "$NAMESPACE" port-forward svc/user-service 18083:8080 >/dev/null 2>&1 &
PIDS+=("$!")
for _ in $(seq 1 30); do
  curl -sf "$USER_URL/health/ready" >/dev/null 2>&1 && break
  sleep 1
done
curl -sf "$USER_URL/health/ready" >/dev/null || {
  echo "FAIL: user-service never became ready" >&2
  exit 1
}

echo "-- synthetic UserRegistered --"
SYNTH_USER="synthetic-$(python3 -c 'import uuid; print(uuid.uuid4())')"
SYNTH_EMAIL="${SYNTH_USER}@example.com"
cat > "$TMP_DIR/synthetic.json" <<EOF
{"eventId":"$(python3 -c 'import uuid; print(uuid.uuid4())')","eventType":"UserRegistered","timestamp":"2026-10-28T12:00:00Z","source":"verify-script","correlationId":"$SYNTH_USER","payload":{"userId":"$SYNTH_USER","email":"$SYNTH_EMAIL"}}
EOF
kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
  /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
  --topic user-events < "$TMP_DIR/synthetic.json" >/dev/null
wait_for_log "user_id=$SYNTH_USER"
wait_for_log "to=$SYNTH_EMAIL"
echo "   welcome email logged for the synthetic event"

echo "-- real signup end to end --"
EMAIL="verify-$(python3 -c 'import uuid; print(uuid.uuid4())')@example.com"
PASSWORD="correct horse battery"
STATUS="$(curl -sS -o "$TMP_DIR/signup.json" -w '%{http_code}' -X POST \
  "$USER_URL/auth/signup" -H 'Content-Type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}")"
[ "$STATUS" = "201" ] || { echo "FAIL: signup -> $STATUS" >&2; cat "$TMP_DIR/signup.json" >&2; exit 1; }
USER_ID="$(python3 -c 'import json,sys; print(json.load(sys.stdin)["userId"])' < "$TMP_DIR/signup.json")"
wait_for_log "user_id=$USER_ID"
wait_for_log "to=$EMAIL"
echo "   welcome email logged for the real signup ($USER_ID)"

echo "-- cleaning up the test account --"
kubectl exec -n "$NAMESPACE" deploy/user-postgres -- psql -U user -d user -q -c \
  "delete from outbox_events where aggregate_id = '$USER_ID'; delete from user_account where id = '$USER_ID'" >/dev/null

echo "All US-16.4 live checks passed."
