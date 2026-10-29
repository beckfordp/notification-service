#!/usr/bin/env bash
set -euo pipefail

# Verifies notification-service's AsyncAPI documentation end-to-end,
# generated via the sbt-purekafka-docs plugin (asyncApiGenerateSpec/
# asyncApiGenerate, auto-chained after Compile/compile) — generation needs no
# running server at all, since AsyncApiSpecWriter reads AsyncApiChannels
# directly via reflection rather than curling a live HTTP route. Confirms the
# registered event type shows up in the generated spec/HTML *and* the
# live-served raw spec + static HTML routes, so both halves (build-time
# generation and runtime serving) are checked, not just that a process
# started or a task exited 0. Also confirms /health/ready returns 200
# unconditionally (TD-2.3's readiness simplification). Adapted from
# inventory-service's/order-service's own verify-kafka-asyncapi-docs.sh.
#
# No order-service produces a real order lifecycle event here -
# notification-service starts with no inbound traffic, same as
# inventory-service's own verify script; this only checks the AsyncAPI docs
# + health surface, not the actual email-sending flow.
#
# Usage: ./scripts/verify-kafka-asyncapi-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

NOTIFICATION_SERVICE_URL="http://localhost:8080"
FAILED=0
SBT_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
  docker compose down -v >/dev/null 2>&1 || true
}
trap cleanup EXIT

wait_ready() {
  for _ in $(seq 1 90); do
    status="$(curl -s -o /dev/null -w '%{http_code}' "${NOTIFICATION_SERVICE_URL}/health" || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: notification-service did not become ready in time." >&2
  return 1
}

echo "1. sbt asyncApiGenerate (no server needed - AsyncApiSpecWriter reads AsyncApiChannels directly)..."
sbt asyncApiGenerate

echo
echo "2. Checking the generated raw spec for all 8 registered event types..."
SPEC_FILE="target/asyncapi-docs-spec/asyncapi.json"
for needle in "order-events" "OrderConfirmed" "OrderPaymentFailed" "OrderCancelled" "UserRegistered" "DispatchConfirmed" "DispatchFailed" "PaymentCaptured" "PaymentFailed"; do
  if ! grep -q "$needle" "$SPEC_FILE"; then
    echo "FAIL: generated spec missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated spec contains \"${needle}\""
  fi
done
if grep -q "OrderStatusChanged" "$SPEC_FILE"; then
  echo "FAIL: generated spec still contains retired content: OrderStatusChanged" >&2
  FAILED=1
else
  echo "   OK: generated spec omits retired \"OrderStatusChanged\""
fi

echo
echo "3. Checking the generated static HTML for the same content..."
HTML_FILE="target/asyncapi-docs/index.html"
for needle in "OrderConfirmed" "OrderPaymentFailed" "OrderCancelled"; do
  if ! grep -q "$needle" "$HTML_FILE"; then
    echo "FAIL: generated HTML missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: generated HTML contains \"${needle}\""
  fi
done

echo
echo "4. docker compose up -d (fresh volumes, kafka)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   waiting for kafka to be healthy..."
for _ in $(seq 1 60); do
  health="$(docker compose ps --format json kafka 2>/dev/null | python3 -c "import json,sys; print(json.load(sys.stdin).get('Health',''))" 2>/dev/null || true)"
  [ "$health" = "healthy" ] && break
  sleep 1
done

echo
echo "5. Starting notification-service (sbt run)..."
sbt run >/tmp/notification-service-asyncapi-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: notification-service ready on :8080"

echo
echo "6. Checking /health/ready returns 200 unconditionally (TD-2.3 readiness simplification)..."
ready_status="$(curl -s -o /dev/null -w '%{http_code}' "${NOTIFICATION_SERVICE_URL}/health/ready")"
if [ "$ready_status" != "200" ]; then
  echo "FAIL: /health/ready returned ${ready_status}, expected 200" >&2
  FAILED=1
else
  echo "   OK: /health/ready returned 200"
fi

echo
echo "7. Checking the live raw-spec route (/docs/asyncapi.json) for the same content..."
for needle in "OrderConfirmed" "OrderPaymentFailed" "OrderCancelled"; do
  if ! curl -s "${NOTIFICATION_SERVICE_URL}/docs/asyncapi.json" | grep -q "$needle"; then
    echo "FAIL: live raw-spec route missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: live raw-spec route contains \"${needle}\""
  fi
done

echo
echo "8. Checking the live static HTML route (/docs/asyncapi/) for the same content..."
for needle in "OrderConfirmed" "OrderPaymentFailed" "OrderCancelled"; do
  ok=0
  for _ in $(seq 1 30); do
    if curl -s "${NOTIFICATION_SERVICE_URL}/docs/asyncapi/index.html" | grep -q "$needle"; then
      ok=1
      break
    fi
    sleep 1
  done
  if [ "$ok" -ne 1 ]; then
    echo "FAIL: live static HTML route missing expected content: ${needle}" >&2
    FAILED=1
  else
    echo "   OK: live static HTML route contains \"${needle}\""
  fi
done

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed — see FAIL lines above." >&2
  exit 1
fi
