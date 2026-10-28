#!/usr/bin/env bash
# Live verification for track payment-emails_20261028 (US-19.4).
# Synthetic PaymentCaptured/PaymentFailed and a real dispatched order must
# each produce the matching stub email log. Cleans up test data.
set -euo pipefail

NAMESPACE="${NAMESPACE:-gluon-local}"
ORDER_URL="${ORDER_URL:-http://localhost:18080}"
PAYMENT_URL="${PAYMENT_URL:-http://localhost:18081}"
INVENTORY_URL="${INVENTORY_URL:-http://localhost:18082}"
FULFILMENT_URL="${FULFILMENT_URL:-http://localhost:18085}"
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
start_pf() { kubectl -n "$NAMESPACE" port-forward "svc/$1" "$2:8080" >/dev/null 2>&1 & PIDS+=("$!"); }
wait_ready() { for _ in $(seq 1 30); do curl -sf "$1/health/ready" >/dev/null 2>&1 && return 0; sleep 1; done; echo "FAIL: $2 not ready" >&2; exit 1; }
post_json() { curl -sS -o "$3" -w '%{http_code}' -X POST "$1" -H 'Content-Type: application/json' -d "$2"; }
json_field() { python3 -c "import json,sys; print(json.load(sys.stdin)$1)" < "$2"; }
logs() { kubectl logs -n "$NAMESPACE" deploy/notification-service --since=30m 2>/dev/null || true; }
wait_log() {
  local needle="$1"
  for _ in $(seq 1 30); do logs | grep -q "$needle" && return 0; sleep 1; done
  echo "FAIL: notification-service never logged: $needle" >&2
  logs | tail -20 >&2
  exit 1
}
publish() {
  printf '%s\n' "$1" | kubectl exec -i -n "$NAMESPACE" deploy/kafka -- \
    /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
    --topic payment-events >/dev/null
}
stocked_sku() {
  curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])'
}
echo "== payment emails live verification =="
start_pf order-service 18080
start_pf payment-service 18081
start_pf inventory-service 18082
start_pf fulfilment-service 18085
wait_ready "$ORDER_URL" order-service
wait_ready "$PAYMENT_URL" payment-service
wait_ready "$INVENTORY_URL" inventory-service
wait_ready "$FULFILMENT_URL" fulfilment-service

echo "-- synthetic receipt + payment-problem emails --"
SYNTH_OK="synthetic-$(python3 -c 'import uuid; print(uuid.uuid4())')"
EVENT_OK="$(python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'PaymentCaptured','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$SYNTH_OK','payload':{'orderId':'$SYNTH_OK','amountCents':1234,'timestamp':'2026-10-28T12:00:00Z'}}))")"
publish "$EVENT_OK"
wait_log "order_id=$SYNTH_OK"
logs | grep "order_id=$SYNTH_OK" | grep -q "outcome=payment_captured" || { echo "FAIL: no receipt log for $SYNTH_OK" >&2; exit 1; }
logs | grep "$SYNTH_OK" | grep -q '\$12.34' || { echo "FAIL: receipt body missing formatted amount" >&2; exit 1; }
SYNTH_FAIL="synthetic-$(python3 -c 'import uuid; print(uuid.uuid4())')"
EVENT_FAIL="$(python3 -c "
import json,uuid
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':'PaymentFailed','timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':'$SYNTH_FAIL','payload':{'orderId':'$SYNTH_FAIL','paymentId':str(uuid.uuid4()),'amountCents':1234,'reason':'capture_failed','timestamp':'2026-10-28T12:00:00Z'}}))")"
publish "$EVENT_FAIL"
wait_log "order_id=$SYNTH_FAIL"
logs | grep "order_id=$SYNTH_FAIL" | grep -q "outcome=payment_failed" || { echo "FAIL: no payment-problem log for $SYNTH_FAIL" >&2; exit 1; }
echo "   receipt + payment-problem stub logs observed"

echo "-- real dispatched order produces the receipt email --"
ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SKU="$(stocked_sku)"
STATUS="$(post_json "$PAYMENT_URL/payments/methods" \
  "{\"customerId\":\"$CUSTOMER\",\"cardNumber\":\"4242424242424242\",\"expiry\":\"12/30\",\"billingAddress\":$ADDRESS}" "$TMP_DIR/pm.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: payment method -> $STATUS" >&2; exit 1; }
PM="$(json_field '["id"]' "$TMP_DIR/pm.json")"
STATUS="$(post_json "$ORDER_URL/orders" \
  "{\"customerId\":\"$CUSTOMER\",\"items\":[{\"sku\":\"$SKU\",\"productName\":\"Verify Watch\",\"unitPriceCents\":100,\"quantity\":1}],\"shippingAddress\":$ADDRESS,\"paymentMethodId\":\"$PM\"}" "$TMP_DIR/order.json")"
[ "$STATUS" = "201" ] || { echo "FAIL: order -> $STATUS" >&2; exit 1; }
ORDER_ID="$(json_field '["id"]' "$TMP_DIR/order.json")"
for _ in $(seq 1 30); do
  curl -sf "$FULFILMENT_URL/fulfilment-tasks?state=PENDING" | grep -q "$ORDER_ID" && break
  sleep 1
done
for _ in 1 2 3; do
  post_json "$FULFILMENT_URL/fulfilment-tasks/$ORDER_ID/simulate-next" '{}' "$TMP_DIR/sim.json" >/dev/null
done
wait_log "order_id=$ORDER_ID"
logs | grep "order_id=$ORDER_ID" | grep -q "outcome=payment_captured" || { echo "FAIL: no real receipt email" >&2; exit 1; }
echo "   receipt email for real order $ORDER_ID"

echo "-- cleaning up --"
curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$ORDER_ID/release" >/dev/null || true
curl -sf -X DELETE "$ORDER_URL/orders/$ORDER_ID" >/dev/null || true
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from fulfilment_task where order_id = '$ORDER_ID'" >/dev/null
kubectl exec -n "$NAMESPACE" deploy/payment-postgres -- psql -U payment -d payment -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from payment where order_id = '$ORDER_ID'; delete from payment_methods where customer_id = '$CUSTOMER'" >/dev/null
kubectl exec -n "$NAMESPACE" deploy/order-postgres -- psql -U order -d order -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from order_reconciliation where order_id = '$ORDER_ID'" >/dev/null || true

echo "All payment-email live checks passed."
