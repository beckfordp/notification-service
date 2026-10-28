#!/usr/bin/env bash
# Live verification for track fulfilment-emails_20261028 (US-17.5/18.4).
# Synthetic DispatchConfirmed/DispatchFailed and a real dispatched order must
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
    --topic fulfilment-events >/dev/null
}
envelope() {
  python3 -c "
import json,uuid,sys
order=sys.argv[1]; etype=sys.argv[2]; payload=json.loads(sys.argv[3])
print(json.dumps({'eventId':str(uuid.uuid4()),'eventType':etype,'timestamp':'2026-10-28T12:00:00Z','source':'verify-script','correlationId':order,'payload':payload}))" "$1" "$2" "$3"
}

echo "== fulfilment emails live verification =="
start_pf order-service 18080
start_pf payment-service 18081
start_pf inventory-service 18082
start_pf fulfilment-service 18085
wait_ready "$ORDER_URL" order-service
wait_ready "$PAYMENT_URL" payment-service
wait_ready "$INVENTORY_URL" inventory-service
wait_ready "$FULFILMENT_URL" fulfilment-service

echo "-- synthetic shipped + failed emails --"
SYNTH_OK="synthetic-$(python3 -c 'import uuid; print(uuid.uuid4())')"
publish "$(envelope "$SYNTH_OK" DispatchConfirmed "{\"orderId\":\"$SYNTH_OK\",\"trackingNo\":\"GLN-SYNTH\",\"items\":[{\"sku\":\"sku-1\",\"productName\":\"Watch\",\"quantity\":1,\"unitPriceCents\":100}]}")"
wait_log "order_id=$SYNTH_OK"
logs | grep "order_id=$SYNTH_OK" | grep -q "outcome=shipped" || { echo "FAIL: no shipped log for $SYNTH_OK" >&2; exit 1; }
SYNTH_FAIL="synthetic-$(python3 -c 'import uuid; print(uuid.uuid4())')"
publish "$(envelope "$SYNTH_FAIL" DispatchFailed "{\"orderId\":\"$SYNTH_FAIL\",\"reason\":\"damaged\"}")"
wait_log "order_id=$SYNTH_FAIL"
logs | grep "order_id=$SYNTH_FAIL" | grep -q "outcome=dispatch_failed" || { echo "FAIL: no failed log for $SYNTH_FAIL" >&2; exit 1; }
echo "   shipped + dispatch_failed stub logs observed"

echo "-- real dispatched order produces the shipped email --"
ADDRESS='{"line1":"1 Verification Lane","city":"London","postalCode":"EC1A 1BB","country":"GB"}'
CUSTOMER="$(python3 -c 'import uuid; print(uuid.uuid4())')"
SKU="$(curl -sf "$INVENTORY_URL/inventorys" | python3 -c '
import json,sys
rows=json.load(sys.stdin); stocked=[r for r in rows if r["quantityAvailable"]>0]
if not stocked: sys.exit("no stocked sku")
print(stocked[0]["sku"])')"
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
logs | grep "order_id=$ORDER_ID" | grep -q "outcome=shipped" || { echo "FAIL: no real shipped email" >&2; exit 1; }
echo "   shipped email for real order $ORDER_ID"

echo "-- cleaning up --"
curl -sf -X POST "$INVENTORY_URL/inventorys/order-reservations/$ORDER_ID/release" >/dev/null || true
curl -sf -X DELETE "$ORDER_URL/orders/$ORDER_ID" >/dev/null || true
kubectl exec -n "$NAMESPACE" deploy/fulfilment-postgres -- psql -U fulfilment -d fulfilment -q -c \
  "delete from outbox_events where aggregate_id = '$ORDER_ID'; delete from fulfilment_task where order_id = '$ORDER_ID'" >/dev/null

echo "All fulfilment-email live checks passed."
