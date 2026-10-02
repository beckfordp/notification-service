#!/usr/bin/env bash
set -euo pipefail

# Verifies Phase 1 of track strip-crud_20261002 (store-less readiness check):
# brings up Postgres (still required to boot the service at this point in the
# track - Main.scala still runs Migrations/wires NotificationStore.postgres
# until Phase 2) + a real sbt run instance, then confirms /health/ready
# answers unconditionally (no longer pinging the store) alongside /health.
#
# Usage: ./scripts/verify-health-ready.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

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
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
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

echo "1. docker compose up -d (fresh volumes)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d
echo "   OK: postgres starting"

echo
echo "2. Starting notification-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/notification-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: notification-service ready on :8080"

echo
echo "3. Health and readiness endpoints..."
health="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health)"
check "GET /health returns 200" "200" "$health"
ready="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready)"
check "GET /health/ready returns 200 (no store dependency)" "200" "$ready"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
