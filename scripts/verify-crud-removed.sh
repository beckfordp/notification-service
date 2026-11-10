#!/usr/bin/env bash
# SPDX-License-Identifier: MIT

set -euo pipefail

# Verifies Phase 2 of track strip-crud_20261002 (remove Postgres/CRUD scaffold):
# brings up docker-compose (Kafka only; no Postgres) + a real sbt run
# instance with NO Postgres running anywhere, then confirms the service still
# boots and serves health checks, and that the old CRUD surface is gone.
#
# Usage: ./scripts/verify-crud-removed.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

FAILED=0
SBT_PID=""

cleanup() {
  echo
  echo "Cleaning up..."
  [ -n "$SBT_PID" ] && kill "$SBT_PID" >/dev/null 2>&1 || true
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

echo "1. docker-compose.yml declares no Postgres service..."
postgres_count="$(docker compose config --services | grep -cx 'postgres' || true)"
check "docker-compose.yml declares no Postgres service" "0" "$postgres_count"

echo
echo "2. Starting notification-service (sbt run) in the background, no GITHUB_TOKEN-for-Postgres needed..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run >/tmp/notification-service-verify.log 2>&1 &
SBT_PID=$!
wait_ready
echo "   OK: notification-service ready on :8080 with no Postgres running"

echo
echo "3. Health and readiness endpoints..."
health="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health)"
check "GET /health returns 200" "200" "$health"
ready="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health/ready)"
check "GET /health/ready returns 200" "200" "$ready"

echo
echo "4. The old CRUD surface is gone..."
create_status="$(curl -s -o /dev/null -w '%{http_code}' -X POST http://localhost:8080/notifications \
  -H "Content-Type: application/json" -d '{}')"
check "POST /notifications returns 404 (route no longer exists)" "404" "$create_status"
docs_status="$(curl -s -L -o /dev/null -w '%{http_code}' http://localhost:8080/docs)"
check "GET /docs returns 200 (Swagger UI; TD-20.9)" "200" "$docs_status"

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
else
  echo "One or more checks FAILED. See above." >&2
  exit 1
fi
