#!/usr/bin/env bash
set -euo pipefail

# Verifies the REST docs convention end-to-end (track rest-docs_20261101,
# gluon TD-20.9): starts Kafka and a real sbt run instance, fetches the served
# OpenAPI spec at /docs/docs.yaml, and checks both health endpoints carry their
# summary/description/tag, declare no parameters, bodies or undocumented error
# responses, and leave /docs (Swagger UI) and /docs/asyncapi.json serving.
# Complements NotificationDocsSuite's in-process Docs.undocumented check.
#
# Requires: sbt, Docker, python3 with PyYAML, GITHUB_TOKEN/GITHUB_ACTOR set
# (purerestlib resolution).
#
# Usage: ./scripts/verify-rest-docs.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

SBT_PID=""
SPEC="$(mktemp)"

cleanup() {
  echo
  echo "Cleaning up..."
  if [ -n "$SBT_PID" ]; then
    pkill -9 -P "$SBT_PID" >/dev/null 2>&1 || true
    kill -9 "$SBT_PID" >/dev/null 2>&1 || true
  fi
  docker compose down -v >/dev/null 2>&1 || true
  rm -f "$SPEC"
}
trap cleanup EXIT

wait_health() {
  for _ in $(seq 1 90); do
    status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/health || true)"
    [ "$status" = "200" ] && return 0
    sleep 1
  done
  echo "FAIL: notification-service did not become ready in time." >&2
  return 1
}

echo "1. docker compose up -d --wait (Kafka broker)..."
docker compose down -v >/dev/null 2>&1 || true
docker compose up -d --wait --wait-timeout 120
echo "   OK: Kafka healthy"

echo
echo "2. Starting notification-service (sbt run) in the background..."
if [ -z "${GITHUB_TOKEN:-}" ] || [ -z "${GITHUB_ACTOR:-}" ]; then
  echo "WARN: GITHUB_TOKEN / GITHUB_ACTOR not set - resolving purerestlib from" >&2
  echo "      GitHub Packages will fail without a read:packages token." >&2
fi
sbt run </dev/null >/tmp/notification-service-verify-rest-docs.log 2>&1 &
SBT_PID=$!
wait_health
echo "   OK: notification-service ready on :8080"

echo
echo "3. GET /docs/docs.yaml, /docs and /docs/asyncapi.json..."
status="$(curl -s -o "$SPEC" -w '%{http_code}' http://localhost:8080/docs/docs.yaml)"
[ "$status" = "200" ] || { echo "FAIL: /docs/docs.yaml returned $status" >&2; exit 1; }
echo "   OK: /docs/docs.yaml served (200)"
status="$(curl -s -L -o /dev/null -w '%{http_code}' http://localhost:8080/docs)"
[ "$status" = "200" ] || { echo "FAIL: /docs returned $status" >&2; exit 1; }
echo "   OK: Swagger UI /docs served (200)"
status="$(curl -s -o /dev/null -w '%{http_code}' http://localhost:8080/docs/asyncapi.json)"
[ "$status" = "200" ] || { echo "FAIL: /docs/asyncapi.json returned $status" >&2; exit 1; }
echo "   OK: /docs/asyncapi.json still served (200)"

echo
echo "4. Checking the spec..."
python3 -I - "$SPEC" <<'PY'
import sys, yaml

spec = yaml.safe_load(open(sys.argv[1]))
paths = spec["paths"]
failed = False

def check(label, ok):
    global failed
    print(("   OK: " if ok else "   FAIL: ") + label, file=sys.stdout if ok else sys.stderr)
    failed |= not ok

expected = {
    ("/health", "get"): {
        "summary": "Liveness check",
        "description": "200 while the process is up. Checks no dependencies.",
    },
    ("/health/ready", "get"): {
        "summary": "Readiness check",
        "description": (
            "Always 200 while the process is up: this service has no datastore "
            "to gate readiness on, and the Kafka consumers are supervised with "
            "restart-with-backoff rather than left permanently dead, so there "
            "is no not-ready state to report."
        ),
    },
}

for (path, method), want in expected.items():
    op = paths.get(path, {}).get(method)
    name = f"{method.upper()} {path}"
    if op is None:
        check(f"{name} documented", False)
        continue
    check(f"{name} has the approved summary", op.get("summary") == want["summary"])
    check(f"{name} has the approved description",
          op.get("description") == want["description"])
    check(f"{name} tagged 'health'", op.get("tags") == ["health"])
    check(f"{name} takes no parameters", not op.get("parameters"))
    check(f"{name} has no request body", "requestBody" not in op)
    responses = op.get("responses", {})
    check(f"{name} lists only 200 (got {sorted(responses)})",
          set(responses) <= {"200"})
    for code, r in responses.items():
        if code.startswith("2"):
            continue
        check(f"{name} {code} described", bool(r.get("description")))

check("both operations documented", len(paths) == 2)
check("no component schemas (no DTOs)", not spec.get("components", {}).get("schemas"))

sys.exit(1 if failed else 0)
PY

echo
echo "All checks passed."
