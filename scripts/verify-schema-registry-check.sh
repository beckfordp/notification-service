#!/usr/bin/env bash
set -euo pipefail

# Verifies notification-service's schemaRegistryCheck sbt task end-to-end
# (Rework 5 step 3, ADR 0003) against the real local Apicurio Registry
# instance (gluon's infra/k8s/local-infra). Mirrors the prior three
# services' own equivalent scripts - exercises the real production
# AsyncApiChannels registration in AsyncApiChannels.scala, registering this
# service's three per-outcome order schemas under the shared "gluon" group.
#
# Proves: a run registers the channel and sets a BACKWARD compatibility
# rule (first time only); a second identical run is a no-op (no error).
#
# Requires GITHUB_ACTOR/GITHUB_TOKEN (read:packages) to resolve
# sbt-purekafka-docs from GitHub Packages - derived here from `gh auth
# token` if not already set.
#
# Usage: ./scripts/verify-schema-registry-check.sh

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

export GITHUB_ACTOR="${GITHUB_ACTOR:-beckfordp}"
export GITHUB_TOKEN="${GITHUB_TOKEN:-$(gh auth token)}"

FAILED=0

echo "1. First run (registers the channel + sets the BACKWARD rule)..."
if sbt --error schemaRegistryCheck; then
  echo "   OK: first run succeeded"
else
  echo "FAIL: first run should have succeeded" >&2
  FAILED=1
fi

echo
echo "2. Checking the registry directly for the registered artifact..."
ARTIFACTS_URL="http://apicurio-registry.gluon-local.svc.cluster.local:8080/apis/registry/v3/groups/gluon/artifacts"
RESPONSE="$(curl -s "$ARTIFACTS_URL")"
for artifact in order-events-OrderConfirmed order-events-OrderPaymentFailed order-events-OrderCancelled; do
  if ! echo "$RESPONSE" | grep -q "$artifact"; then
    echo "FAIL: registry missing expected artifact: ${artifact}" >&2
    FAILED=1
  else
    echo "   OK: registry contains artifact \"${artifact}\""
  fi
done

echo
echo "3. Second, identical run (should be a no-op, not an error)..."
if sbt --error schemaRegistryCheck; then
  echo "   OK: second run succeeded"
else
  echo "FAIL: second identical run should have succeeded" >&2
  FAILED=1
fi

echo
if [ "$FAILED" -eq 0 ]; then
  echo "All checks passed."
  exit 0
else
  echo "One or more checks failed - see FAIL lines above." >&2
  exit 1
fi
