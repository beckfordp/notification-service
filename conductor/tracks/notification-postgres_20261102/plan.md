# Plan: ADM-6 — notification Postgres readiness

## Phase 1: Readiness

- [x] Task: Failing `HealthRoutesSuite` (ready 200/503, liveness ignores DB)
  and Postgres `ping` tests (reachable/unreachable); add `ping`, gate
  `readyServerEndpoint`, update stubs, docs suite and `verify-rest-docs.sh`.
- [x] Task: Conductor - User Manual Verification 'Readiness'
  (Protocol in workflow.md) — prompting off: full suite green (108 passed)
  plus live cluster rollout with `notification-postgres`.

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass;
  readiness mirrors payment-service's ping gate; no blocking findings.
