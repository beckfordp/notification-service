# Plan: US-9.9 — reason-specific cancellation email

## Phase 1: Copy

- [x] Task: Failing tests — each reason's body, None/unknown neutral (Red)
- [x] Task: Add optional reason to the mirror; branch `cancelledEmail` (Green)
- [x] Task: `sbt scalafmtCheck test` and `schemaRegistryCheck` green
- [x] Task: Conductor - User Manual Verification 'Copy' (Protocol in workflow.md)

## Review (2026-11-02)

- `sbt scalafmtCheck test` green (59 tests, 6 pre-existing ignored);
  `sbt schemaRegistryCheck` green.
- Live path covered by `verify-auth-expiry.sh`: expiry copy observed in the
  notification pod logs; unit tests cover every reason and the None fallback.
- No blocking findings.
