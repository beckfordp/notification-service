# Plan: ADM-10 — guest email fallback

## Phase 1: Fallback

- [x] Task: Failing `OrderClientSuite` (guest email decoded) and
  `RecipientResolverSuite` (order event guest, orderId-only single lookup,
  guest without email terminal, signed-in no lookup); implement.
- [x] Task: Conductor - User Manual Verification 'Fallback'
  (Protocol in workflow.md) — prompting off: full suite green (113 passed).

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass;
  transient order-lookup failures still raise for retry/DLQ. No blocking
  findings.
