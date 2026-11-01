# Notification review fixes — Implementation Plan

## Phase 1 — Shared envelope handling (H1, H2, M1, M2, M3)

- [x] Task: Shared `EnvelopeHandler` (failing tests first): null skipped; bad envelope and bad known payload dead-lettered raw; unknown type logged; known type guarded. — 102f7d9
- [~] Task: Route all four consumers through it via a broker-free `handle`, narrowed to `MonadThrow`; `EmailClient.logging` unconstrained; per-consumer handler tests.
- [ ] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md) — suite

## Phase 2 — Consumer lifecycle and cleanup (M4, L1, L2)

- [ ] Task: Surface consumer-fiber failure: `KafkaConsumers.resource` yields a watcher that logs and raises; `Main` fails on it.
- [ ] Task: Sign-aware `formatAmount`; stale-doc fixes.
- [ ] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md) — suite

**Risks:** DLQ on decode failure retries `dlqRetry` times before publishing (adds ~1s per bad record, payment-service precedent); `Main` now exits if consumers die (intended).
