# Notification review fixes — Implementation Plan

## Phase 1 — Shared envelope handling (H1, H2, M1, M2, M3) [checkpoint: ac04dcc]

- [x] Task: Shared `EnvelopeHandler` (failing tests first): null skipped; bad envelope and bad known payload dead-lettered raw; unknown type logged; known type guarded. — 102f7d9
- [x] Task: Route all four consumers through it via a broker-free `handle`, narrowed to `MonadThrow`; `EmailClient.logging` unconstrained; per-consumer handler tests. — d9361e8
- [x] Task: Conductor - User Manual Verification 'Phase 1' (Protocol in workflow.md) — suite (52 green) — ac04dcc

## Phase 2 — Consumer lifecycle and cleanup (M4, L1, L2) [checkpoint: ee4707f]

- [x] Task: Surface consumer-fiber failure: `KafkaConsumers.resource` yields a watcher that logs and raises; `Main` fails on it. — 07bcbb9
- [x] Task: Sign-aware `formatAmount`; stale-doc fixes. — 0fd5675
- [x] Task: Conductor - User Manual Verification 'Phase 2' (Protocol in workflow.md) — suite (56 green) — ee4707f

**Risks:** DLQ on decode failure retries `dlqRetry` times before publishing (adds ~1s per bad record, payment-service precedent); `Main` now exits if consumers die (intended).

## Phase: Review Fixes

- [x] Task: Apply review suggestions — reuse the shared `SentEmail` fixture in `KafkaConsumersSuite`/`OrderEventsConsumerSuite` (were duplicate local copies); rename `TestDeadLetter.scala` → `TestFixtures.scala` (holds both fixtures). Accepted, not changed: `EnvelopeHandler` rebuilds its route map per record (4 entries max); decode failures pass through `dlqRetry` before the DLQ publish (payment-service precedent). — d2261ad
