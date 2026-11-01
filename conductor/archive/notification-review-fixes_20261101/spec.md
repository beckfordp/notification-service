# Spec: notification-service review fixes (gluon TD-16.6)

## Overview
Full review of notification-service main + test code against
`code_styleguides/scala-fp.md` (Review checklist) and `general.md`, 2026-11-01.
Baseline: 38 tests green, 6 ignored (`OrderEventsConsumerSuite`, separate backlog
item) at `de9b0d3`. 2 High / 4 Medium / 7 Low.

## Findings

### High
- **H1 Consumer logic untested.** Per-record handling (null value, envelope/payload
  decode, eventType dispatch, DLQ guard) lives inside each `run` stream, so it is
  only exercised by the ignored `OrderEventsConsumerSuite` and one
  `KafkaConsumersSuite` test. Fulfilment/payment/user consumer dispatch has no test.
  Fix: extract a broker-free `handle(key, value)` per consumer; test each.
- **H2 Over-broad capabilities.** Per-message helpers (`send`, `handleConfirmed`, ...)
  require `Async`; `EmailClient.logging` requires `Sync` but never `delay`s. Fix:
  `MonadThrow` for handlers, no constraint for `EmailClient.logging`; `Async` stays
  only on the Kafka streams and composition root.

### Medium
- **M1 Dispatch duplicated 4x.** The null/envelope/payload/unknown-type decision is
  copy-pasted across the four consumers (§1 "defined once"). Fix: one shared
  `EnvelopeHandler`; consumers supply only `eventType -> handler` routes.
- **M2 Undecodable records silently lost.** Bad envelope / bad known-type payload is
  logged and the offset committed. Fix: route raw to `<topic>-dlq` (payment-service
  `payment-review-fixes` FR-3 precedent). Unknown event types stay log-and-skip
  (these topics carry other services' events by design).
- **M3 `UserRegisteredConsumer` swallows unknown event types** (`Async[F].unit`), §6.
  Fix: log like the other consumers (falls out of M1).
- **M4 Consumer fiber death is silent.** `KafkaConsumers.resource` does
  `.background.void`: a failure outside `StreamSupervisor` (e.g. DLQ producer
  acquisition) kills every consumer, logs nothing, and `/health` stays 200. Fix:
  expose the fiber's outcome; log and fail `Main` so k8s restarts the pod.

### Low
- **L1** `PaymentEventConsumer.formatAmount` renders negative cents as `$-1.-50`.
  Fix: sign-aware formatting (cheap).
- **L2** Stale docs: `AsyncApiChannels` scaladoc (lists 2 of 4 topics),
  `KafkaConsumersSuite` scaladoc ("only one consumer"), `EmailClient` scaladoc
  (`to` is customerId/orderId/email), `tech-stack.md` (purerestlib 0.1.0,
  `OrderStatusChangedConsumer`, 2 channels). Fix.
- **L3** Ids are raw `String`s (no opaque types). Skipped: mirrors come from gluon's
  `registry/` via `bin/registry-scala`; ids are pass-through only, never parsed or
  compared.
- **L4** Null-value records are logged and skipped, not dead-lettered. Deferred
  (same as TD-16.1's Low; `DeadLetter[String, String]` cannot carry a null value).
- **L5** `EmailClient.logging` logs recipient (an email address for welcome mails)
  and body. Skipped: it is the stub's purpose until a real provider (PLAN Phase 5);
  verify scripts assert on it.
- **L6** Merge isolation (one consumer crashing doesn't restart the other three) is
  untested here. Deferred: needs a Testcontainers broker; `StreamSupervisor` itself
  is covered in purekafka.
- **L7** CORS allows POST/PATCH/PUT/DELETE though only GET is served. Skipped:
  platform-consistent policy, documented in `tech-stack.md`.

## Non-Functional
- TDD per workflow; scalafmt clean; no REST or Kafka wire-contract change.

## Acceptance Criteria
- Each fixed finding has a failing-first test now green.
- Full suite green (ignored suite unchanged).

## Out of Scope
- Un-ignoring `OrderEventsConsumerSuite` (backlog item); real email provider.
