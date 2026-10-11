# Spec: Migrate to order-events with envelope

## Overview
Migrate notification-service's Kafka consume from the per-event-type topic
`order.status-changed` to the domain topic `order-events`, filtering by
`eventType == "OrderStatusChanged"` against a shared envelope that carries
`eventType` alongside the payload. Part of a coordinated cross-repo cutover
([ADR 0009](../../../../docs/adr/0009-one-kafka-topic-per-domain.md),
Accepted 2026-10-11; `gluon/PLAN.md`'s Rework 2) — inventory-service,
payment-service, and order-service (all `kafka-envelope_20261011`) already
migrated their own sides. notification-service is the simplest of the four:
a pure consumer, no publish side, and it only cares about one of
`order-events`'s two event types (`OrderReserved` is irrelevant here and
must be silently skipped, not logged as malformed).

## Functional Requirements
- New `EventEnvelope` case class (same shape as every other repo's copy, no
  shared library — matching this codebase's existing pattern): `eventId`,
  `eventType`, `timestamp`, `source`, `correlationId`, `payload: Json`.
- `OrderStatusChangedConsumer` subscribes to `order-events` (was
  `order.status-changed`). Decodes the envelope first (preserving the
  existing `Option[String]`/`Option[String]` null-safe key/value
  deserializer and the null-value-record handling); if `eventType !=
  "OrderStatusChanged"` (e.g. `OrderReserved`), silently skip
  (`Async[F].unit`) — not logged as malformed. If `eventType ==
  "OrderStatusChanged"`, decode `envelope.payload` as
  `OrderStatusChangedEvent` and proceed exactly as before (send the
  matching email, or log a decode failure for a bad payload).
- Keep the existing `ready: Ref[F, Boolean]` / `onFinalizeCase` health-check
  wiring unchanged — only a genuine stream crash flips `/health/ready`.
- `OrderStatusChangedEvent`/`OrderStatusChanged` (the payload shape) are
  unchanged — only how the raw Kafka record is unwrapped before reaching
  them changes.

## Non-Functional Requirements
- No new dependency.
- Existing at-least-once/commit-after-process risk profile unchanged.
- `OrderStatusChangedConsumerSuite`'s Kafka integration tests stay
  `.ignore`'d in this dev environment (pre-existing Testcontainers/
  Docker-Desktop compatibility issue — see the suite's own header comment,
  tracked separately in this repo's Backlog) — still rewritten for the new
  envelope shape so they're correct and ready once un-ignored, but not
  runnable here as proof.

## Acceptance Criteria
- [ ] `OrderStatusChangedConsumerSuite` rewritten for `order-events` +
  envelope (reservation_failed/confirmed/payment_failed emails, malformed
  payload, null value, and a new "ignores a different eventType (e.g.
  OrderReserved)" case) — still `.ignore`'d per the Non-Functional note.
- [ ] `scripts/verify-order-status-changed.sh` updated to publish
  `order-events` envelopes and live-verified against a real docker-compose
  Kafka + notification-service.
- [ ] Full `sbt test` suite stays green.

## Out of Scope
- Un-ignoring the Kafka integration tests (separate, already-tracked
  Backlog item — Testcontainers/Docker-Desktop compatibility, not related
  to this migration).
- `purekafka`-based consumer resilience (Rework 3) — separate track.
- `system-design.md`'s "Kafka topics"/payload-contracts rewrite — this is
  the fourth and final repo in this cutover, so that rewrite happens next,
  as its own separate step in `gluon` itself.
