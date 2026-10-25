# Product Guide — notification-service

## Context
Part of the **Gluon** platform (v1) — a production-grade microservices
e-commerce platform built pure-FP-first in Scala 3 (Cats Effect / http4s).
See the cross-repo [Gluon Product Vision](../../../docs/product.md) (in the
`gluon/` monorepo root) for the full platform vision, naming scheme, and
goals. This document scopes that vision down to notification-service's own
slice.

## What this service does
notification-service closes the walking skeleton: it consumes
`OrderStatusChanged` envelopes on the `order-events` domain topic and sends
the matching customer email for `reservation_failed` / `confirmed` /
`payment_failed`. No HTTP write API, no customer-facing endpoints — Kafka
consumer only, per `user-stories.md`'s US-7 framing ("New
`notification-service`, Kafka consumer only, no database").

Generated via `pure-service-generator` (giter8 template over `purerest`), no
field-spec applied (`gluon/specs/notification.yaml` doesn't exist — none was
needed, there's no CRUD domain entity here).

## Current state
As of `conductor/tracks/strip-crud_20261002/` (complete), the generated
Postgres/CRUD scaffold has been removed: `NotificationStore`/
`NotificationRoutes`/`Migrations`/`NotificationError` are gone, `build.sbt`
no longer carries skunk/Flyway/postgresql-jdbc, and `docker-compose.yml`
declares zero services. The service now serves only `/health` and
`/health/ready` (unconditional 200, no store to ping).
`gluon/docs/system-design.md`'s "new (generator, no DB module)"
services-table line is now accurate (it was aspirational before this
track).

Track `conductor/tracks/consume-order-status_20261003/` (US-7.1, complete)
built the real Kafka consumer on top of this shell:
`OrderStatusChangedConsumer` subscribes with null-safe key/value
deserializers, decodes each event, and dispatches the matching email via a
stub `EmailClient.logging` (structured log line, no real provider). A
shared `Ref[F, Boolean]` tracks the consumer fiber's liveness —
`/health/ready` returns `503` only if that fiber actually crashes, `200`
otherwise. `Main.scala` starts the consumer in the background and serves
both routes. Verified end-to-end against a real local Kafka broker via
`scripts/verify-order-status-changed.sh`.

Track `conductor/archive/kafka-envelope_20261011/` (complete) migrated from
the per-event-type `order.status-changed` topic to the `order-events`
domain topic (ADR 0009, `gluon/PLAN.md` Rework 2): `OrderStatusChangedConsumer`
now decodes a shared `EventEnvelope` first and dispatches on
`eventType == "OrderStatusChanged"`; any other event type on that topic
(e.g. `OrderReserved`) is silently skipped, not logged as malformed.

Track `conductor/archive/purekafka-adoption_20261024/` (TD-2.3/TD-2.4, ADR 0010,
complete) replaced the `readyRef`/`onFinalizeCase` mechanism above entirely:
`OrderStatusChangedConsumer` is now wrapped in `purekafka`'s `StreamSupervisor`
(restart-with-backoff) and `DeadLetter` (bounded-retry-then-`order-events-dlq`),
via the new `KafkaConsumers.supervised` — closing the 2026-10-09 incident where a
dropped Kafka connection left `/health/ready` stuck at `503` forever, only fixed
by a manual `kubectl rollout restart`. `/health/ready` now always returns `200`
(same as `/health`) — there's no DB/store to gate on, and `StreamSupervisor`
structurally eliminates the "stuck forever" state the old mechanism existed to
signal. Also added AsyncAPI documentation for the consumed `OrderStatusChanged`
channel (`AsyncApiChannels`, served at `/docs/asyncapi.json`/`/docs/asyncapi`).

## Domain model
No domain entity in the CRUD sense — this service has none now that the
scaffold is stripped (see "Current state" above). The only "model" is the
consumed event shape — the `OrderStatusChanged` payload, carried inside the
shared envelope on `order-events`, pinned in `gluon/docs/system-design.md`'s
"Payload contracts":
```json
{
  "orderId": "string (UUID)",
  "customerId": "string",
  "status": "string — one of: reservation_failed | confirmed | payment_failed",
  "timestamp": "string (ISO-8601 instant)"
}
```
The outbound side (the "email") has no real provider decided yet — per
PLAN.md Phase 5, stub the send (log line or fake client) rather than
integrating a real email service.

## User stories in scope (gluon/docs/user-stories.md)
- US-7.1 — consume `order.status-changed`, send the matching email per
  status (`reservation_failed` / `confirmed` / `payment_failed`)

## Sequencing (gluon/PLAN.md)
- **Phase 5** (depends on Phase 3's `order.status-changed` publish side and
  Phase 4's payment settlement path both landing in order-service) —
  US-7.1. This is the last leg of the walking skeleton
  (checkout → reserve → async status → payment → notification).
- Stub/fan-out for this phase: publish synthetic `order.status-changed`
  events (one per status) to a test Kafka — no live order-service needed to
  test this repo. Stub the actual send (log line or fake client).

## Events
- Publishes: none
- Consumes: `order-events` (eventType `OrderStatusChanged`, wrapped in the
  shared envelope — one topic per domain, ADR 0009, migrated 2026-10-11,
  kafka-envelope_20261011; previously the dedicated `order.status-changed`
  topic. Other event types on `order-events`, e.g. `OrderReserved`, are
  silently skipped)

## Out of scope for this service
- Any HTTP write API / CRUD persistence (removed — see "Current state"
  above; not being reintroduced)
- A real email/notification provider (not decided yet — stub only, per
  PLAN.md Phase 5's exit criteria)
- The cancel-order flow (US-9, separate epic) and stale-pending-order
  detection (US-10, separate epic) — notification-service only sends the
  email; it doesn't act on it
