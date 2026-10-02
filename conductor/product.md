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
`order.status-changed` from Kafka and sends the matching customer email for
`reservation_failed` / `confirmed` / `payment_failed`. No HTTP write API, no
customer-facing endpoints — Kafka consumer only, per `user-stories.md`'s
US-7 framing ("New `notification-service`, Kafka consumer only, no
database").

Generated via `pure-service-generator` (giter8 template over `purerest`), no
field-spec applied (`gluon/specs/notification.yaml` doesn't exist — none was
needed, there's no CRUD domain entity here).

## ⚠️ Generated state doesn't match the intended shape yet
`gluon/backlogs/notification-service.md`'s first line is "Generate
notification-service, strip Postgres/CRUD layer down to a bare Kafka
consumer (infra)" — **the generate half is done, the strip half is not**.
As generated, this repo still carries the full CRUD scaffold with no
field-spec applied:
- `Notification` (`NotificationStore.scala`) has only `id`/`createdAt`/
  `updatedAt` — every codegen placeholder (`CREATE_PARAMS`,
  `CONSTRUCT_ARGS`, `SQL_INSERT_COLUMNS`, etc.) is still a literal, empty
  `/* codegen:fields:... */` comment, never filled in.
- `NotificationRoutes`/`NotificationStore`/`Migrations` still wire a full
  Postgres CRUD API (`POST`/`GET`/`PATCH`/`PUT`/`DELETE /notifications`),
  and `build.sbt` still carries skunk/Flyway/postgresql-jdbc.
- `gluon/docs/system-design.md`'s services table already describes the
  *intended* end state ("new (generator, no DB module)") — that line is
  aspirational, not a description of what's in this repo today.

Stripping this down (dropping `NotificationStore`/`NotificationRoutes`/
`Migrations`/the Postgres deps, replacing the CRUD surface with a Kafka
consumer) is the first track to run here — see `conductor/tracks.md`.

## Domain model
No domain entity in the CRUD sense. Once stripped (above), the only "model"
is the consumed event shape — `order.status-changed`, pinned in
`gluon/docs/system-design.md`'s "Payload contracts":
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
- Consumes: `order.status-changed`

## Out of scope for this service
- Any HTTP write API / CRUD persistence (the generated scaffold has one;
  it's being stripped — see the warning above)
- A real email/notification provider (not decided yet — stub only, per
  PLAN.md Phase 5's exit criteria)
- The cancel-order flow (US-9, separate epic) and stale-pending-order
  detection (US-10, separate epic) — notification-service only sends the
  email; it doesn't act on it
