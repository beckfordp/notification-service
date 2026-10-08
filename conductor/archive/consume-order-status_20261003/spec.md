# Spec: US-7.1 — consume order.status-changed, send the matching email per status

## Overview
Builds the real Kafka consumer this service was stripped down for
(`strip-crud_20261002`). Subscribes to `order.status-changed`, decodes it against
the pinned payload contract in `gluon/docs/system-design.md`, and for each of the
three recognized statuses (`reservation_failed`/`confirmed`/`payment_failed`) sends
the matching email via a stub `EmailClient`. Mirrors payment-service's
`OrderReservedConsumer` pattern (the one existing consumer in Gluon that already
fixed the null-key/value silent-crash bug) rather than order-service's
`StockEventConsumer` (which hasn't).

**Known simplification:** there's no user-service/email-address field anywhere in
Gluon yet (`customerId` is a bare string, per `user-stories.md`'s own
"Auth/identity" open question). The stub `EmailClient.send` takes `customerId` as
the recipient identifier, not a real email address — correct scope for the walking
skeleton, revisit once real customer accounts exist.

## Functional Requirements
- **Domain model** (`OrderStatusChangedEvent.scala`): case class
  `{orderId: String, customerId: String, status: OrderStatusChanged, timestamp: Instant}`
  mirroring the pinned payload. `OrderStatusChanged` is a closed enum
  (`ReservationFailed`/`Confirmed`/`PaymentFailed`) with a circe decoder that only
  accepts the three pinned wire strings (`reservation_failed`/`confirmed`/
  `payment_failed`) — anything else is a decode failure, not a silent fallthrough.
- **`EmailClient[F]`** (`EmailClient.scala`): a small algebra,
  `def send(to: String, subject: String, body: String): F[Unit]`. One constructor,
  `EmailClient.logging[F](logger)`, stubs the send as a structured log line (no real
  provider — per `gluon/PLAN.md` Phase 5). Subject/body per status:
  - `ReservationFailed`: "Your order could not be fulfilled" / mentions `orderId`
  - `Confirmed`: "Your order is confirmed" / mentions `orderId`
  - `PaymentFailed`: "We couldn't process your payment" / mentions `orderId`
- **Kafka infra**: add `fs2-kafka` 3.6.0 + `testcontainers-scala-kafka` to
  `build.sbt`; add a single-node KRaft Kafka broker to `docker-compose.yml`
  (mirroring payment-service's `apache/kafka:3.8.0` service exactly); add
  `KafkaConfig(bootstrapServers: String)` to `NotificationServiceConfig`.
- **`OrderStatusChangedConsumer.scala`**: subscribes to `order.status-changed` with
  `ConsumerSettings[F, Option[String], Option[String]]` (fs2-kafka's null-safe
  `Deserializer.option` for both key and value — baked in from the start, not
  retrofitted like order-service/inventory-service still need). Group id
  `notification-service-order-status-changed`. On each record: null value → log
  error, skip; decode failure (includes any unrecognized status) → log error with
  the raw payload, skip; success → dispatch the matching email via `EmailClient`,
  then log info. Offset committed unconditionally after handling, same
  at-least-once/commit-after-process stance as payment-service's consumer — no
  retry on the consume side.
- **Readiness wiring**: a shared `Ref[F, Boolean]` (starts `true`) flips to `false`
  via the consumer stream's `.onFinalizeCase` only on an `Errored` exit (not on
  ordinary cancellation/shutdown). `HealthRoutes.readyServerEndpoint[F]` takes this
  ref and returns `200`/`503` accordingly — closes the exact silent-fiber-death gap
  `system-design.md` flags, which neither order-service nor payment-service have
  fixed yet.
- **`Main.scala`**: wire `KafkaConfig`, start the consumer via
  `.compile.drain.background.use`, pass the health ref into `HealthRoutes`.

## Non-Functional Requirements
- `sbt scalafmtCheck test` passes.
- TDD throughout: domain model, `EmailClient`, consumer decode/dispatch logic, and
  readiness wiring each get red→green unit tests (a `FakeEmailClient` records calls
  for assertions) before a Testcontainers-Kafka integration test proves the real
  wiring end-to-end.

## Acceptance Criteria
- Publishing a synthetic `order.status-changed` event (each of the 3 statuses) to a
  test Kafka results in exactly one `EmailClient.send` call with the right
  recipient/content, and the offset is committed.
- An event with an unrecognized `status` (or a null value) is logged and skipped —
  no email sent, no crash, offset still committed.
- Killing the consumer's connection to Kafka flips `/health/ready` to `503`; a
  healthy consumer keeps it at `200`.
- A committed `scripts/verify-order-status-changed.sh` publishes one real event per
  status to the local Kafka broker and confirms the (stub-logged) email fires,
  matching the pattern of this track's predecessor scripts.

## Out of Scope
- A real email/notification provider (still not decided — stub only).
- Redelivery/idempotency guards on the consume side (same at-least-once risk
  profile payment-service accepted for US-6.1; not this track's job).
- Fixing order-service's/inventory-service's own still-open null-key consumer risk
  (flagged in `system-design.md`, tracked separately).
- Real customer email addresses / any user-service integration (per the "Known
  simplification" above).
