# Plan: ADM-2 — sent-email audit in notification-service

## Phase 1: Persistence foundation

- [x] Task: Update `conductor/tech-stack.md` and `conductor/product.md` —
  record the reintroduced Postgres/Skunk/Flyway stack and the new read API
  (workflow principle 2, before code). (8218f8d)
- [x] Task: Failing config test for `PostgresConfig` defaults and `POSTGRES_*`
  env overrides; add `PostgresConfig`, the `postgres` block in
  `application.conf`, and config wiring (Red → Green). (d8edda4)
- [x] Task: Failing `MigrationsSuite` (Testcontainers Postgres) asserting
  `sent_email` columns, check constraints and indexes; add Skunk, Flyway
  (core + postgresql), pgjdbc, Testcontainers-Postgres and the otel4s
  overrides to `build.sbt`; add `Migrations.scala` and
  `V1__create_sent_email.sql` (Red → Green). (8cde91f)
- [x] Task: Conductor - User Manual Verification 'Persistence foundation'
  (Protocol in workflow.md) — prompting off: verified by the committed
  `NotificationServiceConfigSuite` and `MigrationsSuite`, both green.

## Phase 2: Sent-email store

- [x] Task: Failing `SentEmailStorePostgresSuite`: insert-pending; duplicate
  `source_event_id` on `sent`; retry on `pending`/`failed`; mark-sent and
  mark-failed transitions; filters by customerId, orderId, both, neither;
  newest-first ordering (Red). Implement `SentEmail` / `SentEmailStore` with
  Skunk (Green). (48b56d3)
- [x] Task: Conductor - User Manual Verification 'Sent-email store'
  (Protocol in workflow.md) — prompting off: verified by the committed
  `SentEmailStorePostgresSuite` (7 tests, green).

## Phase 3: Envelope eventId and audited EmailClient

- [x] Task: Failing `EnvelopeHandlerSuite` / consumer tests asserting the
  envelope `eventId` reaches each route handler (Red); change
  `EnvelopeRoute.run` / `EnvelopeHandler` to pass it and update all four
  consumers (Green). (890e816)
- [x] Task: Failing `EmailClientSuite` with a fake store and transport:
  pending → sent; transport error → failed + rethrow; duplicate `sent` →
  transport not called (Red). Implement `EmailMessage`, the auditing
  `EmailClient`, and keep `EmailClient.logging` as the transport (Green).
  (890e816)
- [x] Task: Failing handler tests for all event types asserting event id,
  `recipient_type`/`recipient_key`, `customer_id`/`order_id` and `reason`
  mapping (Red); wire each consumer's send to the audited client (Green).
  (890e816)
- [x] Task: Conductor - User Manual Verification 'Envelope eventId and audited
  EmailClient' (Protocol in workflow.md) — prompting off: verified by
  `EnvelopeHandlerSuite`, `EmailClientSuite`, `SentEmailMappingSuite` and
  `ConsumerHandlersSuite` (27 tests, green).

## Phase 4: Read API and wiring

- [ ] Task: Failing `EmailRoutesSuite` (filters, AND semantics, ordering,
  empty result, JSON shape) and updated `NotificationDocsSuite` (Red);
  implement `EmailRoutes` with tapir and register it in `AppRoutes` /
  `Docs.routes` (Green).
- [ ] Task: Wire `Migrations`, `SentEmailStore` and the audited `EmailClient`
  into `Main` and `KafkaConsumers`; document the endpoint and local-Postgres
  prerequisite in `README.md` (Green).
- [ ] Task: Conductor - User Manual Verification 'Read API and wiring'
  (Protocol in workflow.md)

## Phase 5: Verification

- [ ] Task: `sbt scalafmtCheck test`; run `sbt schemaRegistryCheck` to confirm
  no Kafka-contract drift; confirm >80% coverage on new code.
- [ ] Task: Live check: start a throwaway Postgres, run the service, publish one
  synthetic event per topic and confirm the rows and `GET /emails` filters;
  record the results.
- [ ] Task: Conductor - User Manual Verification 'Verification'
  (Protocol in workflow.md)

## Review

- Filled after `/conductor:review`.
