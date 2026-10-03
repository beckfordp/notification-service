# Spec: Strip Postgres/CRUD layer down to a bare Kafka consumer shell

## Overview
notification-service was generated via `pure-service-generator`'s standard CRUD
template with no field-spec applied (every `codegen:fields` placeholder is a literal
empty comment). Per `gluon/docs/user-stories.md` US-7, this service is meant to be
"Kafka consumer only, no database." This track removes the unused, never-field-specced
Postgres/CRUD scaffold and leaves a minimal shell (health checks only) for the next
track (US-7.1) to build the real `order.status-changed` consumer on top of.

## Functional Requirements
- Delete `NotificationStore.scala`, `NotificationRoutes.scala`, `Migrations.scala`,
  `NotificationError.scala`, and
  `src/main/resources/db/migration/V1__create_notification_table.sql`.
- `Main.scala`: remove `Migrations.run`, `NotificationStore.postgres` wiring, and all
  `NotificationRoutes.*` endpoints from the served route list. Keep tracing/metrics/
  logging middleware wiring as-is. Served routes become just `/health` and
  `/health/ready`.
- `HealthRoutes.scala`: `readyServerEndpoint` drops its `NotificationStore` parameter
  and returns `200` unconditionally (no dependency left to check yet).
- `NotificationServiceConfig.scala` / `PostgresConfig`: remove the `postgres` field
  and the `PostgresConfig` case class entirely.
- `application.conf`: remove the `postgres { ... }` block. Leave `example-client`
  as-is (unrelated — backs `ClientResilienceExampleSuite`, not the CRUD layer).
- `build.sbt`: remove `skunk-core`, `flyway-core`, `flyway-database-postgresql`,
  `postgresql` (JDBC) dependencies, `testcontainers-scala-postgresql`, and the
  now-unused `dependencyOverrides` for otel4s (that override exists solely to resolve
  Skunk's otel4s version conflict — confirm no other dependency still needs it before
  removing).
- `docker-compose.yml`: remove the `postgres` service and its volume, leaving the file
  structurally empty (no services) until US-7.1 adds a Kafka broker.
- Delete the now-irrelevant tests: `NotificationStoreSuite`,
  `NotificationStorePostgresSuite`, `MigrationsSuite`, `NotificationRoutesSuite`,
  `NotificationDocsSuite`. Keep `HealthRoutesSuite` (update for the new no-arg
  `readyServerEndpoint`), `NotificationServiceConfigSuite` (update for the trimmed
  config), and `ClientResilienceExampleSuite` (untouched — unrelated to the CRUD
  layer).
- `README.md`: rewrite Quickstart/testing sections to drop Postgres prerequisites and
  the `/notifications` CRUD curl examples; describe the service as a to-be-built Kafka
  consumer (pointing at US-7.1).

## Non-Functional Requirements
- `sbt scalafmtCheck test` must pass after the strip.
- No change to CI (`.github/workflows/ci.yml`) — it doesn't reference Postgres
  directly (Testcontainers manages its own containers), so nothing there depends on
  what's being removed.

## Acceptance Criteria
- `sbt scalafmtCheck test` passes with zero Postgres/skunk/Flyway references left
  anywhere in `src/` or `build.sbt`.
- `docker compose up` starts zero services (file left structurally valid,
  service-less).
- `curl :8080/health` → 200, `curl :8080/health/ready` → 200, with no running
  Postgres.
- No `/notifications` routes remain (`curl -X POST :8080/notifications` → 404).

## Out of Scope
- Adding fs2-kafka or any consumer logic (that's US-7.1's own track).
- Adding a Kafka broker to `docker-compose.yml` (deferred to US-7.1).
- Choosing/stubbing an email provider (deferred to US-7.1, per `gluon/PLAN.md`
  Phase 5).
