# Tech Stack — notification-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server, http4s-dsl, http4s-circe) — backs the two
  health-check routes only (the CRUD routes were removed in
  `conductor/tracks/strip-crud_20261002/`)
- circe 0.14.16 (circe-generic, circe-parser)
- CORS — `org.http4s.server.middleware.CORS` (ships in `http4s-server`, no new
  dependency) wraps every response (`Cors.scala`), including the two
  health-check routes. Allow all origins, GET/POST/PATCH/PUT/DELETE,
  `Content-Type` header allowed, credentials disallowed — same policy as
  every other generated service, kept for consistency even though this
  service has no domain routes for it to protect. Ported from
  `pure-service-generator`'s `cors_20261010` track (TD-1.3).

## API layer
- tapir 1.11.25 (tapir-core, tapir-json-circe, tapir-http4s-server) —
  defines the two health endpoints only. Since `rest-docs_20261101`
  (TD-20.9) they are served through `purerest.docs.Docs.routes`, which
  also generates the OpenAPI spec at `/docs/docs.yaml` and Swagger UI at
  `/docs`; `NotificationDocsSuite` asserts `Docs.undocumented` is empty.
  The CRUD routes/DTOs removed by `strip-crud_20261002` stay gone, so
  there are no parameters, request/response bodies or error variants in
  the spec
- purekafka.docs (`AsyncApiChannel`/`Docs`) — generates AsyncAPI
  documentation for every consumed channel (three order outcomes on
  `order-events`, `UserRegistered` on `user-events`, two dispatch outcomes on
  `fulfilment-events`, two capture outcomes on `payment-events`;
  `AsyncApiChannels`),
  served at `/docs/asyncapi.json` (raw spec) and
  `/docs/asyncapi` (rendered static HTML) — the event-side generated API
  documentation, alongside the REST OpenAPI spec above; still no CRUD API.
  Generation handled by the `sbt-purekafka-docs` plugin, auto-chained
  after `Compile / compile` (ADR 0010).
- `schemaRegistryCheck` (sbt-purekafka-docs, Rework 5 step 3/ADR 0003) —
  reuses the same `AsyncApiChannels` registration to check/register the
  event's JSON Schema against Apicurio Registry, run explicitly (not
  chained after `compile`): locally against the shared OrbStack instance
  (`schemaRegistryUrl`'s default), in CI against a throwaway
  `docker run` Apicurio container (`SCHEMA_REGISTRY_URL` env override —
  see `ci.yml`). Build/CI-time only, zero runtime presence.
- Schema registry baseline (gluon TD-18, purekafka 0.5.0 / sbt-purekafka-docs
  0.5.0-dev): only producer channels register (draft-07, closed, labelled
  `producer=<service>`, `BACKWARD_TRANSITIVE`); subscribe channels dry-run
  their mirror. CI restores gluon's `registry/` git export into the throwaway
  Apicurio first, so the check compares against the real baseline;
  `scripts/verify-schema-registry-check.sh` does the same locally.
- AsyncAPI 3.0 (gluon TD-19/19.6, purekafka 0.6.1 / sbt-purekafka-docs 0.6.1-dev):
  `send`/`receive` from this service's side, sends listed first. Consumed-event
  mirrors come from gluon's `registry/` via `bin/registry-scala` (see gluon
  `registry/README.md`).

## Config
- pureconfig 0.17.10 — typed config from `application.conf`: port/metrics-port/
  service-name, plus the `postgres` block (host/port/database/user/password
  with `POSTGRES_*` env overrides) reintroduced by ADM-2 for the sent-email
  audit store

## Shared platform library
- `purerestlib` 0.7.3 (`io.github.beckfordp`) — tracing, structured logging,
  metrics, resilience (retry + circuit breaker), tapir-based docs and the
  `Docs.undocumented` coverage check (TD-20.1). Consumed
  as a published GitHub Packages artifact, never vendored/`.dependsOn`.
  ADR 0005 flags a possible future migration off GitHub Packages (JitPack or
  Maven Central) — not yet adopted. (Resilience middleware is `Client[F]`-
  only — not applicable to this service's Kafka consumer; see
  `system-design.md`'s "Reliability" note.)

## Persistence (reintroduced by ADM-2)
- Skunk 1.0.0 (`skunk-core`) — non-blocking, pure-FP Postgres access for the
  `sent_email` audit store; runtime queries only, mirroring order-service.
- Flyway 11.8.2 (`flyway-core` + `flyway-database-postgresql`) —
  startup-only schema migration (`Migrations`, `V1__create_sent_email.sql`).
  Independent of Skunk.
- postgresql (pgjdbc) 42.7.13 — Runtime only, used solely by Flyway through
  JDBC's `DriverManager`.
- Skunk's otel4s 0.16.0 transitive dependencies conflict with purerestlib's
  1.1.0, so the three `otel4s-core*` coordinates are pinned via
  `dependencyOverrides` (same resolution as order-service and purerest).

## Testing
- munit 1.3.6 + munit-cats-effect 2.2.1
- log4cats-testing 2.8.0 — assert on structured log output
- scalafmt (default Scala 3 style) — `sbt scalafmtCheck test` run in CI
- testcontainers-scala-postgresql — real ephemeral Postgres for the
  migration, store and route suites added by ADM-2.
- testcontainers-scala-kafka — added for US-7.1's consumer integration
  tests (now `OrderEventsConsumerSuite`). The original
  Docker-API-version blocker (docker-java hardcoded to API 1.32,
  rejected by this machine's Docker ≥ 1.40) was fixed 2026-10-24 by
  bumping `testcontainersScalaVersion` to 0.44.1 (same fix `purerest`'s
  own build.sbt already applied) — containers now actually start. But
  that exposed a *separate*, real bug: all 6 of that suite's tests share
  one container + a fixed consumer group id with `Earliest` offset
  reset, so a later test consumes an earlier test's leftover message.
  Still `.ignore`'d — see `conductor/tracks.md`'s backlog item. The
  newer, separate `KafkaConsumersSuite` (one test, its own container)
  isn't affected and runs normally.

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — still declares one service: a single-node KRaft-mode Kafka
  broker (`apache/kafka:3.8.0`), mirroring payment-service's
  `docker-compose.yml` exactly. The Postgres service, Kubernetes deployment
  and readiness gating for the audit store are ADM-6, not ADM-2.

## Messaging
- fs2-kafka 3.6.0 — consumer side only (no publish side for this
  service). `OrderEventsConsumer` subscribes to `order-events`
  with null-safe `Deserializer.option` for both key and value from the
  start. Group id `notification-service-order-events` (corrected from a
  stale note here naming a different, never-actual id). The same pattern
  covers `UserRegisteredConsumer` (`notification-service-user-events`),
  `FulfilmentEventConsumer` (`notification-service-fulfilment-events`) and
  `PaymentEventConsumer` (`notification-service-payment-events`).
- purekafka (`io.github.beckfordp`) — `consumer.StreamSupervisor` wraps
  each consumer's stream (restart-with-backoff) and `consumer.DeadLetter`
  wraps its per-message processing (bounded retry, then routes to
  `order-events-dlq` / `user-events-dlq` / `fulfilment-events-dlq` /
  `payment-events-dlq` on exhaustion) — TD-2.3/TD-2.4, ADR 0010.
  Per-record handling is shared (`EnvelopeHandler`, TD-16.6): undecodable
  envelopes/payloads go raw to the DLQ too, unknown event types are logged
  and skipped, null values are logged and skipped.
  `KafkaConsumers.watched` fails `Main` if the merged stream dies (e.g. DLQ
  producer acquisition), so the pod restarts.
  `KafkaConsumers.supervised` merges the four supervised streams so a crash
  in one never restarts the others. Replaces the old hand-rolled `readyRef`/
  `onFinalizeCase` restart signal (found broken 2026-10-09 — stuck at
  503 forever after a dropped connection) entirely.
- Readiness: `/health/ready` still always returns `200`, same as
  `/health` — ADM-2 adds a DB but deliberately leaves readiness gating
  (and migrations-at-deploy verification) to ADM-6.
- Email: no real provider decided yet (per PLAN.md Phase 5) —
  `EmailClient.logging` stubs the transport, while the auditing
  `EmailClient` persists each attempt to `sent_email` (ADM-2).

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
