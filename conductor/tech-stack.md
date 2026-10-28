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
  defines the two health endpoints only. No longer wraps
  `purerest.docs.Docs` (removed along with the CRUD surface — nothing left
  to document); `/docs` returns 404 until a new route exists worth
  documenting
- purekafka.docs (`AsyncApiChannel`/`Docs`) — generates AsyncAPI
  documentation for both consumed channels (`OrderStatusChanged` on
  `order-events`, `UserRegistered` on `user-events`; `AsyncApiChannels`),
  served at `/docs/asyncapi.json` (raw spec) and
  `/docs/asyncapi` (rendered static HTML) — this service's only
  generated API documentation (no OpenAPI/Swagger, no CRUD API).
  Generation handled by the `sbt-purekafka-docs` plugin, auto-chained
  after `Compile / compile` (ADR 0010).
- `schemaRegistryCheck` (sbt-purekafka-docs, Rework 5 step 3/ADR 0003) —
  reuses the same `AsyncApiChannels` registration to check/register the
  event's JSON Schema against Apicurio Registry, run explicitly (not
  chained after `compile`): locally against the shared OrbStack instance
  (`schemaRegistryUrl`'s default), in CI against a throwaway
  `docker run` Apicurio container (`SCHEMA_REGISTRY_URL` env override —
  see `ci.yml`). Build/CI-time only, zero runtime presence.

## Config
- pureconfig 0.17.10 — typed config from `application.conf` (now just
  port/metrics-port/service-name, since `PostgresConfig` was removed)

## Shared platform library
- `purerestlib` 0.1.0 (`io.github.beckfordp`) — tracing, structured logging,
  metrics, resilience (retry + circuit breaker), tapir-based docs. Consumed
  as a published GitHub Packages artifact, never vendored/`.dependsOn`.
  ADR 0005 flags a possible future migration off GitHub Packages (JitPack or
  Maven Central) — not yet adopted. (Resilience middleware is `Client[F]`-
  only — not applicable to this service's Kafka consumer; see
  `system-design.md`'s "Reliability" note.)

## Testing
- munit 1.3.6 + munit-cats-effect 2.2.1
- log4cats-testing 2.8.0 — assert on structured log output
- scalafmt (default Scala 3 style) — `sbt scalafmtCheck test` run in CI
- testcontainers-scala-kafka — added for US-7.1's consumer integration
  tests (`OrderStatusChangedConsumerSuite`). The original
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
- Docker Compose — declares one service: a single-node KRaft-mode Kafka
  broker (`apache/kafka:3.8.0`), mirroring payment-service's
  `docker-compose.yml` exactly.

## Messaging
- fs2-kafka 3.6.0 — consumer side only (no publish side for this
  service). `OrderStatusChangedConsumer` subscribes to `order-events`
  with null-safe `Deserializer.option` for both key and value from the
  start. Group id `notification-service-order-events` (corrected from a
  stale note here naming a different, never-actual id).
- purekafka (`io.github.beckfordp`) — `consumer.StreamSupervisor` wraps
  each consumer's stream (restart-with-backoff) and `consumer.DeadLetter`
  wraps its per-message processing (bounded retry, then routes to
  `order-events-dlq` / `user-events-dlq` on exhaustion) — TD-2.3/TD-2.4,
  ADR 0010. `KafkaConsumers.supervised` merges the two supervised streams
  so a crash in one never restarts the other. Replaces the old hand-rolled `readyRef`/
  `onFinalizeCase` restart signal (found broken 2026-10-09 — stuck at
  503 forever after a dropped connection) entirely.
- Readiness: `/health/ready` now always returns `200`, same as
  `/health` — there's no DB/store to gate on, and `StreamSupervisor`
  structurally eliminates the "stuck forever" state the old mechanism
  existed to signal.
- Email: no real provider decided yet (per PLAN.md Phase 5) —
  `EmailClient.logging` stubs the send as a structured log line.

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
