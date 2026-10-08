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

## API layer
- tapir 1.11.25 (tapir-core, tapir-json-circe, tapir-http4s-server) —
  defines the two health endpoints only. No longer wraps
  `purerest.docs.Docs` (removed along with the CRUD surface — nothing left
  to document); `/docs` returns 404 until a new route exists worth
  documenting

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
  tests (`OrderStatusChangedConsumerSuite`). Currently `.ignore`'d in this
  dev environment: a docker-java/Testcontainers-vs-local-Docker-Desktop
  version incompatibility makes every client strategy get a degenerate
  `/info` response, even though `docker`/`docker compose`/`curl` against
  the same socket all work fine — not a code problem. Un-ignore once
  resolved.

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — declares one service: a single-node KRaft-mode Kafka
  broker (`apache/kafka:3.8.0`), mirroring payment-service's
  `docker-compose.yml` exactly.

## Messaging
- fs2-kafka 3.6.0 — consumer side only (no publish side for this
  service). `OrderStatusChangedConsumer` subscribes to
  `order.status-changed` with null-safe `Deserializer.option` for both
  key and value from the start (mirrors payment-service's
  `OrderReservedConsumer`, which already fixed the plain-`String`-
  deserializer-throws-on-null bug that order-service's `StockEventConsumer`
  still has). Group id `notification-service-order-status-changed`.
  At-least-once, commit-after-process, no retry on the consume side.
- Readiness: a shared `Ref[F, Boolean]` flips to `false` via the
  consumer stream's `.onFinalizeCase`, only on an `Errored` exit (not
  ordinary cancellation/shutdown). `HealthRoutes.readyServerEndpoint`
  reads it and returns `200`/`503` accordingly.
- Email: no real provider decided yet (per PLAN.md Phase 5) —
  `EmailClient.logging` stubs the send as a structured log line.

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
