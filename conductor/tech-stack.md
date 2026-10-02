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
- No testcontainers module currently in `build.sbt` (the postgresql +
  munit modules were dropped along with the CRUD layer) — a kafka module
  will be added for US-7.1's consumer, see "Not yet in build.sbt" below

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — currently declares zero services (`docker-compose.yml`);
  a Kafka broker will be added once US-7.1 builds the consumer

## Not yet in build.sbt (needed for US-7.1, per system-design.md's services table)
- **fs2-kafka** — consumer side only (no publish side for this service);
  subscribes to `order.status-changed`, mirrors order-service's
  `StockEventConsumer` / payment-service's `OrderReservedConsumer` pattern.
  Use fs2-kafka's null-safe `Deserializer.option` for key/value from the
  start — `system-design.md`'s "Open design questions" flags a live bug
  where the plain `String` deserializer throws (and silently kills the
  backgrounded consumer fiber) on a null key/value; payment-service already
  fixed this, order-service hasn't yet
- **testcontainers-scala-kafka** — Kafka module, for the consumer's own
  integration tests (publish synthetic `order.status-changed` events,
  assert the stubbed send fires)
- An email/notification client — no real provider decided yet (per
  PLAN.md Phase 5); stub it (log line or fake client) rather than adding a
  real dependency

## Target infrastructure (platform-wide, from `gluon/docs/system-design.md`)
- Local: OrbStack Kubernetes (see ADR 0002)
- Promotion: dev → staging → prod, all on AWS EKS, single AWS account /
  multi-namespace (see ADR 0004)
- CI/CD: GitHub Actions, candidate release = container image tag promoted
  through environments with automated smoke-test gates
