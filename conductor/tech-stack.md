# Tech Stack — notification-service

## Language / runtime
- Scala 3.9.0
- JDK 21

## Effects / HTTP
- Cats Effect 3.7.0
- http4s 0.23.37 (ember-server, http4s-dsl, http4s-circe) — currently backs
  the generated CRUD routes; only health-check routes are expected to
  survive once the Postgres/CRUD layer is stripped (see
  `conductor/product.md`)
- circe 0.14.16 (circe-generic, circe-parser)

## API layer
- tapir 1.11.25 (tapir-core, tapir-json-circe, tapir-http4s-server) — route
  definitions + generated Swagger/OpenAPI docs (`purerest.docs.Docs`).
  Scoped down to whatever routes remain (likely just `/health`) once the
  CRUD surface is removed

## Persistence (generated, slated for removal)
- skunk-core 1.0.0 — pure-FP, non-blocking Postgres access
- Flyway 11.8.2 (+ flyway-database-postgresql) — schema migrations on
  startup
- postgresql JDBC 42.7.13 — Flyway-only (runtime scope)
- pureconfig 0.17.10 — typed config from `application.conf`

These four are all generator defaults for a standard CRUD service.
`gluon/docs/system-design.md` describes this service's *intended* end state
as having no DB module — these stay in `build.sbt` only until the backlog's
"strip Postgres/CRUD layer down to a bare Kafka consumer" item is done.

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
- testcontainers-scala 0.43.6 (postgresql + munit modules currently in
  `build.sbt`; postgresql module to be dropped, kafka module to be added —
  see "Not yet in build.sbt" below)
- scalafmt (default Scala 3 style) — `sbt scalafmtCheck test` run in CI

## Packaging / local deploy
- sbt-native-packager (`JavaAppPackaging`, `DockerPlugin`)
- Docker image: `eclipse-temurin:21-jre`
- Docker Compose — currently starts Postgres only (`docker-compose.yml`);
  needs a Kafka broker once the consumer is built, Postgres dropped once
  the CRUD layer is stripped

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
