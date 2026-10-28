# notification-service

A microservice built on [purerest](https://github.com/beckfordp/purerest), generated
from the [pure-service-generator](https://github.com/beckfordp/pure-service-generator)
giter8 template. Its own `build.sbt` resolves `purerestlib` as a published GitHub
Packages dependency.

## Current shape

This service's generated Postgres/CRUD scaffold has been stripped down (see
`conductor/tracks/strip-crud_20261002/`) to match its intended role per
[`gluon/docs/user-stories.md`](../../docs/user-stories.md) US-7: a **Kafka consumer
only, no database**. It runs four supervised, DLQ-guarded consumers:
`OrderStatusChanged` on `order-events` (US-7.1, matching status email),
`UserRegistered` on `user-events` (US-16.4, welcome email),
`DispatchConfirmed`/`DispatchFailed` on `fulfilment-events` (US-17.5/18.4,
shipped / dispatch-failed emails) and `PaymentCaptured`/`PaymentFailed` on
`payment-events` (US-19.4, receipt / payment-problem emails). Email sending is
stubbed (`EmailClient.logging`); there is no HTTP domain API.

## Prerequisites

- sbt / JDK 21 (for building and running)
- A GitHub [personal access token](https://github.com/settings/tokens) with `read:packages`
  scope, exported as `GITHUB_TOKEN` (and `GITHUB_ACTOR` set to your GitHub username) — needed to
  resolve `purerestlib` from GitHub Packages. GitHub Packages requires authentication to *read*
  Maven artifacts even from a public repo.

## Quickstart

```
export GITHUB_ACTOR=<your-github-username>
export GITHUB_TOKEN=<your-PAT-with-read:packages>

sbt run   # starts the service on :8080 (no other local infra needed yet)
```

Then, in another terminal:

```
# Liveness / readiness
curl http://localhost:8080/health
curl http://localhost:8080/health/ready
```

`docker-compose.yml` declares a single-node KRaft Kafka broker for local runs.

## View Kafka AsyncAPI docs

`OrderStatusChangedConsumer` consumes `OrderStatusChanged` on `order-events`,
`UserRegisteredConsumer` consumes `UserRegistered` on `user-events` (welcome email),
`FulfilmentEventConsumer` consumes `DispatchConfirmed`/`DispatchFailed` on
`fulfilment-events` (shipped / dispatch-failed emails), and `PaymentEventConsumer`
consumes `PaymentCaptured`/`PaymentFailed` on `payment-events` (receipt /
payment-problem emails). All are registered for [AsyncAPI](https://www.asyncapi.com/) documentation via
`purekafka.docs` (see `AsyncApiChannels`) — this service has no OpenAPI/Swagger docs (no CRUD
API at all), so this is its only generated API documentation.

Generation is handled by the `sbt-purekafka-docs` plugin (`asyncApiChannelsClass` in `build.sbt`),
auto-chained after `Compile / compile` — every `sbt compile`/`run`/`Docker/publishLocal` already
regenerates both files locally, no running server or manual script needed. Run it directly with:
```
sbt asyncApiGenerate
```

- **Raw spec** — generated at `target/asyncapi-docs-spec/asyncapi.json`; also served live at
  http://localhost:8080/docs/asyncapi.json while the service is running.
- **Rendered HTML** — generated at `target/asyncapi-docs/index.html`
  (`open target/asyncapi-docs/index.html`); also served live at
  http://localhost:8080/docs/asyncapi/index.html while the service is running.

## Development guidelines

Extending this service? See [`development-guidelines.md`](./development-guidelines.md) for the
pure-FP/tagless-final standards it follows — error modeling as ADTs, patterns to follow (and
avoid) — before adding new code.

## Testing

```
sbt scalafmtCheck test
```

Unit tests cover every event model and email mapping. `KafkaConsumersSuite` is a
Testcontainers Kafka integration test proving DLQ routing. The older
`OrderStatusChangedConsumerSuite` remains `.ignore`'d pending its own fix (see
`conductor/tracks.md`'s backlog).

## Calling other services with resilience

This service doesn't call any other service out of the box, so purerest's retry + circuit-breaker
middleware (`purerest.resilience.Resilience.middleware`) isn't wired into `Main`. When you adapt
it to call a real downstream service, wrap its http4s `Client[F]` with `Resilience.middleware`
before building your own client on top of it — see
[`ClientResilienceExampleSuite`](./src/test/scala/notificationservice/examples/ClientResilienceExampleSuite.scala)
for the pattern, tested against a dummy client so it stays correct as purerest evolves.
