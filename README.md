# notification-service

A microservice built on [purerest](https://github.com/beckfordp/purerest), generated
from the [pure-service-generator](https://github.com/beckfordp/pure-service-generator)
giter8 template. Its own `build.sbt` resolves `purerestlib` as a published GitHub
Packages dependency.

## Current shape

This service's generated Postgres/CRUD scaffold has been stripped down (see
`conductor/tracks/strip-crud_20261002/`) to match its intended role per
[`gluon/docs/user-stories.md`](../../docs/user-stories.md) US-7: a **Kafka consumer
only, no database**. Right now it serves just health checks; the real consumer —
subscribing to `order.status-changed` and sending the matching email per status — is
**US-7.1**, a separate, not-yet-built track (`gluon/backlogs/notification-service.md`).

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

`docker-compose.yml` currently declares no services — a Kafka broker will be added
there once US-7.1 builds the real consumer.

## Development guidelines

Extending this service? See [`development-guidelines.md`](./development-guidelines.md) for the
pure-FP/tagless-final standards it follows — error modeling as ADTs, patterns to follow (and
avoid) — before adding new code.

## Testing

```
sbt scalafmtCheck test
```

Unit tests only for now (no Postgres/Testcontainers needed) — a Testcontainers Kafka
suite will be added alongside US-7.1's consumer.

## Calling other services with resilience

This service doesn't call any other service out of the box, so purerest's retry + circuit-breaker
middleware (`purerest.resilience.Resilience.middleware`) isn't wired into `Main`. When you adapt
it to call a real downstream service, wrap its http4s `Client[F]` with `Resilience.middleware`
before building your own client on top of it — see
[`ClientResilienceExampleSuite`](./src/test/scala/notificationservice/examples/ClientResilienceExampleSuite.scala)
for the pattern, tested against a dummy client so it stays correct as purerest evolves.
