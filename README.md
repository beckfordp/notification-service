# notification-service

A microservice built on [purerest](https://github.com/beckfordp/purerest), generated
from the [pure-service-generator](https://github.com/beckfordp/pure-service-generator)
giter8 template. Its own `build.sbt` resolves `purerestlib` as a published GitHub
Packages dependency.

## Current shape

This service's generated Postgres/CRUD scaffold was stripped down (see
`conductor/tracks/strip-crud_20261002/`) to match its intended role per
[`gluon/docs/user-stories.md`](../../docs/user-stories.md) US-7. It runs four
supervised, DLQ-guarded consumers:
`OrderConfirmed`/`OrderPaymentFailed`/`OrderCancelled` on `order-events`
(US-7.1/TD-15, matching status email; the old single `OrderStatusChanged`
event is retired), `UserRegistered` on `user-events` (US-16.4, welcome email),
`DispatchConfirmed`/`DispatchFailed` on `fulfilment-events` (US-17.5/18.4,
shipped / dispatch-failed emails) and `PaymentCaptured`/`PaymentFailed` on
`payment-events` (US-19.4, receipt / payment-problem emails).

ADM-2 reintroduced Postgres as an email audit store: every send is recorded in
`sent_email`, keyed by the envelope `eventId` (so a redelivery never sends the
same email twice), and exposed read-only at `GET /emails`. Email sending is
still stubbed (`EmailClient.logging`); there is no HTTP write API.

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

# The sent-email audit store (ADM-2) needs Postgres. Kubernetes/Helm wiring is
# ADM-6; for a local run, use a throwaway container:
docker run --rm -d --name notification-postgres \
  -p 5432:5432 \
  -e POSTGRES_DB=notification \
  -e POSTGRES_USER=notification \
  -e POSTGRES_PASSWORD=notification \
  postgres:16-alpine

sbt run   # starts the service on :8080; Flyway creates sent_email on startup
```

Then, in another terminal:

```
# Liveness / readiness
curl http://localhost:8080/health
curl http://localhost:8080/health/ready

# Sent-email audit history (both filters optional)
curl 'http://localhost:8080/emails?customerId=<customerId>'
```

`docker-compose.yml` still declares only the single-node KRaft Kafka broker;
the Postgres service and readiness gating are ADM-6.

## View REST/OpenAPI docs

The health and sent-email endpoints are documented via
`purerest.docs.Docs` (gluon TD-20.9):

- **Swagger UI** — http://localhost:8080/docs
- **Raw OpenAPI spec** — http://localhost:8080/docs/docs.yaml

`NotificationDocsSuite` keeps `Docs.undocumented` at zero, and
`scripts/verify-rest-docs.sh` checks the served spec end-to-end.

## View Kafka AsyncAPI docs

`OrderEventsConsumer` consumes `OrderConfirmed`/`OrderPaymentFailed`/
`OrderCancelled` on `order-events`,
`UserRegisteredConsumer` consumes `UserRegistered` on `user-events` (welcome email),
`FulfilmentEventConsumer` consumes `DispatchConfirmed`/`DispatchFailed` on
`fulfilment-events` (shipped / dispatch-failed emails), and `PaymentEventConsumer`
consumes `PaymentCaptured`/`PaymentFailed` on `payment-events` (receipt /
payment-problem emails). All are registered for [AsyncAPI](https://www.asyncapi.com/) documentation via
`purekafka.docs` (see `AsyncApiChannels`) — the event-side counterpart to the REST/OpenAPI docs
above. This service still has no CRUD API.

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
Testcontainers Kafka integration test proving DLQ routing. `MigrationsSuite`,
`SentEmailStorePostgresSuite` and `EmailRoutesSuite` run against a Testcontainers
Postgres and cover the sent-email audit store and `GET /emails`.
`OrderEventsConsumerSuite`'s broker-backed tests remain `.ignore`'d pending the
shared-container group-id fix (see `conductor/tracks.md`'s backlog); the handler
suites cover the same logic without a broker.

## Calling other services with resilience

Recipient resolution (ADM-3/ADR 0025) calls two services synchronously: `UserClient`
(user-service `GET /users/{id}`) and `OrderClient` (order-service `GET /orders/{id}`).
`Main` builds one shared Ember client, wraps it with
`purerest.resilience.Resilience.middleware` (retry + circuit breaker) and a 5-second
request timeout, then constructs the two typed clients. `USER_SERVICE_URL` and
`ORDER_SERVICE_URL` override the local defaults; the k8s values point at the in-cluster
services. `ClientResilienceExampleSuite` still demonstrates the generic pattern against a
dummy client.
