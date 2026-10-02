# Product Guidelines — notification-service

## Code & engineering conventions
Canonical source: [`development-guidelines.md`](../development-guidelines.md)
(pure-FP, tagless-final, ADT error modeling, no partial functions, no
null/var, explicit DI, exhaustive pattern matching). This file doesn't
restate those — keep following them as the service grows, even once it's
reshaped from generated CRUD scaffold into a Kafka-only consumer.

## API design conventions
This service has **no CRUD HTTP API** once stripped (see
`conductor/product.md`'s warning) — it's a Kafka consumer only, so most of
order-service's REST conventions (pluralized paths, PATCH vs PUT,
one-db-per-service) don't apply here:
- No Postgres, no Flyway, no skunk — no database at all, once the generated
  scaffold is stripped down per `gluon/backlogs/notification-service.md`'s
  first line
- Health endpoints stay (`GET /health`, `GET /health/ready`) — same
  generator convention as every other Gluon service, `readiness` adjusted
  to check Kafka consumer connectivity instead of a DB ping once the store
  is removed
- Errors: domain errors stay an explicit ADT per `development-guidelines.md`
  (e.g. an unrecognized `status` value on a consumed event), even with no
  HTTP edge to map them to — log them loudly rather than silently dropping,
  same reliability stance `system-design.md` already applies to the
  producer side (order-service/payment-service's publishers)
- Swagger/OpenAPI docs (`purerest.docs.Docs`) only apply if a route exists —
  drop the `/docs` endpoint along with the rest of the CRUD surface if
  nothing is left to document

## Testing
- scalafmt + munit + munit-cats-effect
- Testcontainers Kafka module for the consumer (publish synthetic
  `order.status-changed` events, assert the stubbed send is invoked with
  the right content) — no Testcontainers Postgres needed once the CRUD
  layer is gone
- `sbt scalafmtCheck test` before every commit (CI enforces the same)
