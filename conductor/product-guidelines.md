# Product Guidelines — notification-service

## Code & engineering conventions
Canonical source: [`development-guidelines.md`](../development-guidelines.md)
(pure-FP, tagless-final, ADT error modeling, no partial functions, no
null/var, explicit DI, exhaustive pattern matching). This file doesn't
restate those — keep following them as the service grows, even once it's
reshaped from generated CRUD scaffold into a Kafka-only consumer.

## API design conventions
This service has **no CRUD or write HTTP API** — it is a Kafka consumer with a
read-only audit surface:
- Postgres + Skunk + Flyway are back for the `sent_email` audit store (ADM-2),
  **not** a CRUD domain. One dedicated database per service remains the rule;
  no service reads or writes another's database.
- Health endpoints stay (`GET /health`, `GET /health/ready`) — same generator
  convention as every other Gluon service. Readiness is still unconditional
  `200`; DB/readiness gating is ADM-6.
- The read API is `GET /emails?customerId=&orderId=` (ADM-2): newest first,
  both filters optional and ANDed, no auth (local/demo read API), served via
  tapir and documented like every other route.
- Errors: domain errors stay an explicit ADT per `development-guidelines.md`
  (e.g. an unrecognized `status` value on a consumed event), even with no HTTP
  write edge to map them to — log them loudly rather than silently dropping,
  same reliability stance `system-design.md` already applies to the producer
  side (order-service/payment-service's publishers)
- Swagger/OpenAPI docs (`purerest.docs.Docs`) cover every served route,
  including `/health`, `/health/ready` and `/emails` (`/docs` +
  `/docs/docs.yaml`, TD-20.9). Still no CRUD API or write-DTO surface to
  document.

## Testing
- scalafmt + munit + munit-cats-effect
- Testcontainers Kafka for the consumers (publish synthetic events, assert
  the stubbed send is invoked with the right content) and Testcontainers
  Postgres for the ADM-2 migration, store and `/emails` route suites
- `sbt scalafmtCheck test` before every commit (CI enforces the same)
