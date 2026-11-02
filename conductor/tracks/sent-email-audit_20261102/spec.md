# Specification: ADM-2 — sent-email audit in notification-service

## Overview

notification-service consumes five event streams and sends stub emails with no
record and no read API. The Admin console's Customers tab needs per-customer
email history. This track gives the service a Postgres datastore, a
`sent_email` audit table keyed by the envelope `eventId`, and
`GET /emails?customerId=&orderId=`. It is an email audit/send queue, not a
Kafka outbox: the row is written by the sending path, not by a producer
transaction.

## Functional Requirements

### 1. Persistence and migrations

- Add Skunk, Flyway (core + postgresql), PostgreSQL JDBC (Runtime) and
  Testcontainers-PostgreSQL, mirroring order-service, including the otel4s
  dependency overrides.
- `PostgresConfig` plus a `postgres` block in `application.conf` with
  `POSTGRES_*` env overrides (host/port/db/user/password); local defaults
  only, no secrets.
- `Migrations` runs on startup before consumers and routes.
- Flyway `V1__create_sent_email.sql` creates `sent_email`:
  - `id uuid primary key`
  - `source_event_id text not null unique`
  - `event_type text not null`
  - `reason text null`
  - `recipient_type text not null check in ('customer','email','order')`
  - `recipient_key text not null`
  - `customer_id text null`, `order_id text null`
  - `subject text not null`, `body text not null`
  - `status text not null check in ('pending','sent','failed')`
  - `created_at timestamptz not null default now()`, `sent_at timestamptz
    null`, `failed_at timestamptz null`, `error text null`
  - indexes on `(customer_id, created_at desc)` and
    `(order_id, created_at desc)`

### 2. EmailClient contract and recipient mapping

- `EmailClient.send` takes an `EmailMessage` carrying `sourceEventId`,
  `eventType`, `reason`, `recipientType`, `recipientKey`, `customerId`,
  `orderId`, `subject`, `body`.
- `EnvelopeHandler` passes the decoded envelope's `eventId` to each route so
  every consumer can build the message.
- Recipient mapping:
  - `OrderConfirmed` / `OrderPaymentFailed` / `OrderCancelled` →
    `recipient_type=customer`, `recipient_key=customer_id=customerId`,
    `order_id=orderId`.
  - `PaymentCaptured` / `PaymentFailed` → `recipient_type=order`,
    `recipient_key=order_id=orderId`, customer null.
  - `DispatchConfirmed` / `DispatchFailed` → `recipient_type=order`,
    `recipient_key=order_id=orderId`, customer null.
  - `UserRegistered` → `recipient_type=email`, `recipient_key=email`,
    `customer_id=userId` (gshop uses `userId` as `customerId`).
- `reason` stores the event-supplied cancellation/failure reason where present
  (`OrderCancelled.reason`, `OrderPaymentFailed.reason`, `PaymentFailed.reason`,
  `DispatchFailed.reason`); null otherwise.

### 3. Send lifecycle and idempotency

- Insert the row as `pending`; if `source_event_id` already exists:
  - status `sent` → log duplicate, do not send again;
  - status `pending`/`failed` → re-attempt on the same row (set `pending`,
    clear error), so a retry can finish. At most one successful email per
    event.
- On successful stub send: `status=sent`, `sent_at=now()`.
- On send error: `status=failed`, `failed_at=now()`, `error` message, then
  rethrow so `DeadLetter.guard` retries and finally DLQs.

### 4. Read API

- `GET /emails?customerId=&orderId=`: both filters optional, combined with
  AND; newest first by `created_at desc`; JSON array with camelCase fields
  `id`, `sourceEventId`, `eventType`, `reason`, `recipientType`,
  `recipientKey`, `customerId`, `orderId`, `subject`, `body`, `status`,
  `createdAt`, `sentAt`, `failedAt`, `error`.
- Unknown filter values return an empty array; no auth (local/demo read API).
- Defined with tapir and registered through `purerest.docs.Docs.routes` in
  `AppRoutes` so the TD-20 docs-coverage test passes; CORS applies.

### 5. Health

- `/health` and `/health/ready` behavior unchanged in this track.

## Non-Functional Requirements

- Pure-FP runtime: Skunk repository; Flyway stays startup-only, as in
  order-service.
- A DB failure while handling one record goes through `DeadLetter.guard`
  (bounded retries, then DLQ), never a silent drop.
- The unique constraint guarantees no duplicate audit rows; behavior is
  concurrency-safe.
- TDD; `sbt scalafmtCheck test` green; >80% coverage on new code.
- Update `conductor/tech-stack.md` (Skunk/Flyway/Postgres reintroduced) and
  `conductor/product.md` (its "no database" wording is superseded).
- No Kafka contract or registry-artifact changes.

## Acceptance Criteria

- Migration suite (Testcontainers Postgres) asserts `sent_email` columns,
  constraints and indexes.
- Store suite covers insert-pending, mark sent/failed, duplicate-id behavior
  and both filters.
- Handler suites: every event type writes the expected event id and recipient
  columns; a duplicate `sent` event calls the client once; a send failure marks
  `failed` and rethrows.
- Route suite covers each filter, both together, ordering, empty result and
  JSON shape.
- `Docs.undocumented` remains empty and the new endpoint appears in OpenAPI.
- Config suite loads the `postgres` block and env overrides.
- Live manual verification: service starts against a throwaway local Postgres,
  an event produces a row, and `GET /emails?customerId=` returns it.

## Out of Scope

- Real recipient resolution / customer attribution for payment and fulfilment
  events (ADM-3).
- A real email provider; `EmailClient` stays a logging stub.
- Admin order list, operator identity/audit fields (ADM-4), expiry trigger
  (ADM-5).
- Kubernetes/Helm values, deployment, readiness gating and docker-compose
  Postgres (ADM-6).
- gshop Customers-tab wiring (ADM-8) and the full admin demo (ADM-9).
- Pagination, retention/cleanup and a resend endpoint.
