# Specification: ADM-3 — recipient resolution in notification-service

## Overview

notification-service sends customer email through a logging stub and records
only the identity each event carries. Payment and fulfilment events carry only
`orderId`, so those emails cannot be attributed to a customer or addressed to a
real email address. ADR 0025 decides to resolve at consumption time: orderId →
order-service `GET /orders/{id}` → customerId → user-service `GET /users/{id}`
→ email. This track implements the notification-service half.

## Functional Requirements

### 1. Persistence (V2)
- Flyway `V2__add_recipient_email.sql`:
  - add `recipient_email text` to `sent_email`;
  - backfill `recipient_email = recipient_key where recipient_type = 'email'`;
  - extend the status check with the terminal `unresolved`.
- `NewSentEmail`/`SentEmailRow` gain `recipientEmail`; claim inserts it and
  never clears `customer_id`/`recipient_email` on retry; `sent` and
  `unresolved` rows are duplicates.
- New store operations: `markResolved(id, customerId, recipientEmail)` before
  the send, and `markUnresolved(id, reason)` as a terminal outcome.

### 2. Outbound clients
- `UserClient`: `GET /users/{id}` → email; 400/404 → `None` (terminal);
  other/transport error → raise.
- `OrderClient`: `GET /orders/{id}` → customerId; 400/404 → `None`;
  other/transport error → raise.
- Both mirror order-service's `InventoryClient`: `apply(Client, Uri)` plus
  `resilient(httpClient, config, telemetry)` wrapping
  `purerest.resilience.Resilience.middleware`.
- Config: `user-client` (`USER_SERVICE_URL`, local default 8084) and
  `order-client` (`ORDER_SERVICE_URL`, local default 8083), each with a
  retry/circuit-breaker block; `ConfigReader[Uri]` and purerest resilience
  readers added.

### 3. Resolver and send lifecycle
- `RecipientResolver.resolve(message)`:
  - `recipientEmail` already known → resolved, no lookup;
  - customerId → user lookup;
  - orderId only → order lookup, then user lookup;
  - 400/404 at either hop → terminal `Unresolved(reason)`; transient errors
    propagate.
- `EmailMessage` gains `recipientEmail`. `EmailClient.send` returns
  `EmailSendOutcome` (`Sent`/`AlreadySent`/`Unresolved`) so consumer logs are
  truthful.
- `EmailClient.audited` (with a resolver): claim → duplicate short-circuit →
  resolve → `markUnresolved` and stop, or `markResolved` → transport → mark
  sent; resolution/send failures mark `failed` and rethrow for DLQ retry.
- `UserRegisteredConsumer` sets `recipientEmail` from the event.
- `Main` builds one shared Ember client with an explicit request timeout, the
  two resilient clients and the resolver.

## Non-Functional Requirements
- No Kafka payload, topic or registry change.
- Unresolved is a normal, terminal outcome — never a send to a non-address.
- Transient lookup failures follow the existing DeadLetter retry → DLQ path.
- TDD; `sbt scalafmtCheck test` green; >80% coverage on new code.
- Update `conductor/tech-stack.md` (ember-client + resilience now in use),
  `conductor/product.md` and README.

## Acceptance Criteria
- A payment/fulfilment event resolves orderId → customerId → email and stores
  both `customer_id` and `recipient_email`.
- An order event resolves customerId → email.
- `UserRegistered` performs no lookup and stores its known address.
- A 400/404 from either lookup marks the row `unresolved` and no transport
  call is made; a redelivery is a duplicate, not a retry.
- A 5xx/transport failure marks the row `failed` and rethrows.
- `markResolved` attribution survives a subsequent failed send/retry.
- `GET /emails` exposes `recipientEmail` and the new status.
- Full suite and docs-coverage tests green.

## Out of Scope
- order-service/user-service changes (independent ADM-3 tracks).
- gshop Customers-tab wiring (ADM-8).
- notification Postgres deployment/Helm values (ADM-6).
- A real email provider.
