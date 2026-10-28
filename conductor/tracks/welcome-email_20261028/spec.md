# Specification: US-16.4 — welcome email on UserRegistered

## Overview

user-service publishes `user-events: UserRegistered` on signup (built
2026-10-28). notification-service gains a second consumer for it that sends the
welcome email through the existing stubbed `EmailClient`, closing the backend
half of Journey 2's signup → welcome-email path.

## Functional Requirements

- **FR-1 — Event model + consumer.** Add `UserRegisteredEvent {userId, email}`
  and `UserRegisteredConsumer` on `user-events` (group
  `notification-service-user-events`, null-safe deserializers, envelope decode,
  `eventType` filter, malformed = log-and-commit, DLQ-guarded processing).
- **FR-2 — Email.** Send to the event's `email`, subject "Welcome to gshop",
  body greeting the address and confirming the account is ready. The stub logs
  rather than sending.
- **FR-3 — Supervision.** `KafkaConsumers` merges both consumers, each
  supervised and DLQ-wrapped (`order-events-dlq` / `user-events-dlq`).
- **FR-4 — AsyncAPI + registry.** `AsyncApiChannels` registers
  `UserRegistered` as `Subscribe` on `user-events`; `schemaRegistryCheck`
  cross-checks it against the producer artifact in the shared `gluon` group.
- **FR-5 — Duplicates accepted.** A redelivery sends a second welcome email;
  documented, matching the existing consumer.
- **FR-6 — Live proof.** Synthetic `UserRegistered` triggers the welcome log;
  a real signup through user-service does too.

## Non-Functional Requirements

- No new dependencies or datastore.
- Only `userId`/`email` are logged; never passwords/tokens.
- `sbt scalafmtCheck test` green.

## Acceptance Criteria

- Both consumers run supervised and independently DLQ-wrapped; health/docs
  still work.
- AsyncAPI shows both subscriptions; registry check passes.
- Live synthetic + real-signup welcome logs verified.

## Out of Scope

- Real email provider, dedupe store, the existing
  `OrderStatusChangedConsumerSuite` group-id bug, gshop UI wiring (US-16.5).
