# Specification: US-19.4 — receipt / capture-failure emails

## Overview

notification-service consumes `payment-events` and sends the customer email for
both capture outcomes: a receipt when `PaymentCaptured` arrives (capture happens
on dispatch), and a "payment problem" email when capture fails. One consumer
handles both event types, mirroring `FulfilmentEventConsumer`.

## Functional Requirements

- **FR-1 — Consumer.** New `PaymentEventConsumer` on `payment-events` (group
  `notification-service-payment-events`): null-safe, envelope decode, handles
  `PaymentCaptured {orderId, amountCents, timestamp}` and `PaymentFailed
  {orderId, paymentId, amountCents, reason, timestamp}`; unknown event types
  skipped; malformed logged and dropped; processing DLQ-guarded
  (`payment-events-dlq`).
- **FR-2 — Emails.** PaymentCaptured → subject "Your payment was received",
  body naming the order and the amount captured (formatted `$12.34`, platform
  convention). PaymentFailed → subject "We couldn't take your payment", body
  naming the order, amount and reason. Stub recipient is the `orderId` (payment
  events carry no email/customerId).
- **FR-3 — Supervision.** `KafkaConsumers` supervises the new consumer
  independently (own DLQ) and merges it with the existing three.
- **FR-4 — AsyncAPI.** Subscribe `PaymentCaptured`/`PaymentFailed` on
  `payment-events`; `schemaRegistryCheck` passes.
- **FR-5 — Duplicates accepted.** A redelivery sends a duplicate email; no
  datastore.
- **FR-6 — Live proof.** Synthetic events produce both stub logs; a real
  dispatched order produces the receipt email via the real capture path.

## Non-Functional Requirements

- No new dependencies or datastore; `sbt scalafmtCheck test` green.
- `system-design.md`, this service's README/product/tech-stack current-state
  notes updated.

## Acceptance Criteria

- Both emails produced with correct subject/body/recipient; AsyncAPI + registry
  updated; live check passes against the local cluster; US-19.4 checked off in
  `gluon/docs/user-stories.md` and `PLAN.md`.

## Out of Scope

- gshop Order History statuses (US-19.5 — its own gshop track), auth-expiry
  email (US-20.5), real recipient identity/provider, dedupe, refunds/PDF
  receipts, currency handling beyond `$`.
