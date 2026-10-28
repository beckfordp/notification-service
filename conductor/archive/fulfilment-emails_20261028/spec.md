# Specification: US-17.5/18.4 — shipped / dispatch-failed emails

## Overview

notification-service consumes `fulfilment-events` and sends the customer email
for both dispatch outcomes. One consumer handles both event types, supervised
independently and merged with the existing two.

## Functional Requirements

- **FR-1 — Consumer.** New `FulfilmentEventConsumer` on `fulfilment-events`
  (group `notification-service-fulfilment-events`): null-safe, envelope decode,
  handles `DispatchConfirmed {orderId,trackingNo,items}` and
  `DispatchFailed {orderId,reason}`; unknown event types skipped; malformed
  logged and dropped; processing DLQ-guarded (`fulfilment-events-dlq`).
- **FR-2 — Emails.** DispatchConfirmed → subject "Your order has shipped",
  body naming the order and tracking number. DispatchFailed → subject "We
  couldn't ship your order", body naming the order and the reason. Stub
  recipient is the `orderId`.
- **FR-3 — Supervision.** `KafkaConsumers` supervises the new consumer
  independently and merges it with the other two.
- **FR-4 — AsyncAPI.** Subscribe `DispatchConfirmed`/`DispatchFailed` on
  `fulfilment-events`; schemaRegistryCheck passes.
- **FR-5 — Duplicates accepted.**
- **FR-6 — Live proof.** Synthetic and real events produce the matching stub
  logs.

## Non-Functional Requirements

- No new dependencies/datastore; `sbt scalafmtCheck test` green.

## Acceptance Criteria

- Both emails produced with correct content; AsyncAPI/registry updated; live
  check passes.

## Out of Scope

- Real recipient identity, real provider, receipt emails (US-19.4), dedupe.
