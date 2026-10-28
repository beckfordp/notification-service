# Plan: US-17.5/18.4 — shipped / dispatch-failed emails

## Phase 1: Event model + consumer

- [x] Task: Failing tests — `DispatchConfirmedEvent`/`DispatchFailedEvent` decode; email mapping (subjects/bodies/recipient orderId); malformed payload fails decode; AsyncAPI registers both channels (Red) `642946d`
- [x] Task: Implement the two event models + `FulfilmentEventConsumer` (null-safe, filtered, DLQ-guarded) and merge it independently supervised in `KafkaConsumers` (Green) `642946d`
- [x] Task: Refactor, `sbt scalafmtCheck test` green `642946d`
- [x] Task: Conductor - User Manual Verification 'Event model + consumer' (Protocol in workflow.md) `642946d`

## Phase 2: AsyncAPI, registry, docs

- [x] Task: Register Subscribe `DispatchConfirmed`/`DispatchFailed` on `fulfilment-events`; run `schemaRegistryCheck` `642946d`
- [x] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack `642946d`
- [x] Task: Conductor - User Manual Verification 'AsyncAPI, registry, docs' (Protocol in workflow.md) `642946d`

## Phase 3: Live proof

- [x] Task: Rebuild/redeploy notification-service; `scripts/verify-fulfilment-emails.sh`: synthetic both events produce the matching stub logs (and a real dispatched order too); cleanup `642946d`
- [x] Task: Conductor - User Manual Verification 'Live proof' (Protocol in workflow.md) `642946d`

## Risks / notes

- No email/contact on fulfilment events; stub recipient is the orderId.
- Duplicate deliveries send duplicate emails.
- Existing ignored `OrderStatusChangedConsumerSuite` untouched.
