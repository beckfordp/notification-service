# Plan: US-19.4 — receipt / capture-failure emails

## Phase 1: Event model + consumer

- [ ] Task: Failing tests — `PaymentCapturedEvent`/`PaymentFailedEvent` decode; email mapping (subjects/bodies/recipient orderId, incl. `$` amount formatting); malformed payload fails decode; AsyncAPI registers both channels (Red)
- [ ] Task: Implement the two event models + `PaymentEventConsumer` (null-safe, filtered, DLQ-guarded) and merge it independently supervised in `KafkaConsumers` (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Event model + consumer' (Protocol in workflow.md)

## Phase 2: AsyncAPI, registry, docs

- [ ] Task: Register Subscribe `PaymentCaptured`/`PaymentFailed` on `payment-events`; run `schemaRegistryCheck`
- [ ] Task: Update `gluon/docs/system-design.md`, README/product/tech-stack
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, docs' (Protocol in workflow.md)

## Phase 3: Live proof

- [ ] Task: Rebuild/redeploy notification-service; `scripts/verify-payment-emails.sh`: synthetic both events produce the matching stub logs, and a real dispatched order produces the receipt email; cleanup
- [ ] Task: Conductor - User Manual Verification 'Live proof' (Protocol in workflow.md)

## Risks / notes

- Payment events carry no email/customerId; stub recipient is the orderId.
- Duplicate deliveries send duplicate emails.
- Amounts formatted with the platform's `$` convention; events carry no currency field.
