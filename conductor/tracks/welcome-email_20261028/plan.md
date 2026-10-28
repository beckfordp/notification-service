# Plan: US-16.4 — welcome email on UserRegistered

## Phase 1: Event, consumer, supervision

- [ ] Task: Add failing tests — `UserRegisteredEvent` envelope decode (valid, wrong eventType skipped, malformed payload), welcome email mapping (subject/body/recipient), and AsyncAPI channel registration for `user-events: UserRegistered` (Red)
- [ ] Task: Implement `UserRegisteredEvent` + `UserRegisteredConsumer` (null-safe deserializers, log-and-commit malformed, DLQ-guarded processing, group `notification-service-user-events`) and merge it with the order-status consumer in `KafkaConsumers` (Green)
- [ ] Task: Refactor, `sbt scalafmtCheck test` green
- [ ] Task: Conductor - User Manual Verification 'Event, consumer, supervision' (Protocol in workflow.md)

## Phase 2: AsyncAPI, registry, docs

- [ ] Task: Register `UserRegistered` as `Subscribe` on `user-events`; run `schemaRegistryCheck`
- [ ] Task: Update `gluon/docs/system-design.md` (user-service consumer built), README, product.md, tech-stack
- [ ] Task: Conductor - User Manual Verification 'AsyncAPI, registry, docs' (Protocol in workflow.md)

## Phase 3: Live proof

- [ ] Task: Rebuild/redeploy notification-service; add `scripts/verify-user-registered-email.sh`: publish a synthetic `UserRegistered` and assert the structured welcome log; then sign up through the real user-service and assert it again end to end
- [ ] Task: Conductor - User Manual Verification 'Live proof' (Protocol in workflow.md)

## Risks / notes

- `OrderStatusChangedConsumerSuite` is `.ignore`d for a shared-container/group-id bug; the new consumer is proven live instead of adding another flaky suite.
- Merging two supervised streams mirrors order-service; each keeps its own DLQ topic.
- Duplicate deliveries send duplicate welcome emails by decision.
- The email stub logs `to`/`subject`/`body`; the live script greps those lines.
