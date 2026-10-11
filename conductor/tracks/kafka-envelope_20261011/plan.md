# Plan: Migrate to order-events with envelope

## Phase 1: Envelope + domain-topic consume

- [ ] Task: Update `OrderStatusChangedConsumerSuite` for the new
  `order-events` topic + envelope shape, adding a new "ignores a different
  eventType" case — stays `.ignore`'d per spec.md's Non-Functional note
  (no Red/Green cycle possible without a runnable Testcontainers Kafka in
  this dev environment; edited directly instead)
- [ ] Task: Add `EventEnvelope.scala`; update `OrderStatusChangedConsumer`
  to subscribe to `order-events`, decode the envelope first, branch on
  `eventType` (handle `OrderStatusChanged`, silently skip anything else)
- [ ] Task: Run full `sbt test`, confirm green
- [ ] Task: Conductor - User Manual Verification 'Envelope + domain-topic
  consume' (Protocol in workflow.md)
