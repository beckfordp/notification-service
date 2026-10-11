# Plan: Migrate to order-events with envelope

## Phase 1: Envelope + domain-topic consume

- [x] Task: Update `OrderStatusChangedConsumerSuite` for the new
  `order-events` topic + envelope shape, adding a new "ignores a different
  eventType" case — stays `.ignore`'d per spec.md's Non-Functional note
  (no Red/Green cycle possible without a runnable Testcontainers Kafka in
  this dev environment; edited directly instead) (c480925)
- [x] Task: Add `EventEnvelope.scala`; update `OrderStatusChangedConsumer`
  to subscribe to `order-events`, decode the envelope first, branch on
  `eventType` (handle `OrderStatusChanged`, silently skip anything else)
  (c480925)
- [x] Task: Run full `sbt test`, confirm green (c480925, 17/17 passing,
  6 `.ignore`'d)
- [x] Task: Conductor - User Manual Verification 'Envelope + domain-topic
  consume' (Protocol in workflow.md) (382abb8 — updated and live-ran
  `scripts/verify-order-status-changed.sh` against real docker-compose
  Kafka + notification-service, including a new check that an
  `OrderReserved` envelope on the same topic is silently skipped; all
  checks pass)
