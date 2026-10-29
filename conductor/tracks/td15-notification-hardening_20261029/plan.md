# TD-15.2 Implementation Plan

## Phase 1 — Order-event split consumer

- [ ] Write failing tests: replace `OrderStatusChangedEventSuite` with decode tests for the 3 new events; update `AsyncApiChannelsSuite`/`NotificationAsyncApiDocsSuite` to expect 3 order-events subscribe channels and no `OrderStatusChanged`; confirm failure.
- [ ] Implement `OrderEvents.scala` (3 mirrors), `OrderEventsConsumer` (emails confirmed / payment failed / cancelled), `AsyncApiChannels`, `KafkaConsumers` wiring.
- [ ] Refactor: delete `OrderStatusChangedEvent.scala`/`OrderStatusChangedConsumer.scala`; rewrite the ignored consumer suite for the new events.
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Order-event split' (Protocol in workflow.md)

## Phase 2 — Observability and config

- [ ] Write failing tests: unknown-event logging and raw-payload redaction assertions across the 3 consumers; config loads `asyncApiHtmlDir`; confirm failure.
- [ ] Implement logging changes, `asyncApiHtmlDir` config/`AppRoutes`/`Main`, `application.conf` entries.
- [ ] Refactor: remove `sys.env` and all `Map("raw" -> raw)` uses.
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Observability & config' (Protocol in workflow.md)

## Phase 3 — ScalaCheck and narrower constraints

- [ ] Write failing tests: `munit-scalacheck` dependency + property tests for email mapping non-emptiness and event decode round-trips; confirm failure.
- [ ] Implement properties and narrow `Cors` to `Monad`, `HealthRoutes` health to `Applicative`, ready/routes to `Monad` where possible.
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — ScalaCheck & purity' (Protocol in workflow.md)

## Phase 4 — Verification, registry, scripts

- [ ] Update `verify-order-status-changed.sh`, `verify-kafka-asyncapi-docs.sh`, `verify-schema-registry-check.sh`.
- [ ] Run full suite + coverage, scalafmt checks.
- [ ] Run `schemaRegistryCheck`; delete the old `order-events-OrderStatusChanged` artifact; confirm 3 new mirrors.
- [ ] Run the live verification scripts against a running instance.
- [ ] Task: Conductor - User Manual Verification 'Phase 4 — Verification & registry' (Protocol in workflow.md)
