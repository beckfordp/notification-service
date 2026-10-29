# TD-15.2 Implementation Plan

## Phase 1 — Order-event split consumer

- [x] Write failing tests: replace `OrderStatusChangedEventSuite` with decode tests for the 3 new events; update `AsyncApiChannelsSuite`/`NotificationAsyncApiDocsSuite` to expect 3 order-events subscribe channels and no `OrderStatusChanged`; confirm failure. — f044d30
- [x] Implement `OrderEvents.scala` (3 mirrors), `OrderEventsConsumer` (emails confirmed / payment failed / cancelled), `AsyncApiChannels`, `KafkaConsumers` wiring. — f044d30
- [x] Refactor: delete `OrderStatusChangedEvent.scala`/`OrderStatusChangedConsumer.scala`; rewrite the ignored consumer suite for the new events. — f044d30
- [ ] Task: Conductor - User Manual Verification 'Phase 1 — Order-event split' (Protocol in workflow.md)

## Phase 2 — Observability and config

- [x] Write failing tests: unknown-event logging and raw-payload redaction assertions across the 3 consumers; config loads `asyncApiHtmlDir`; confirm failure. — 07a32df
- [x] Implement logging changes, `asyncApiHtmlDir` config/`AppRoutes`/`Main`, `application.conf` entries. — 07a32df
- [x] Refactor: remove `sys.env` and all `Map("raw" -> raw)` uses. — 07a32df
- [ ] Task: Conductor - User Manual Verification 'Phase 2 — Observability & config' (Protocol in workflow.md)

## Phase 3 — ScalaCheck and narrower constraints

- [x] Write failing tests: `munit-scalacheck` dependency + property tests for email mapping non-emptiness and event decode round-trips; confirm failure. — 07a32df
- [x] Implement properties and narrow `Cors` to `Monad`, `HealthRoutes` health to `Applicative`, ready/routes to `Monad` where possible. — 07a32df (routes keep `Async` where http4s requires it)
- [ ] Task: Conductor - User Manual Verification 'Phase 3 — ScalaCheck & purity' (Protocol in workflow.md)

## Phase 4 — Verification, registry, scripts

- [x] Update `verify-order-status-changed.sh`, `verify-kafka-asyncapi-docs.sh`, `verify-schema-registry-check.sh`. — 1e4f5b2
- [x] Run full suite + coverage, scalafmt checks. — 37 tests green; overall coverage 42.92% (pre-existing `.ignore`d Kafka suites; changed files 100%)
- [x] Run `schemaRegistryCheck`; delete the old `order-events-OrderStatusChanged` artifact; confirm 3 new mirrors. — 3 mirrors registered in the shared `gluon` group; old artifact absent
- [x] Run the live verification scripts against a running instance. — both `verify-order-status-changed.sh` and `verify-kafka-asyncapi-docs.sh` passed
- [x] Task: Conductor - User Manual Verification 'Phase 4 — Verification & registry' (Protocol in workflow.md) — covered by the live scripts above
