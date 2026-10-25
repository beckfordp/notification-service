# Spec — purekafka adoption + AsyncAPI docs for notification-service (TD-2.3, TD-2.4, ADR 0010)

## Overview
notification-service's `OrderStatusChangedConsumer` hand-rolls its own
restart signaling: `onFinalizeCase` flips a `ready: Ref[F, Boolean]` to
`false` on any stream error, with no actual retry/resubscribe — found
2026-10-09 restarting OrbStack, this is the one service that didn't
self-heal, staying `/health/ready` 503 forever until a manual `kubectl
rollout restart`. This track replaces that with `purekafka`'s
`StreamSupervisor` (restart-with-backoff, TD-2.3) and adds `DeadLetter`
(bounded-retry-then-DLQ, TD-2.4 — the last of 4 Kafka-consuming services),
restructures `Main.scala` to the `Telemetry`/`AppRoutes`/`KafkaConsumers`
shape the other two tracks established, and wires AsyncAPI documentation
for the consumed `OrderStatusChanged` channel — closing the final gap in
`gluon/PLAN.md` Rework 3's "Adopt across..." bullet (3 of 3).

Also fixes this repo's own Testcontainers blocker (diagnosed during
scoping): `testcontainersScalaVersion` 0.43.6 ships a `docker-java` client
hardcoded to Docker API 1.32, rejected by this machine's Docker daemon
(requires ≥1.40) — `purerest` already fixed this itself by bumping to
0.44.1. Needed so this track's own new Testcontainers-Kafka test can
actually run (not just compile).

## Functional Requirements
- **Testcontainers fix**: bump `testcontainersScalaVersion` to `0.44.1` in
  `build.sbt`.
- **Main.scala restructure**: new `Telemetry.scala` (tracer/meter/logger
  bundle) and `AppRoutes.scala`, mirroring the other two tracks. Since this
  service has no business HTTP routes or `purerest.docs.Docs` OpenAPI docs
  at all, `AppRoutes.all` here is just health + AsyncAPI routes.
  `Main.scala` becomes a flat `Resource` for-comprehension.
- **Readiness simplification**: remove the `ready: Ref[F, Boolean]`
  mechanism entirely — `OrderStatusChangedConsumer.run` drops the `ready`
  param and `onFinalizeCase` hook; `HealthRoutes.readyServerEndpoint`
  always returns 200 (mirrors `healthServerEndpoint`). Confirmed decision:
  the original incident is structurally fixed by `StreamSupervisor`, and
  there's no other dependency (no DB/store) left to gate readiness on.
- **KafkaConsumers.scala** (new): wraps `OrderStatusChangedConsumer.run` in
  `StreamSupervisor.supervise` + `DeadLetter`, backgrounds it —
  single-consumer version, mirroring payment-service's own
  `KafkaConsumers.scala`.
- **Config**: `KafkaConfig` gains `dlqRetry`/`dlqPublishResilience`/
  `streamSupervisor` fields (mirroring `OrderServiceConfig.KafkaConfig`
  exactly) + `application.conf` keys.
- **OrderStatusChangedConsumer**: gains a `deadLetter: DeadLetter[F,
  String, String]` param; `handleEvent(...)` (email send + log) wrapped in
  `deadLetter.guard(topic, key, raw)(...)`. Decode failures stay
  logged-and-dropped, unchanged — matches the established convention
  exactly.
- **AsyncApiChannels.scala** (new): registers `OrderStatusChangedEvent` on
  `order-events` as `Subscribe`. Requires adding tapir `Schema[A]`
  derivation to `OrderStatusChangedEvent`.
- **sbt-purekafka-docs wiring**: `project/plugins.sbt` + `build.sbt`,
  `AppRoutes.scala` serves raw spec + static HTML (same route-ordering
  fix).
- **Verify script + README**: adapt from the other two services (own
  port).

## Non-Functional Requirements
- Bump `io.github.beckfordp:purekafka` to 0.2.1.
- Does **not** touch/un-ignore the pre-existing
  `OrderStatusChangedConsumerSuite`'s 6 tests — that suite has a separate,
  real cross-test-contamination bug (shared consumer group id + shared
  container across tests) exposed once Testcontainers actually starts
  successfully. Left `.ignore`'d, flagged as its own backlog item.
- DLQ guard scope matches order-service's/payment-service's established
  convention exactly.

## Acceptance Criteria
- `sbt test` green (existing suite's 6 tests stay `.ignore`'d, not newly
  broken), `scalafmtCheck` clean.
- A new Testcontainers-Kafka test (in a new, isolated test file — not
  touching `OrderStatusChangedConsumerSuite`) proves a permanently-failing
  message is routed to `order-events-dlq` after exhausting retries, and
  the stream keeps consuming later messages (mirroring payment-service's
  `KafkaConsumersSuite`).
- `sbt compile` produces `target/asyncapi-docs-spec/asyncapi.json` +
  `target/asyncapi-docs/index.html` with `OrderStatusChanged`.
- `./scripts/verify-kafka-asyncapi-docs.sh` passes end-to-end.
- `/health/ready` returns 200 unconditionally (manual/live check).

## Out of Scope
- Fixing `OrderStatusChangedConsumerSuite`'s cross-test-contamination bug
  or un-ignoring its tests — separate backlog item.
- A new transient-not-ready signal tied to `StreamSupervisor`'s backoff
  state — decided against; see Readiness simplification above.
- Real email provider integration.
