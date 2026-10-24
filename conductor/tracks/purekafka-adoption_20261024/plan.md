# Plan — purekafka adoption + AsyncAPI docs for notification-service (TD-2.3, TD-2.4, ADR 0010)

## Phase 1: Main.scala restructure (Telemetry + AppRoutes) + readiness simplification [checkpoint: PENDING]
- [x] Task: New `Telemetry.scala`, mirroring the other two tracks `cead625`
- [x] Task: New `AppRoutes.scala` — health + CORS/tracing/metrics
      assembly only (no business/OpenAPI routes exist here); no AsyncAPI
      routes yet (Phase 3) `cead625`
- [x] Task: `HealthRoutes.readyServerEndpoint` drops its `ready: Ref`
      param, always returns 200; `HealthRoutes.routes` simplified to
      match `cead625`
- [x] Task: Rewrite `Main.scala` as a flat `Resource` for-comprehension
      calling `AppRoutes.all` (consumer wiring stays inline for now —
      extracted into `KafkaConsumers` in Phase 2, dropping the `readyRef`
      param it currently passes) `cead625`
- [x] Task: `OrderStatusChangedConsumer.run` drops its
      `ready`/`onFinalizeCase` plumbing (no behavior to replace it with
      — confirmed decision) `cead625` — also updated the 1 existing
      (ignored) test call site to the new 3-arg signature; the suite's
      own cross-test-contamination bug is untouched, still `.ignore`'d
- [x] Task: Run full test suite, confirm no regressions (existing
      `.ignore`'d suite stays ignored/untouched) — 16/16 passing (6
      ignored), scalafmtCheck clean
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Main.scala
      restructure + readiness simplification' (Protocol in workflow.md)

## Phase 2: StreamSupervisor + DeadLetter adoption (TD-2.3, TD-2.4)
- [ ] Task: Bump `testcontainersScalaVersion` to `0.44.1` in `build.sbt`
      (fixes this repo's Docker-API-version Testcontainers blocker)
- [ ] Task: `KafkaConfig` gains `dlqRetry`/`dlqPublishResilience`/
      `streamSupervisor` fields + `application.conf` keys
- [ ] Task: Bump `io.github.beckfordp:purekafka` to 0.2.1
- [ ] Task: `OrderStatusChangedConsumer.run` gains a `deadLetter:
      DeadLetter[F, String, String]` param; wrap `handleEvent(...)` in
      `deadLetter.guard(topic, key, raw)(...)`
- [ ] Task: New `KafkaConsumers.scala` — wraps
      `OrderStatusChangedConsumer.run` in `StreamSupervisor.supervise`,
      backgrounds it
- [ ] Task: New `KafkaConsumersSuite.scala` (new, isolated file — not
      touching `OrderStatusChangedConsumerSuite`) — Testcontainers-Kafka
      test proving a permanently-failing message is routed to
      `order-events-dlq` and the stream keeps consuming later messages,
      using its own unique consumer group id to avoid the
      cross-test-contamination bug found during scoping
- [ ] Task: Wire `KafkaConsumers.resource` into `Main.scala`
- [ ] Task: Conductor - User Manual Verification 'Phase 2:
      StreamSupervisor + DeadLetter adoption' (Protocol in workflow.md)

## Phase 3: AsyncAPI docs
- [ ] Task: Add tapir `Schema.derived` to `OrderStatusChangedEvent`
- [ ] Task: New `AsyncApiChannels.scala` — `OrderStatusChangedEvent` on
      `order-events` (`Subscribe`)
- [ ] Task: `project/plugins.sbt` + `build.sbt` — `sbt-purekafka-docs`/
      `PureKafkaDocsPlugin` wiring
- [ ] Task: `AppRoutes.scala` gains the raw-spec + static-HTML AsyncAPI
      routes (same route-ordering fix), `channels` param added to
      `AppRoutes.all`
- [ ] Task: Adapt `verify-kafka-asyncapi-docs.sh` (own port; confirms
      `/health/ready` returns 200 as part of its readiness-wait step)
- [ ] Task: README "View Kafka AsyncAPI docs" section
- [ ] Task: Run full test suite + verify script end-to-end
- [ ] Task: Conductor - User Manual Verification 'Phase 3: AsyncAPI
      docs' (Protocol in workflow.md)
