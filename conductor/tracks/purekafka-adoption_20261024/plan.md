# Plan — purekafka adoption + AsyncAPI docs for notification-service (TD-2.3, TD-2.4, ADR 0010)

## Phase 1: Main.scala restructure (Telemetry + AppRoutes) + readiness simplification [checkpoint: 9090b94]
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
- [x] Task: Conductor - User Manual Verification 'Phase 1: Main.scala
      restructure + readiness simplification' (Protocol in workflow.md)
      `9090b94` — reduced-prompting mode: automated-test verification
      only (16 passed, 6 unrelated pre-existing tests stay ignored), no
      interactive walkthrough; see git note on `9090b94`

## Phase 2: StreamSupervisor + DeadLetter adoption (TD-2.3, TD-2.4) [checkpoint: 0528d48]
- [x] Task: Bump `testcontainersScalaVersion` to `0.44.1` in `build.sbt`
      (fixes this repo's Docker-API-version Testcontainers blocker) `badefa6`
- [x] Task: `KafkaConfig` gains `dlqRetry`/`dlqPublishResilience`/
      `streamSupervisor` fields + `application.conf` keys `badefa6`
- [x] Task: Bump `io.github.beckfordp:purekafka` to 0.2.1 `badefa6`
- [x] Task: `OrderStatusChangedConsumer.run` gains a `deadLetter:
      DeadLetter[F, String, String]` param; wrap `handleEvent(...)` in
      `deadLetter.guard(topic, key, raw)(...)` `badefa6` — also updated
      the 1 existing (ignored) test call site with a no-op `DeadLetter`
      helper (same `noOpDeadLetter` pattern as order-service's/
      payment-service's own test suites), purely for compile
      compatibility; the suite's own bug is untouched, still `.ignore`'d
- [x] Task: New `KafkaConsumers.scala` — wraps
      `OrderStatusChangedConsumer.run` in `StreamSupervisor.supervise`,
      backgrounds it `badefa6`
- [x] Task: New `KafkaConsumersSuite.scala` (new, isolated file — not
      touching `OrderStatusChangedConsumerSuite`) `badefa6` — deviation:
      no group-id override needed. The cross-test-contamination risk is
      specific to multiple tests sharing one container (true of
      `OrderStatusChangedConsumerSuite`'s 6 tests); this new suite has
      exactly one test with its own dedicated container
      (`TestContainerForAll` starts one per class), so the default
      hardcoded group id is safe as-is - matches payment-service's own
      `KafkaConsumersSuite` precedent exactly.
- [x] Task: Wire `KafkaConsumers.resource` into `Main.scala` `badefa6`
- [x] Task: Conductor - User Manual Verification 'Phase 2:
      StreamSupervisor + DeadLetter adoption' (Protocol in workflow.md)
      `0528d48` — reduced-prompting mode: automated-test verification
      only (17 passed, including a real Testcontainers-Kafka DLQ-routing
      test), no interactive walkthrough; see git note on `0528d48`

## Phase 3: AsyncAPI docs [checkpoint: 90c9601]
- [x] Task: Add tapir `Schema.derived` to `OrderStatusChangedEvent`
      `cf3f4b7` — deviation: `status: OrderStatusChanged` is a real ADT
      (not a plain `String` like order-service's own copy of this
      event), so `Schema.derived` alone would misdocument it as a
      coproduct/oneOf object. Wrote an explicit
      `Schema.string[OrderStatusChanged]` + `Validator.enumeration`
      (confirmed `Schema.string[T]` exists in tapir-core's `Schema.scala`)
      instead, matching order-service's enum-validator technique adapted
      for a real sealed trait instead of a plain String field.
- [x] Task: New `AsyncApiChannels.scala` — `OrderStatusChangedEvent` on
      `order-events` (`Subscribe`) `cf3f4b7`
- [x] Task: `project/plugins.sbt` + `build.sbt` — `sbt-purekafka-docs`/
      `PureKafkaDocsPlugin` wiring `cf3f4b7`
- [x] Task: `AppRoutes.scala` gains the raw-spec + static-HTML AsyncAPI
      routes (same route-ordering fix), `channels` param added to
      `AppRoutes.all` `cf3f4b7`
- [x] Task: Adapt `verify-kafka-asyncapi-docs.sh` (own port; confirms
      `/health/ready` returns 200 as part of its readiness-wait step)
      `cf3f4b7`
- [x] Task: README "View Kafka AsyncAPI docs" section `cf3f4b7`
- [x] Task: Run full test suite + verify script end-to-end — 17/17
      passing, scalafmtCheck clean, verify script all green (build-time
      generation + live raw-spec + live static HTML + /health/ready 200)
- [x] Task: Conductor - User Manual Verification 'Phase 3: AsyncAPI
      docs' (Protocol in workflow.md) `90c9601` — reduced-prompting
      mode: end-to-end script verification only
      (`./scripts/verify-kafka-asyncapi-docs.sh`, all checks passed,
      including /health/ready 200); see git note on `90c9601`

## Phase: Review Fixes
- [x] Task: Apply review suggestions `6e41a97`
