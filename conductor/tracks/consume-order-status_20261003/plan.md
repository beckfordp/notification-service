# Plan: US-7.1 — consume order.status-changed, send the matching email per status

## Phase 1: Domain model + EmailClient stub [checkpoint: 781c050]
- [x] Task 1.1: `OrderStatusChanged` enum + event codec (TDD) [751c0ca]
  - [x] Red: `OrderStatusChangedEventSuite` — decode each of the 3 pinned wire
        strings (`reservation_failed`/`confirmed`/`payment_failed`) correctly;
        assert an unrecognized status string is a decode failure, not silently
        accepted. Run `sbt test`, confirm it fails to compile (types don't exist
        yet).
  - [x] Green: implement `OrderStatusChangedEvent.scala` (case class +
        `OrderStatusChanged` enum + custom circe decoder). Run `sbt test`, confirm
        pass.
- [x] Task 1.2: `EmailClient[F]` stub (TDD) [beb2636]
  - [x] Red: `EmailClientSuite` — `EmailClient.logging(logger).send(...)` logs a
        structured line (assert via `StructuredTestingLogger`, same pattern used
        elsewhere in Gluon's test suites) containing the recipient and subject.
        Confirm it fails (type doesn't exist).
  - [x] Green: implement `EmailClient.scala` (trait + `logging` constructor).
        Confirm pass.
- [x] Task: Conductor - User Manual Verification 'Phase 1: Domain model + EmailClient stub' (Protocol in workflow.md) [781c050]

## Phase 2: Kafka infra
- [ ] Task 2.1: `build.sbt` — add `fs2-kafka` 3.6.0 and `testcontainers-scala-kafka`
      (mirroring payment-service's exact versions/deps)
- [ ] Task 2.2: `docker-compose.yml` — add a single-node KRaft Kafka broker, copied
      verbatim from payment-service's `apache/kafka:3.8.0` service definition
- [ ] Task 2.3: `NotificationServiceConfig` — add `KafkaConfig(bootstrapServers: String)` (TDD)
  - [ ] Red: update `NotificationServiceConfigSuite` to expect a `kafka` block;
        confirm it fails against the current 3-field case class
  - [ ] Green: add `KafkaConfig`/the `kafka` field, and the
        `kafka { bootstrap-servers = ... }` block in `application.conf`. Confirm
        pass.
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Kafka infra' (Protocol in workflow.md)

## Phase 3: OrderStatusChangedConsumer + readiness wiring
- [ ] Task 3.1: Consumer happy path (TDD, Testcontainers Kafka — mirroring
      `OrderReservedConsumerSuite`'s `TestContainerForAll`/`KafkaContainer` pattern
      exactly)
  - [ ] Red: `OrderStatusChangedConsumerSuite` — produce one synthetic event per
        status to a real (test) Kafka broker; assert a `FakeEmailClient` (records
        calls to a `Ref`) receives exactly one `send` with the matching
        subject/recipient, racing the infinite consumer stream against a polled
        timeout (same `IO.race` shape as `OrderReservedConsumerSuite`). Confirm it
        fails (file doesn't exist).
  - [ ] Green: implement `OrderStatusChangedConsumer.scala` — null-safe
        `Option[String]` key/value deserializers from the start, group id
        `notification-service-order-status-changed`, decode + dispatch to
        `EmailClient`, commit offset unconditionally. Confirm pass.
- [ ] Task 3.2: Bad-event handling (TDD)
  - [ ] Red: extend the suite — a malformed-JSON record and a null-*value* record
        (this topic's analogue of `OrderReservedConsumerSuite`'s null-key test) are
        both logged and skipped, and a later good event on the same topic still
        triggers its email. Confirm it fails.
  - [ ] Green: implement the decode-failure/null-value branches (log error, skip,
        commit). Confirm pass.
- [ ] Task 3.3: Readiness wiring (TDD)
  - [ ] Red: update `HealthRoutesSuite` — `readyServerEndpoint[F]` now takes a
        `Ref[F, Boolean]`; returns `200` when `true`, `503` when `false`. Confirm
        it fails against the current no-arg signature.
  - [ ] Green: implement the ref-based `readyServerEndpoint`; wire
        `OrderStatusChangedConsumer`'s stream with `.onFinalizeCase` to flip the
        ref `false` only on an `Errored` exit. Confirm pass.
- [ ] Task: Conductor - User Manual Verification 'Phase 3: OrderStatusChangedConsumer + readiness wiring' (Protocol in workflow.md)

## Phase 4: Wire Main.scala + end-to-end verification
- [ ] Task 4.1: `Main.scala` — load `KafkaConfig`, build `EmailClient.logging`,
      start the consumer via `.compile.drain.background.use`, pass the health ref
      into `HealthRoutes`, serve routes. Run `sbt compile`, confirm clean.
- [ ] Task 4.2: `scripts/verify-order-status-changed.sh` — `docker compose up -d`
      (now includes Kafka), `sbt run` (logging to a file, same pattern as the
      prior track's scripts), publish one real `order.status-changed` event per
      status to the local broker, grep the service's log output for each status's
      email log line, and confirm `/health/ready` reports `200` throughout. Run
      it, confirm all checks pass.
- [ ] Task: Conductor - User Manual Verification 'Phase 4: Wire Main.scala + end-to-end verification' (Protocol in workflow.md)
