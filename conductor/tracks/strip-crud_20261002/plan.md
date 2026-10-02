# Plan: Strip Postgres/CRUD layer down to a bare Kafka consumer shell

## Phase 1: Store-less readiness check
- [ ] Task 1.1: Update readiness behavior (TDD)
  - [ ] Red: update `HealthRoutesSuite` to exercise a no-arg
        `HealthRoutes.readyServerEndpoint[F]` that returns 200 with no store passed
        in; run `sbt test`, confirm it fails against the current store-requiring
        signature
  - [ ] Green: change `HealthRoutes.scala`'s `readyServerEndpoint` to drop the
        `NotificationStore` parameter and return `200` unconditionally; update its
        call site in `routes`; run `sbt test`, confirm pass
  - [ ] Refactor (optional): simplify the now-trivial `readyServerEndpoint` if useful
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Store-less readiness check' (Protocol in workflow.md)

## Phase 2: Remove Postgres/CRUD scaffold
- [ ] Task 2.1: Trim `NotificationServiceConfig` (TDD)
  - [ ] Red: update `NotificationServiceConfigSuite` to load config with no
        `postgres` block and assert the config type has no `postgres`/
        `PostgresConfig` field; run `sbt test`, confirm it fails against the current
        case class
  - [ ] Green: remove `PostgresConfig` and the `postgres` field from
        `NotificationServiceConfig.scala`; remove the `postgres { ... }` block from
        `application.conf`; run `sbt test`, confirm pass
- [ ] Task 2.2: Rewire `Main.scala`
  - Remove `Migrations.run`, the `NotificationStore.postgres(...)` resource, and all
    `NotificationRoutes.*` entries from the served endpoint list; serve only the two
    `HealthRoutes` endpoints (using Phase 1's new signature)
  - Run `sbt compile`, confirm it builds clean
- [ ] Task 2.3: Delete the dead files
  - Delete `NotificationStore.scala`, `NotificationRoutes.scala`, `Migrations.scala`,
    `NotificationError.scala`,
    `src/main/resources/db/migration/V1__create_notification_table.sql`
  - Delete the now-obsolete suites: `NotificationStoreSuite.scala`,
    `NotificationStorePostgresSuite.scala`, `MigrationsSuite.scala`,
    `NotificationRoutesSuite.scala`, `NotificationDocsSuite.scala`
  - Run `sbt scalafmtCheck test`, confirm the (now smaller) suite passes
- [ ] Task 2.4: Trim `build.sbt`
  - Remove `skunk-core`, `flyway-core`, `flyway-database-postgresql`, `postgresql`
    (Runtime JDBC), `testcontainers-scala-postgresql`
  - Check whether the `otel4s-core*` `dependencyOverrides` block (added solely for
    Skunk's otel4s conflict) is still needed; drop it if nothing else pulls a
    conflicting otel4s version, otherwise keep with an updated comment explaining why
  - Run `sbt update compile`, confirm clean resolution
- [ ] Task 2.5: Trim `docker-compose.yml`
  - Remove the `postgres` service and `postgres-data` volume, leaving the file valid
    with zero services
  - Manually confirm `docker compose config` still parses
- [ ] Task 2.6: Update `README.md`
  - Rewrite the Quickstart to drop the Postgres-start step and the `/notifications`
    CRUD curl examples
  - Describe the service as a to-be-built Kafka consumer, pointing at US-7.1 for the
    real implementation
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Remove Postgres/CRUD scaffold' (Protocol in workflow.md)
