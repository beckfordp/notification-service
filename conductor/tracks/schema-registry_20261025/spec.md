# Spec — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Overview
Wire notification-service's existing `AsyncApiChannels` registration into
`sbt-purekafka-docs`'s `schemaRegistryCheck` task (purerest `v0.3.0`/
`sbt-purekafka-docs 0.2.0-dev`, already really published to GitHub Packages),
so this service's one registered Kafka event schema is checked/registered
against Apicurio Registry — locally against the real OrbStack instance, and
in real CI via an ephemeral container. Reuses the exact design settled in
the three prior services' equivalent tracks (no new design questions):
default `schemaRegistryGroupId`, CI via a throwaway `docker run` Apicurio
container, no new purerest release needed.

## Functional Requirements
- Bump `project/plugins.sbt`'s `sbt-purekafka-docs` version `0.1.0-dev` ->
  `0.2.0-dev`.
- Bump `build.sbt`'s `purekafkaVersion` `0.2.1` -> `0.3.0`.
- Add `schemaRegistryUrl` (local-dev default: the OrbStack cluster-DNS
  address, env-var overridable) / `schemaRegistryGroupId` (plugin default,
  `name.value` = `"notification-service"`) settings to `build.sbt`.
- Add the same CI step shape as the prior three services' `ci.yml`:
  `docker run` a plain `apicurio/apicurio-registry:3.3.2` container, wait
  for `/health/ready` on port 9000, run `sbt schemaRegistryCheck` against
  it, tear the container down.
- New `scripts/verify-schema-registry-check.sh` (mirroring the prior
  services' scripts), registering this service's one real channel
  (`OrderStatusChanged`).

## Non-Functional Requirements
- No change to runtime behavior — build/CI-time only.
- `notification-service`'s CI already has the `GH_PACKAGES_TOKEN` repo
  secret (confirmed set, 2026-10-25 — the user added it ahead of this
  track).

## Acceptance Criteria
- `sbt schemaRegistryCheck` succeeds locally against the real OrbStack
  Apicurio instance, registering the one channel.
- The new CI step passes on a real GitHub Actions run.
- Full existing test suite + the new verify script run clean, no
  regressions.

## Out of Scope
- `pure-service-generator` template retrofit (Rework 5 step 4, separate
  track — this is the last of the 4 service-adoption tracks).
- Any change to the Kafka-to-email relay logic itself.
