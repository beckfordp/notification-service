# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance
- [ ] Task: Bump `project/plugins.sbt`'s `sbt-purekafka-docs` `0.1.0-dev`
      -> `0.2.0-dev`; bump `build.sbt`'s `purekafkaVersion` `0.2.1` ->
      `0.3.0`
- [ ] Task: Add `schemaRegistryUrl`/`schemaRegistryGroupId` settings to
      `build.sbt` (same env-var-override shape as the prior services)
- [ ] Task: New `scripts/verify-schema-registry-check.sh` (mirroring the
      prior services' own scripts), run against the real local Apicurio
      instance: registers the one real channel (`OrderStatusChanged`) +
      sets the `BACKWARD` rule on first run; a second identical run is a
      no-op
- [ ] Task: Run full test suite + the verify script end-to-end; confirm
      no regressions
- [ ] Task: Conductor - User Manual Verification 'Phase 1: Local sbt
      wiring + verification' (Protocol in workflow.md)

## Phase 2: Real CI wiring
- [ ] Task: Add the same `docker run` Apicurio CI step shape as the
      prior services' `ci.yml` to this repo's `ci.yml`
- [ ] Task: Push this branch and confirm the new CI step passes on a
      real GitHub Actions run (`GH_PACKAGES_TOKEN` already confirmed set)
- [ ] Task: Conductor - User Manual Verification 'Phase 2: Real CI
      wiring' (Protocol in workflow.md)
