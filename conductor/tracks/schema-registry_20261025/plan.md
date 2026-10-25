# Plan — Adopt build-time schema registry check (Rework 5 step 3, ADR 0003)

## Phase 1: Local sbt wiring + verification against the real OrbStack Apicurio instance [checkpoint: 0793af7]
- [x] Task: Bump `project/plugins.sbt`'s `sbt-purekafka-docs` `0.1.0-dev`
      -> `0.2.0-dev`; bump `build.sbt`'s `purekafkaVersion` `0.2.1` ->
      `0.3.0` `e98eb53`
- [x] Task: Add `schemaRegistryUrl`/`schemaRegistryGroupId` settings to
      `build.sbt` (same env-var-override shape as the prior services)
      `e98eb53`
- [x] Task: New `scripts/verify-schema-registry-check.sh` (mirroring the
      prior services' own scripts), run against the real local Apicurio
      instance: registers the one real channel (`OrderStatusChanged`) +
      sets the `BACKWARD` rule on first run; a second identical run is a
      no-op `e98eb53` — live-verified: artifact confirmed present under
      the real `notification-service` group; second run succeeded as a
      no-op
- [x] Task: Run full test suite + the verify script end-to-end; confirm
      no regressions `e98eb53` — found and fixed along the way,
      unrelated to this track: `scalafmtCheck`/`Test/scalafmtCheck`
      failed on 5 pre-existing unformatted test files — ran
      `Test/scalafmt` (whitespace-only diff, confirmed), then
      `scalafmtCheck; Test/scalafmtCheck; test`: clean, exit 0 (2 tests
      stay `.ignore`d, a pre-existing, already-tracked backlog item
      unrelated to this track); verify script: all checks passed
- [x] Task: Conductor - User Manual Verification 'Phase 1: Local sbt
      wiring + verification' (Protocol in workflow.md) `0793af7` —
      reduced-prompting mode: automated-test + verify-script
      verification only, no interactive walkthrough; see git note on
      `0793af7`

## Phase 2: Real CI wiring [checkpoint: TBD]
- [x] Task: Add the same `docker run` Apicurio CI step shape as the
      prior services' `ci.yml` to this repo's `ci.yml` `ec2a58b`
- [x] Task: Push this branch and confirm the new CI step passes on a
      real GitHub Actions run (`GH_PACKAGES_TOKEN` already confirmed
      set) — CI run `40186673681`: all steps green, including
      `schemaRegistryCheck` against the throwaway Apicurio container,
      first try
- [x] Task: Conductor - User Manual Verification 'Phase 2: Real CI
      wiring' (Protocol in workflow.md) — reduced-prompting mode: the
      real CI run itself is the verification; no interactive walkthrough
