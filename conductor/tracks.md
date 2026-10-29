# Project Tracks

This file tracks all major tracks for the project.

- [ ] **Track: TD-15.2 — Cross-repo consistency hardening (notification-service)**
  *Link: [./tracks/td15-notification-hardening_20261029/](./tracks/td15-notification-hardening_20261029/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Fix local Testcontainers/Docker-Desktop compatibility (or wire scripts/verify-order-status-changed.sh into CI) so OrderStatusChangedConsumerSuite's Kafka integration tests aren't permanently .ignore'd.
  Diagnosed 2026-10-24 (`purekafka-adoption_20261024` track scoping):
  the Docker-API-version half of this is fixed by that track's
  `testcontainersScalaVersion` 0.43.6→0.44.1 bump (confirmed — containers
  now actually start). But un-ignoring exposed a *second*, separate bug:
  all 6 tests share one Kafka container + a fixed consumer group id with
  `Earliest` offset reset, so a later test consumes an earlier test's
  leftover message (5 of 6 fail on customer-identity assertions once
  un-ignored). Deliberately left out of that track's scope. Fix: give
  each test its own unique consumer group id (or topic), mirroring how
  other suites avoid this.


---
