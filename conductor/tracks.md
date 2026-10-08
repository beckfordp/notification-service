# Project Tracks

This file tracks all major tracks for the project.

- [x] **Track: US-7.1: consume order.status-changed (reservation_failed / confirmed / payment_failed), send the matching email per status**
  *Link: [./tracks/consume-order-status_20261003/](./tracks/consume-order-status_20261003/)*

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Fix local Testcontainers/Docker-Desktop compatibility (or wire scripts/verify-order-status-changed.sh into CI) so OrderStatusChangedConsumerSuite's Kafka integration tests aren't permanently .ignore'd

---
