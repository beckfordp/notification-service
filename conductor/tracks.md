# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Fix local Testcontainers/Docker-Desktop compatibility (or wire scripts/verify-order-status-changed.sh into CI) so OrderStatusChangedConsumerSuite's Kafka integration tests aren't permanently .ignore'd
- `OrderStatusChangedConsumer` doesn't self-heal from a dropped Kafka
  connection — found 2026-10-09 restarting OrbStack: every other service's
  background consumer/publisher recovered on its own once Kafka came back
  up, but this one's consumer stream permanently flips `readyRef` to
  `false` on any stream error (`onFinalizeCase`'s `ExitCase.Errored` branch)
  with no retry/resubscribe, so `/health/ready` stays `503` forever until
  the whole pod is restarted — only fixed by a manual
  `kubectl rollout restart`. Same class of silent-fiber-death risk already
  flagged in `gluon/docs/system-design.md`'s "Open design questions" for
  order-service's `StockEventConsumer`/inventory-service's publisher side
  — wrap the consumer stream in a restart/resubscribe loop (e.g. retry with
  backoff) instead of letting a transient broker blip kill it permanently.

---
