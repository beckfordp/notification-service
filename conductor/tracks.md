# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Fix local Testcontainers/Docker-Desktop compatibility (or wire scripts/verify-order-status-changed.sh into CI) so OrderStatusChangedConsumerSuite's Kafka integration tests aren't permanently .ignore'd
- TD-2.3 / TD-2.4 (ADR 0010): `OrderStatusChangedConsumer` doesn't self-heal
  from a dropped Kafka connection — found 2026-10-09 restarting OrbStack:
  every other service's background consumer/publisher recovered on its own
  once Kafka came back up, but this one's consumer stream permanently flips
  `readyRef` to `false` on any stream error (`onFinalizeCase`'s
  `ExitCase.Errored` branch) with no retry/resubscribe, so `/health/ready`
  stays `503` forever until the whole pod is restarted — only fixed by a
  manual `kubectl rollout restart`. **Decided fix**: adopt `purekafka`'s
  stream supervision (restart-with-backoff on any stream error, unbounded
  attempts) instead of hand-rolling a retry/resubscribe loop, plus
  dead-letter routing (a message that still fails after retries goes to
  `<topic>-dlq` instead of crashing or blocking). Requires
  `io.github.beckfordp:purekafka` published (`sbt publishLocal` in the
  sibling `purerest` checkout) before this can compile against it.

---
