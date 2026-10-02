# Project Tracks

This file tracks all major tracks for the project.

---

## Backlog

Title-only placeholders for future tracks — not yet detailed (no spec/plan, no linked
folder), so `/conductor:implement` cannot pick these up by accident. Reorder freely as
priorities change. When ready to work on one, run `/conductor:newTrack <title>` to go
through the spec/plan questions and promote it into a real track above.

- Generate notification-service, strip Postgres/CRUD layer down to a bare Kafka consumer (infra)
- US-7.1: consume order.status-changed (reservation_failed / confirmed / payment_failed), send the matching email per status

**Note on the first line:** only half-satisfied by what's already generated — the
"Generate" half is done (this repo exists), but the "strip Postgres/CRUD layer down to
a bare Kafka consumer" half is not: the generated code still has the full CRUD
Postgres/Flyway/skunk scaffold with no field-spec ever applied (see
`conductor/product.md`'s warning). Left verbatim per the gluon:add convention — prune
or reword it yourself when you promote it.

---
