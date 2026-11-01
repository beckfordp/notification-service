# Plan — notification-service: REST docs convention (gluon TD-20.9)

## Phase 1: purerestlib 0.7.3 [checkpoint: 3c5e877]
- [x] Task: bump `purerestlibVersion` 0.4.0 → 0.7.3 (+ aligned pins); fix API drift until suite green; sync tech-stack.md — tapir/http4s/purekafka already aligned, no drift, 56/56 3c5e877
- [x] Task: Conductor - User Manual Verification 'Phase 1: purerestlib 0.7.3' (Protocol in workflow.md) — covered by suite, 56/56 3c5e877

## Phase 2: Docs surface and per-endpoint error outputs [checkpoint: fa130d3]
- [x] Task: serve the two health server endpoints through `purerest.docs.Docs.routes` in `AppRoutes` (OpenAPI at `/docs/docs.yaml` + Swagger UI at `/docs`), keeping AsyncAPI routes/static HTML ahead of the Swagger wildcard; no error outputs to narrow (both endpoints only return 200; `/health/ready` stays unconditional 200) — `NotificationDocsSuite` coverage test red with 4 gaps, then green fa130d3
- [x] Task: Conductor - User Manual Verification 'Phase 2: Docs surface and per-endpoint error outputs' (Protocol in workflow.md) — covered by suite fa130d3

## Phase 3: Descriptions [checkpoint: fa130d3]
- [x] Task: coverage test first in `NotificationDocsSuite` (`Docs.undocumented == Nil`, red) → approved summaries/descriptions/tags on both health endpoints; green — 4 gaps → 0, no parameters, DTOs or error variants to describe (Appendix) fa130d3
- [x] Task: Conductor - User Manual Verification 'Phase 3: Descriptions' (Protocol in workflow.md) — covered by Phase 4's verify script fa130d3

## Phase 4: Smoke test [checkpoint: fa130d3]
- [x] Task: `scripts/verify-rest-docs.sh` — run service, fetch `/docs/docs.yaml`, assert both operations' summaries/descriptions/tags and no undocumented error responses; scalafmtCheck; CI green — PASS 17 checks, 58/58, 6 ignored fa130d3
- [x] Task: Conductor - User Manual Verification 'Phase 4: Smoke test' (Protocol in workflow.md) — `scripts/verify-rest-docs.sh` PASS fa130d3

## Phase: Review Fixes
- [x] Task: Apply review suggestions — no code findings; updated stale docs (README, tech-stack, product-guidelines no longer claim `/docs` is absent/404) and fixed `verify-crud-removed.sh` (`/docs` now 200 via Swagger UI; Postgres-removal check no longer assumes zero compose services) — PASS 6 checks f2f7b82
