# Spec: Backport CORS support from pure-service-generator (TD-1.3)

## Overview
notification-service has no domain REST routes (pure Kafka consumer; only `/health`/`/health/ready` for k8s probes) — confirmed via user decision to still backport the same proven pattern for consistency across all 6 services, even though it only wraps the health endpoints.

## Functional Requirements
1. Add `Cors.scala` (package `notificationservice`) — identical to the proven `Cors.middleware[F[_]: Async]`: allow-all origins, GET/POST/PATCH/PUT/DELETE, `Content-Type` header, credentials disallowed.
2. Wire `Cors.middleware` into `Main.scala`, wrapping the composed `routes` (tracing + metrics over `HealthRoutes.routes`) before `.withHttpApp(routes.orNotFound)`.
3. Add `CorsSuite.scala` — same 3 assertions, but against this service's actual `/health` route (GET-only; no POST route exists anywhere in this service, so the dummy routes use GET /health instead of the usual GET+POST pair).

## Non-Functional Requirements
- No new dependency.
- No change to notification-service's Kafka consumer logic.

## Acceptance Criteria
- [ ] `CorsSuite` passes (3 tests), 100% coverage on `Cors.scala`.
- [ ] Full `sbt test` suite green, no regressions.
- [ ] Real verification via `scripts/verify-cors.sh`: GET `/health` with `Origin` shows ACAO; OPTIONS preflight shows correct Allow-Methods/-Headers.

## Out of Scope
- The CORS policy itself (already decided at the generator level).
- Adding any actual domain routes to this service — out of scope, unrelated to TD-1.3.
