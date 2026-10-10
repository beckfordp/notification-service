# Plan: Backport CORS support from pure-service-generator (TD-1.3)

## Phase 1: CORS middleware

- [x] Task: Write failing `CorsSuite` (Red) — 3 tests against a dummy `HttpRoutes.of[IO]` wrapped in `Cors.middleware`, using GET `/health` (this service's actual route shape — no POST anywhere): (1) GET w/ Origin → ACAO header; (2) OPTIONS preflight (Access-Control-Request-Method: POST) → correct Allow-Methods/Allow-Headers — the middleware's policy is static, not route-dependent, so this still exercises the same policy regardless of whether a real POST route exists; (3) no Allow-Credentials. Confirm compile failure.
- [x] Task: Add `Cors.scala` (package `notificationservice`) — verbatim port. Run `CorsSuite` — confirm green. `e95b5b7`
- [x] Task: Wire `Cors.middleware` into `Main.scala`, wrapping the composed `routes` before `.withHttpApp(routes.orNotFound)`. Rerun full `sbt test` — confirm no regressions (17/17 passed, 5 pre-existing unrelated .ignore'd Kafka tests). `e95b5b7`
- [x] Task: Verify coverage (`sbt coverage test coverageReport`) — confirmed `Cors.scala` at 100% statement/branch coverage (overall repo coverage is low due to the pre-existing .ignore'd Kafka tests, unrelated to this change).
- [x] Task: Add `scripts/verify-cors.sh` (same shape as the others, adapted: real Kafka only, no Postgres; GET `/health` instead of a domain route) — real sbt run, curl GET+OPTIONS against `/health`, assert headers, cleanup via `trap`. Ran for real — passed end-to-end. `e41e292`
- [x] Task: Conductor - User Manual Verification 'CORS middleware' — `/prompt off` active: ran `scripts/verify-cors.sh` directly — all checks passed (GET /health: 200 + ACAO header, no Allow-Credentials; OPTIONS preflight: 200 + Allow-Methods includes POST + Allow-Headers includes Content-Type).

## Notes on `/prompt off`
Per-task Plan Mode review and commit-approval prompts skipped; each task committed directly, announced in one line.
