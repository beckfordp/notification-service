# Spec: REST docs convention (gluon TD-20.9)

## Overview
notification-service exposes only `GET /health` and `GET /health/ready`, both
served as bare tapir endpoints and neither wrapped by `purerest.docs.Docs`.
Apply the TD-20 convention (purerest README "Documenting endpoints"), mirroring
order-service `rest-docs_20261101`, and assert it in a test.

## Functional Requirements
- FR1: purerestlib 0.4.0 → 0.7.3 (+ aligned pins); suite green;
  tech-stack.md synced.
- FR2: Serve the two health endpoints through `purerest.docs.Docs.routes`
  (the same server endpoints), so `/docs` returns the generated OpenAPI spec at
  `/docs/docs.yaml` and Swagger UI, matching every other service. AsyncAPI
  routes/static HTML stay mounted before the Swagger wildcard, as in
  order-service's `AppRoutes`. No behaviour change to `/health` or
  `/health/ready`. Per-endpoint error outputs: nothing to narrow — both
  endpoints return only 200 and declare no `errorOut`; `/health/ready` is
  deliberately unconditional 200 (no datastore to gate on, supervised Kafka
  consumers), so it must not gain a 503 variant.
- FR3: Every endpoint `.summary`/`.description`/`.tag` (`health`) — see
  Appendix.
- FR4: Parameters: none — neither endpoint takes a path, query or header input.
- FR5: DTOs: none — neither endpoint has a request or response body.
- FR6: Error variants: none — neither endpoint declares a non-2xx response.
- FR7: `NotificationDocsSuite`: `Docs.undocumented(endpoints.map(_.endpoint)) ==
  Nil` over the exact endpoints passed to `Docs.routes`.
- FR8: `scripts/verify-rest-docs.sh` checks the served `/docs/docs.yaml` for
  both operations' summaries/descriptions/tags (and confirms neither lists
  undocumented error responses).

## Non-Functional
- Wire format and runtime behaviour unchanged (the two routes keep their 200
  responses); scalafmtCheck clean.
- No secrets, ids or credentials in examples — there are no bodies or
  parameters to exemplify.

## Acceptance Criteria
- Full suite green, zero `Docs.undocumented` gaps, verify script PASS, CI green.
- `/docs/docs.yaml` and `/docs` (Swagger UI) return 200; `/docs/asyncapi.json`
  and `/docs/asyncapi/` still work.

## Out of Scope
- Kafka/AsyncAPI documentation (TD-19; unaffected).
- New endpoints, request/response bodies, authentication or error variants.
- Any change to the always-200 readiness semantics.

## Appendix: proposed descriptions (2026-11-01, pending user approval)

### Endpoints
| Endpoint | summary | description | tag |
|---|---|---|---|
| GET /health | Liveness check | 200 while the process is up. Checks no dependencies. | health |
| GET /health/ready | Readiness check | Always 200 while the process is up: this service has no datastore to gate readiness on, and the Kafka consumers are supervised with restart-with-backoff rather than left permanently dead, so there is no not-ready state to report. | health |

### Parameters
None. Neither endpoint takes a path, query, header or cookie input, so there is
no `.description`/`.example` to add.

### Request/response DTOs
None. Neither endpoint has a request or response body — both return only a 200
status code — so there are no case classes, fields, `@description`s or
`@encodedExample`s to add.

### Error variants
None. Neither endpoint declares an `errorOut`; both always return 200.
`/health/ready` intentionally never returns 503 (see product.md and
tech-stack.md), unlike order-service's database-backed readiness probe, so no
error variant is proposed.
