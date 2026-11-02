# Specification: ADM-6 — notification Postgres readiness

## Overview

ADM-2 added the `sent_email` Postgres store, but `/health/ready` still always
returned 200 and the local cluster had no notification Postgres. gluon adds
the `notification-postgres` manifest and Helm values; this track gates
readiness on the store.

## Functional Requirements

- `SentEmailStore.ping` (`SELECT 1`, `false` on any failure).
- `/health/ready`: 200 when Postgres answers, 503 otherwise; `/health`
  unchanged. Documented 503; `verify-rest-docs.sh` updated.

## Non-Functional
- TDD; `sbt scalafmtCheckAll test` green.

## Out of Scope
- Manifests/values/exporter wiring (gluon repo, same ADM-6).
