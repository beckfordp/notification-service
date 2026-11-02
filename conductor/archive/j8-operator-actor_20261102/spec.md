# Specification: Checkout Journey 8 — notification actor mirror

## Overview

`OrderCancelled` v3 adds an optional typed `actor`. notification-service
already sends generic operator copy; it only needs to mirror the field and
include the actor in its structured send log so operator actions are traceable
without changing the customer-facing email or the `sent_email` schema.

## Functional Requirements

- `OrderCancelledEvent` mirror gains optional `actor: {type, id}` with the
  same wire key and inline tapir schema as the producer.
- The cancellation send log includes `actor_type` and `actor_id` when present.
- `cancelledEmail` copy is unchanged (generic “support team” for operator);
  no `sent_email`/`GET /emails` change.
- Old messages without `actor` continue to decode.

## Acceptance Criteria

- Mirror decodes with and without actor; JSON key is `type`.
- Consumer handler logs the actor for operator/customer events and leaves the
  body unchanged.
- `schemaRegistryCheck` passes against order-service’s v3 artifact;
  AsyncAPI docs include the actor.
- `sbt scalafmtCheck test` green.

## Out of Scope

Real operator identity, customer-facing operator names, gshop.
