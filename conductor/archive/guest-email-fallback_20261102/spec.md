# Specification: ADM-10 — guest email fallback

## Overview

A guest customerId has no user-service account, so ADM-3 recorded a terminal
`unresolved` row. order-service now exposes `guestEmail` on
`GET /orders/{id}`; use it as the recipient fallback.

## Functional Requirements

- `OrderClient.getContact(orderId)` → `OrderContact(customerId, guestEmail)`
  (replaces `getCustomerId`; same 400/404 → None, other → raise).
- `RecipientResolver`: when `users.getEmail` is None, use the order's
  `guestEmail` (reusing the contact from an orderId-only lookup; one extra
  lookup for order events). Absent → unchanged `unknown_customer` terminal.
  Signed-in customers never trigger the order lookup.

## Non-Functional
- TDD; `sbt scalafmtCheckAll test` green. No Kafka contract change.
