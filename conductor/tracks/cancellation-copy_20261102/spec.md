# Specification: US-9.9 — reason-specific cancellation email

## Overview

`OrderCancelled` gains an optional `reason` (US-9.2). The cancellation email
must say why: customer request, expired authorization, stale order, or
operator action. The existing copy always claims the authorization expired.

## Functional Requirements

- Mirror adds `reason: Option[String] = None`.
- `cancelledEmail` copy:
  - `customer`: cancelled at the customer's request.
  - `auth_expired`: existing "payment authorization expired" wording.
  - `stale`: could not be completed in time.
  - `operator`: cancelled by the support team.
  - `None`/unknown: neutral "Your order was cancelled."
- Subject stays "Your order was cancelled"; the body distinguishes the reason.

## Acceptance Criteria

- Each reason produces its copy; `None` and unknown fall back neutrally.
- Old reason-less messages decode as `None`.
- `npm`-equivalent not applicable: `sbt scalafmtCheck test` green.

## Out of Scope

Payment/inventory/fulfilment consumers, gshop.
