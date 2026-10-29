# TD-15.2 — Cross-repo consistency hardening (notification-service)

## Overview

Notification-service slice of gluon TD-15. Consumes the per-outcome order
events that replace `OrderStatusChanged` (TD-15.1), and applies the same
boundary/observability/testing/purity fixes: unknown-event logging, raw-payload
redaction, `asyncApiHtmlDir` config, ScalaCheck, and narrower effect bounds.

## Functional Requirements

1. **Order-event split consumer** — consume `OrderConfirmed`,
   `OrderPaymentFailed {reason}`, `OrderCancelled` from `order-events`; retire
   the `OrderStatusChanged` mirror/consumer and its legacy
   `reservation_failed` email. Emails: order confirmed, payment failed, order
   cancelled.
2. **Observability** — unknown `eventType` logged with topic/type in every
   consumer (order, payment, fulfilment); raw payloads never logged (topic /
   byte length / error class instead).
3. **Config** — `asyncApiHtmlDir` field with `${?ASYNCAPI_HTML_DIR}` override;
   no `sys.env` in `AppRoutes`.
4. **Tests** — add `munit-scalacheck`; pure properties for the email mapping
   and event decoding.
5. **Effect constraints** — `Cors` → `Monad`; `HealthRoutes` health →
   `Applicative`, ready/routes → `Monad` (routes keep `Async` where http4s
   requires it).

## Non-Functional Requirements

- Behaviour-preserving apart from the retired event type.
- `sbt scalafmtCheckAll scalafmtSbtCheck test` green; coverage >80%.
- AsyncAPI docs and registry mirrors match the order-service producer;
  `schemaRegistryCheck` green; old artifact removed.

## Acceptance Criteria

- No `OrderStatusChanged` references remain in code, tests, scripts, or docs.
- Unknown event types logged; raw payloads absent from logs (tests assert).
- Three `order-events` subscribe channels; three mirrors registered.
- Full suite, scalafmt, coverage, and registry checks pass.

## Out of Scope

- Fixing the ignored `OrderStatusChangedConsumerSuite` cross-test isolation
  (separate backlog item); the suite is rewritten for the new events but stays
  `.ignore`d.
- Producer-side changes (TD-15.1) and other services (TD-15.3–15.8).
