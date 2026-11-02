# Plan: ADM-3 — recipient resolution

## Phase 1: Persistence

- [x] Task: Failing `MigrationsSuite` assertions for `recipient_email` and
  the `unresolved` status check; add `V2__add_recipient_email.sql` (Red →
  Green).
- [x] Task: Failing `SentEmailStorePostgresSuite` tests: `recipient_email`
  round-trip, `markResolved` survives a retry, `markUnresolved` is terminal
  and claimed as a duplicate; implement the store changes (Red → Green).
- [x] Task: Expose `recipientEmail` in `SentEmailResponse`; update
  `EmailRoutesSuite`/`NotificationDocsSuite`.
- [x] Task: Conductor - User Manual Verification 'Persistence'
  (Protocol in workflow.md) — prompting off: verified by the committed
  Testcontainers suites.

## Phase 2: Outbound clients and config

- [x] Task: Failing `UserClientSuite`/`OrderClientSuite` (200/400/404/5xx,
  request path); implement `UserClient` and `OrderClient`.
- [x] Task: Failing `NotificationServiceConfigSuite` cases; add
  `ConfigReader[Uri]`, purerest resilience readers, `user-client`/
  `order-client` blocks and `application.conf` defaults.
- [x] Task: Conductor - User Manual Verification 'Outbound clients'
  (Protocol in workflow.md) — prompting off: suites green.

## Phase 3: Resolver and send lifecycle

- [x] Task: Failing `RecipientResolverSuite` (known address, customerId path,
  two-hop orderId path, unknown order/customer, no identity, transient
  error); implement `RecipientResolver`.
- [x] Task: Failing `EmailClientSuite` additions (resolved recipient persisted
  and used; unresolved terminal and not retried; transient resolution failure
  marks failed + rethrows); add `recipientEmail`, `EmailSendOutcome` and the
  resolver to `EmailClient.audited` (Green).
- [x] Task: Update the four consumers for the outcome ADT and
  `UserRegistered`'s address; wire `Main` with one shared Ember client, both
  resilient clients and the resolver.
- [x] Task: Conductor - User Manual Verification 'Resolver and send
  lifecycle' (Protocol in workflow.md) — prompting off: full suite green.

## Phase 4: Wrap-up

- [x] Task: Update `conductor/tech-stack.md`, `conductor/product.md`, README
  and `environments/local/notification-service.values.yaml` (gluon root),
  then run `sbt scalafmtCheck test`.
- [x] Task: Conductor - User Manual Verification 'Wrap-up'
  (Protocol in workflow.md) — prompting off: full suite green (105 passed,
  6 pre-existing ignored).

## Review

- Reviewed 2026-11-02 (conductor:review): plan compliance yes; style pass —
  resolution decisions are a pure ADT, `audited` is the effectful shell,
  clients mirror `InventoryClient`, narrow bounds. Tests cover both lookup
  paths, the email-type short-circuit, terminal unresolved vs transient
  retry, persistence/idempotency, config and docs. Full suite 105 passed
  (6 pre-existing ignored). Applied during review: an email-type message now
  falls back to `recipientKey` without a lookup. No blocking findings.
