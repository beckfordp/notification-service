# Plan: Checkout Journey 8 — notification actor mirror

## Phase 1: Mirror and log

- [x] Task: Failing tests for actor decode (present/absent) and the
  cancellation log context (Red). (3b06093)
- [x] Task: Add the optional actor to the mirror and to the send log; leave
  copy and `sent_email` untouched (Green). (3b06093)
- [x] Task: `sbt scalafmtCheck test schemaRegistryCheck`.
- [x] Task: Conductor - User Manual Verification 'Notification actor mirror'
  (Protocol in workflow.md)

## Review

- Reviewed 2026-11-02: plan compliance yes; style pass; mirror decodes v3
  actor, the send log carries actor fields, copy and `sent_email` unchanged.
  Full suite 86 passed (6 pre-existing ignored); `schemaRegistryCheck` green
  against order-service v3. No blocking findings.
