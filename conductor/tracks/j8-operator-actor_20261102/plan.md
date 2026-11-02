# Plan: Checkout Journey 8 — notification actor mirror

## Phase 1: Mirror and log

- [ ] Task: Failing tests for actor decode (present/absent) and the
  cancellation log context (Red).
- [ ] Task: Add the optional actor to the mirror and to the send log; leave
  copy and `sent_email` untouched (Green).
- [ ] Task: `sbt scalafmtCheck test schemaRegistryCheck`.
- [ ] Task: Conductor - User Manual Verification 'Notification actor mirror'
  (Protocol in workflow.md)

## Review

- Filled after `/conductor:review`.
