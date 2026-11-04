# Spec: Journey telemetry (gluon PERF-4.1–4.3, 4.8, 4.11)

## Overview
gluon PLAN.md Performance 4 for notification-service: adopt purerestlib 0.8.1 cross-cutting telemetry and
add the service's own business counters for the Christmas Rush dashboard.

## Functional Requirements
1. Bump purerestlib + purekafka to 0.8.1; `Main` passes `runtime.metrics` (PERF-4.1).
2. `PoolMetrics` on the pool `notification` (PERF-4.3).
3. `EventAge` in all four consumers (fulfilment, order, payment, user events) (PERF-4.2).
4. `EmailJourneyMetrics` decorator on the outermost `EmailClient`:
   `notifications.emails{event_type, outcome=sent|duplicate|unresolved|error}`
   (event types are a fixed set); `Milestones` line `order_notified` for a sent email
   carrying an order id (PERF-4.8, 4.11).

## Non-Functional Requirements
- No ids on metric labels. Coverage > 80% on new code. scalafmt clean.

## Acceptance Criteria
- Unit tests for the new metrics. Live: metrics port shows the new series after traffic.

## Out of Scope
- Spans / OTLP (Perf 5).
