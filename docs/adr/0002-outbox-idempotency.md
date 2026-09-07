# ADR 0002: Transactional outbox and processed-event ledger

Status: accepted

## Decision

Every event producer writes its business state and envelope to an outbox in one transaction. A relay publishes in creation order and sets `published_at` only after acknowledgement. Every consumer inserts `(consumer_name,event_id)` in the same transaction as its side effects. A successful DLQ publish is treated as handled; replay removes that marker for the selected consumer.

## Consequences

The design provides at-least-once delivery without dual-write loss. Duplicate publication and processing remain possible, so event IDs and uniqueness constraints are part of correctness, not merely optimization.

