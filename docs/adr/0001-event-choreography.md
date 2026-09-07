# ADR 0001: Event choreography with local compensation

Status: accepted

## Context

Incident assignment spans independently owned data and cannot use a distributed database transaction.

## Decision

Use versioned Kafka events for forward progress and explicit compensation events for reversible commitments. Keep notification retries independent from the assignment transaction. Partition by incident ID.

## Consequences

Services remain autonomous and failures are observable, but the workflow is eventually consistent. Operators need event history and DLQ tooling. Compensation must be idempotent.

