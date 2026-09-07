# Operations and failure-simulation runbook

## Triage order

1. Check `/api/v1/health/services` and Prometheus target health.
2. Query `rescueflow_outbox_backlog`; a sustained increase indicates broker or relay trouble.
3. Query `rescueflow_dead_letter_total` and inspect `GET /api/v1/operations/dead-letters`.
4. Use correlation ID, incident ID, then event ID to follow the workflow. Do not search by payload text.
5. Confirm that compensation occurred before manually changing state.

## Replay a dead letter

Inspect first, correct the transient/root cause, then replay one entry:

```bash
curl http://localhost:8080/api/v1/operations/dead-letters
curl -X POST 'http://localhost:8080/api/v1/operations/dead-letters?index=0'
```

Replay clears only the failed consumer's processed marker. Handlers remain idempotent. Never bulk replay without checking downstream capacity.

## Failure simulation

- Notification: set `Workflow.FailNotificationsFor` in a test; values ≥3 force DLQ exhaustion.
- Broker outage in the target adapter: stop Kafka after incident transaction commit; outbox rows must remain unpublished, then drain when Kafka returns.
- Redis outage: stop Redis. The durable path must continue because cache misses fall through to owned databases.
- Reservation race: run `go test -race ./internal/platform -run ConcurrentReservation -count=100`.
- Rejection: `POST /api/v1/incidents/{id}/reject-assignment`; verify resource release and a different assignment ID.

## Grafana queries

- Backlog: `rescueflow_outbox_backlog`
- Error ratio: `rate(rescueflow_http_errors_total[5m]) / rate(rescueflow_http_requests_total[5m])`
- Retry rate: `rate(rescueflow_retry_total[5m])`
- Reservation conflicts: `rate(rescueflow_reservation_conflicts_total[5m])`

## Recovery cautions

Do not delete outbox rows to reduce backlog. Do not mark DLQ records processed without either replaying or recording a deliberate discard decision. Never mutate incident history; append a corrective state transition with a reason.

