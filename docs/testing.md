# Testing guide

`go test -race ./...` covers state transitions, envelope validation, HTTP create/get, complete workflow, API idempotency, duplicate-event assignment safety, concurrent reservation, notification exhaustion/DLQ, and rejection compensation/reassignment.

The concurrency test starts two goroutines with the same resource version and asserts exactly one reservation succeeds. The duplicate test publishes the identical event ID twice and asserts one assignment. The workflow test asserts the terminal incident state is `ASSIGNED` and the same idempotency key returns the original incident.

Use `npm run build` as the strict TypeScript/UI compilation gate. Vitest covers deterministic status/severity presentation helpers; add browser component tests before treating the dashboard as fully regression-covered.

Integration test plan once persistent adapters land:

1. Start disposable PostgreSQL, Kafka, and Redis containers.
2. Apply migrations and topic configuration.
3. Kill a consumer after its DB commit but before offset commit; restart and assert one side effect.
4. Pause Kafka, commit a business change plus outbox row, resume Kafka, and assert eventual publication.
5. Stop Redis and assert durable workflow completion.
6. Run full workflow plus both compensation paths.
