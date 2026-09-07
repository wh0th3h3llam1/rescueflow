# Incident service boundary

Owns incidents, immutable status history, request idempotency keys, processed-event IDs, and its outbox. The current runnable composition is in `internal/platform`; the SQL target is `infra/docker/migrations/incident.sql`.
