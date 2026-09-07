# Progress

Last updated: 2026-09-06

## Completed

- Runnable incident → dispatch → inventory → notification → assigned vertical slice
- Versioned event envelope with correlation, causation, and trace-context fields
- Atomic local outbox, relay retry, consumer idempotency, bounded retry, DLQ metadata and replay
- Incident validation, legal transition graph, immutable history, filtering, pagination, and idempotency keys
- Deterministic responder scoring, duplicate assignment protection, rejection compensation and reassignment
- Optimistic resource reservation, release, expiration primitive, and concurrency test
- Simulated notifications with configurable transient failure and persistent attempt history
- Unified REST API, consistent errors, correlation IDs, health summary, metrics, and one-way WebSocket updates
- Functional React/TypeScript dashboard for incidents, timelines, responders, resources, health, outbox, and DLQ indicators
- Docker Compose topology, per-service PostgreSQL migrations, Kafka KRaft, Redis, monitoring stack
- Grafana dashboard provisioning, Kubernetes base manifests, k6 load scenario, and GitHub Actions CI
- OpenAPI, AsyncAPI, ADRs, operational/testing/failure documentation

## Current phase

Phase 5 hardening. The dependency-free local vertical slice is verified. The infrastructure topology and persistent schemas are defined but not yet connected to the runtime adapters.

## Remaining tasks

- Replace local store and bus adapters with PostgreSQL and Kafka implementations; split the composition root into independently deployed binaries
- Add Redis cache adapter and prove fail-open behavior in an integration test
- Add OpenTelemetry SDK spans and Kafka trace-header injection (the envelope field and Tempo receiver are present)
- Add the selected internal gRPC endpoint and protobuf contract
- Add Testcontainers Kafka/PostgreSQL integration and crash-window recovery tests
- Implement assignment-expiration scheduler wiring and a test covering automatic reservation release
- Add notification-failed event publication alongside DLQ handling
- Add a Leaflet map and browser-level UI tests
- Add stateful Kubernetes dependencies or document use of managed services/Helm charts

## Known issues

- Docker is not installed on the authoring host, so Compose images and Kubernetes runtime behavior were not executed locally.
- The current Docker API container runs the verified local adapters; PostgreSQL, Kafka, and Redis are provisioned but not consumed by that binary yet.
- One-way WebSocket streaming is implemented without ping/pong or client-message handling.
- Browser-level component and WebSocket reconnection tests are not implemented; current frontend tests cover deterministic presentation helpers.

## Run and test

```text
go test -race ./...
cd web && npm ci && npm run build && npm test
docker compose up --build -d
curl http://localhost:8080/api/v1/health/services
k6 run tests/load/incidents.js
kubectl kustomize infra/kubernetes
```

## Latest verified results

- `go test -race ./...`: PASS for `internal/api`, `internal/domain`, `internal/events`, and `internal/platform` on Go 1.27.1.
- HTTP smoke test: created one synthetic medical incident; observed final `ASSIGNED`, four timeline entries, healthy service summary, zero outbox backlog, and zero dead letters.
- `npm run build`: PASS on Vite 8.2.2; 15 modules transformed.
- `npm test`: PASS, two frontend unit tests.
- `npm audit`: zero known vulnerabilities after the final dependency install.
- Docker, k6, and Kubernetes execution: not run because the corresponding tools are unavailable locally. No benchmark result is claimed.
