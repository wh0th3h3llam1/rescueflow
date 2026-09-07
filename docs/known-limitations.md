# Known limitations

This is a portfolio prototype over synthetic data. It is not suitable for real emergency response.

- The verified runtime composes boundaries in one Go process with memory-backed ownership and broker adapters. Kafka/PostgreSQL/Redis infrastructure and schemas are present but runtime integration is deferred.
- Process restart loses local adapter state. Database recovery semantics are design-tested only at the unit boundary, not against a live PostgreSQL/Kafka stack.
- No gRPC endpoint is implemented yet; adding one inside the current process would demonstrate syntax, not justified distributed communication.
- Trace context is represented in envelopes and Tempo is provisioned, but OpenTelemetry spans/export are not wired.
- Loki is provisioned, but there is no log shipper. Application logs are structured JSON at startup; request/event logs need expansion.
- WebSocket delivery is best effort and one-way. Clients recover by refetching authoritative HTTP state.
- Kubernetes runs only stateless API/web workloads and expects external data services. Image names and the secret placeholder must be replaced.
- No load benchmark was run and no throughput or latency claim is made.

