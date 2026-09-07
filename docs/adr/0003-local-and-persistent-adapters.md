# ADR 0003: Dependency-free local core before persistent adapters

Status: accepted with follow-up required

## Context

The repository needed a runnable, testable vertical slice before infrastructure-specific work. The authoring host initially had neither Go nor Docker.

## Decision

Define the event, store, and workflow semantics using dependency-free, concurrency-safe local adapters. Mirror the intended PostgreSQL schemas and Kafka topology in infrastructure files. Keep the adapter limitation visible rather than presenting provisioned containers as completed integration.

## Consequences

Core saga and concurrency behavior can be race-tested quickly. The current executable is not yet a distributed microservice deployment; persistent adapters and independently deployed service binaries are the highest-priority follow-up.

