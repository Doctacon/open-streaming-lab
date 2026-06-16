Status: active
Created: 2026-06-15
Updated: 2026-06-15

# Spec: Next-Level Local Streaming Platform

## Purpose and scope

This spec defines the intended behavior for evolving the current local Kafka tutorial into a portfolio-quality streaming data project.

In scope:

- Local Apache Kafka pipeline using the existing `mockingbird.events` source stream.
- JSON Schema data contracts and validation.
- Dead-letter handling for invalid events.
- A later open-source schema registry chapter using Apicurio Registry.
- Python stream processing that emits derived topics.
- Local analytics persistence, initially DuckDB.
- Documentation that teaches both how to run the project and how to explain it in interviews.

Out of scope for this spec's initial phase:

- Managed Kafka, Confluent Cloud, Tinybird SaaS ingestion, or proprietary hosted dependencies.
- Multi-broker production Kafka.
- Kubernetes deployment.
- Enterprise security hardening.
- Full exactly-once guarantees.

## Target architecture

Initial next-level architecture:

```text
Mockingbird Node JSONL helper
        ↓
Python producer
        ↓ validates against local JSON Schema
        ├── valid events   → mockingbird.events
        └── invalid events → mockingbird.events.dlq

Python consumer / later stream processor
        ↓
Derived Kafka topics
        ↓
DuckDB analytics tables
```

Later schema-registry architecture:

```text
schemas/*.json
        ↓ register/read
Apicurio Registry
        ↓
Python producer/consumer validation path
```

## Behavior

### Schema validation

- The tutorial defines a versioned JSON Schema for at least the default Mockingbird `Web Analytics Starter Kit` event shape.
- The producer validates each generated event before publishing it as a normal event.
- Valid events are published to `mockingbird.events`.
- Invalid events are not silently dropped.
- Invalid events are published to `mockingbird.events.dlq` with enough envelope metadata to diagnose the failure.
- DLQ messages include at minimum:
  - original topic intended for the event,
  - validation error message,
  - schema name/version,
  - original event payload,
  - timestamp when rejected.

### Topic management

- Admin tooling can create both the main event topic and DLQ topic.
- README explains why auto topic creation is disabled and why DLQ topics are explicit.
- Topic names remain configurable but have sensible defaults.

### Testing and validation

- Unit tests cover valid event acceptance, invalid event rejection, key selection, and DLQ envelope construction.
- Static checks continue to pass through `make test`.
- Runtime smoke evidence should include producing valid events and at least one intentionally invalid event routed to the DLQ.

### Stream processing

- A later processor consumes from `mockingbird.events` and writes at least one derived topic.
- Derived topic examples may include pageview counts by URL, session activity events, or simple time-windowed aggregates.
- Processor behavior must be deterministic enough to test without relying on wall-clock sleeps except where intentionally teaching event-time behavior.

### Analytics sink

- A later sink writes processed or raw valid events into DuckDB.
- README includes example SQL queries that prove the sink is useful.
- The sink should be local and reproducible without cloud credentials.

### Documentation and portfolio polish

- README evolves into a chaptered tutorial.
- Each chapter includes:
  - what concept it teaches,
  - commands to run,
  - expected output,
  - where to inspect the result,
  - common failure modes.
- Final portfolio docs include an architecture diagram and interview talking points.

## Acceptance criteria

- The project can still be run locally with Docker, `uv`, npm, and no cloud credentials.
- The next-level chapters are independently understandable and do not depend on unstated chat context.
- Invalid data has a visible path through a DLQ rather than being ignored.
- The roadmap remains open-source-first and Python-centered.
- Evidence records capture validation outputs before tickets are closed.

## Constraints

- Prefer open-source/self-hosted tools.
- Keep Python application logic unless a specific tool requires a helper in another language.
- Keep setup approachable for a learner; defer heavyweight components until earlier concepts are demonstrated.
- Do not commit secrets or environment-specific credentials.

## Related records

- `.loom/research/2026-06-15-next-level-streaming-roadmap.md`
- `.loom/decisions/python-first-streaming-roadmap.md`
- `.loom/tickets/done/2026-06-15-next-level-streaming-platform.md`
