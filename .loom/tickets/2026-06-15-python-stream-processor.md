Status: open
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/2026-06-15-schema-validation-dlq.md

# Add Python Stream Processor and Derived Topics

## Scope

Add a Python stream-processing application that consumes valid Mockingbird events and writes derived Kafka topics.

Recommended implementation path:

- Use Quix Streams if it remains viable during implementation research.
- Start with a simple deterministic derived stream, then optionally add event-time/windowed aggregation.

In scope:

- Add a processor entrypoint such as `kafka-process`.
- Consume from `mockingbird.events` with its own consumer group.
- Produce at least one derived topic, for example:
  - `mockingbird.pageviews.by_url`, or
  - `mockingbird.sessions.activity`.
- Add admin topic creation for derived topics.
- Include unit tests for transformation logic independent of Kafka.
- Include README chapter explaining stream processing, keys, repartitioning concerns, and derived topics.

Out of scope:

- Apache Flink.
- Exactly-once semantics.
- Distributed deployment.
- DuckDB persistence, except as a downstream mention.

## Acceptance criteria

- `make test` passes.
- Runtime smoke test demonstrates: produce source events → run processor → consume derived events.
- Derived messages preserve enough metadata to trace back to source concepts, such as URL/session/user/time.
- README has copy/pasteable commands and expected outputs.
- Evidence records validation outputs and any limitations.

## Progress and notes

- 2026-06-15: Ticket opened as processing milestone after validation/DLQ.

## Blockers

- Requires schema validation/DLQ ticket so the processor can assume source events are contract-checked.
