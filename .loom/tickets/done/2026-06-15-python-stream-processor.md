Status: done
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/done/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/done/2026-06-15-schema-validation-dlq.md

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

## Current State

Done. The `kafka-process` Quix Streams processor consumes valid Web Analytics events, writes traceable `pageview_by_url` events to a derived topic, and has static plus runtime smoke evidence.

## Progress and notes

- 2026-06-15: Ticket opened as processing milestone after validation/DLQ.
- 2026-06-15: Set Status to `active` for autonomous Loom-driver execution. Chose Quix Streams per the existing roadmap decision and supporting docs.
- 2026-06-15: Implemented `kafka-process`, derived topic admin/Makefile/README support, Quix dependency, unit tests, runtime smoke validation, and evidence in `.loom/evidence/2026-06-15-python-stream-processor.md`.

## Blockers

None. Independent reviewer subagent timed out; residual review risk is recorded in `.loom/evidence/2026-06-15-python-stream-processor.md`.
