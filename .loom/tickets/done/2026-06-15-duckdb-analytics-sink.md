Status: done
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/done/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/done/2026-06-15-python-stream-processor.md

# Add DuckDB Analytics Sink

## Scope

Persist raw or processed Kafka events into local DuckDB tables for SQL analysis.

In scope:

- Add a Python sink entrypoint such as `kafka-sink-duckdb`.
- Decide whether to sink raw valid events, derived processor output, or both.
- Create local DuckDB database/table files under a documented path, likely ignored by git.
- Add idempotency/replay guidance so rerunning consumers does not silently duplicate data without explanation.
- Add example SQL queries and a Makefile target to run them.
- Update README with an analytics chapter.

Out of scope:

- ClickHouse.
- Cloud object storage.
- Dashboard UI beyond simple query output.

## Acceptance criteria

- `make test` passes.
- A smoke test writes Kafka events into DuckDB.
- Example SQL returns non-empty, understandable analytics output.
- README explains offsets/replay/duplication implications for sinks.
- Evidence records the commands, row counts, and query outputs.

## Current State

Done. The `kafka-sink-duckdb` sink writes derived pageviews to local DuckDB, `duckdb-analytics` returns example SQL summaries, and static plus runtime evidence is recorded.

## Progress and notes

- 2026-06-15: Ticket opened as first persistence milestone.
- 2026-06-15: Set Status to `active` for autonomous Loom-driver execution. Chose to sink the derived `mockingbird.pageviews.by_url` topic first, because it provides a richer analytics story than raw source JSON while keeping replay/idempotency explainable through Kafka topic/partition/offset keys.
- 2026-06-15: Implemented DuckDB sink, analytics query CLI, Makefile/README support, idempotency tests, runtime smoke validation, and evidence in `.loom/evidence/2026-06-15-duckdb-analytics-sink.md`.

## Blockers

None. Independent reviewer subagent timed out; residual review risk is recorded in `.loom/evidence/2026-06-15-duckdb-analytics-sink.md`.
