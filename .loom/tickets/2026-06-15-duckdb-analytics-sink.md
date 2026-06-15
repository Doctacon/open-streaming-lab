Status: open
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/2026-06-15-python-stream-processor.md

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

## Progress and notes

- 2026-06-15: Ticket opened as first persistence milestone.

## Blockers

- Best implemented after at least one derived topic exists, so the sink can support a richer analytics story than raw JSON only.
