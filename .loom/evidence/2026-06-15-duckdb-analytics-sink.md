Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-duckdb-analytics-sink.md, .loom/specs/next-level-streaming-platform.md

# DuckDB Analytics Sink Evidence

## What was observed

A local DuckDB analytics sink was implemented and validated.

Changed implementation surfaces:

- `src/streaming_tutorial/duckdb_sink.py` adds `kafka-sink-duckdb`, which consumes derived pageview events and writes them to DuckDB.
- `src/streaming_tutorial/analytics.py` adds `duckdb-analytics`, which runs example summary/referrer SQL over the local database.
- `src/streaming_tutorial/test_duckdb_sink.py` covers idempotent inserts keyed by Kafka topic/partition/offset.
- `pyproject.toml`/`uv.lock` add the open-source `duckdb` Python package and console scripts.
- `Makefile` adds `sink-duckdb` and `analytics` targets.
- `README.md` documents the DuckDB chapter, example commands, query output expectations, and replay/idempotency behavior.
- `.gitignore` ignores generated `data/` databases.

## Procedure

Static validation:

```bash
uv lock
make test
```

Observed static validation output:

```text
Resolved 49 packages in 393ms
Added duckdb v1.5.3
uv run pytest
collected 15 items
src/streaming_tutorial/test_duckdb_sink.py .                             [  6%]
src/streaming_tutorial/test_processor.py ..                              [ 20%]
src/streaming_tutorial/test_producer.py ......                           [ 60%]
src/streaming_tutorial/test_registry.py ..                               [ 73%]
src/streaming_tutorial/test_validation.py ....                           [100%]
15 passed in 0.17s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Runtime smoke, using unique topics and a generated local DB under ignored `data/`:

```bash
STAMP=$(date +%s)
TOPIC="mockingbird.events.duckdb-smoke-$STAMP"
DLQ="mockingbird.events.duckdb-smoke-$STAMP.dlq"
OUT="mockingbird.pageviews.by_url.duckdb-smoke-$STAMP"
GROUP="duckdb-smoke-$STAMP"
DB="data/duckdb-smoke-$STAMP.duckdb"
uv run kafka-admin create-topics --topic "$TOPIC" --dlq-topic "$DLQ" --partitions 3 --dlq-partitions 1
uv run kafka-admin create-derived-topics --pageviews-topic "$OUT" --partitions 3
uv run kafka-produce --topic "$TOPIC" --dlq-topic "$DLQ" --template "Web Analytics Starter Kit" --eps 5 --limit 4
uv run kafka-process --input-topic "$TOPIC" --output-topic "$OUT" --group-id "$GROUP-processor" --from-beginning --max-outputs 4 --timeout 30
uv run kafka-sink-duckdb --topic "$OUT" --group-id "$GROUP-sink" --db-path "$DB" --from-beginning --max-messages 4
uv run duckdb-analytics --db-path "$DB" --query summary
uv run duckdb-analytics --db-path "$DB" --query referrers
```

Observed sink output excerpt:

```text
Sinking topic='mockingbird.pageviews.by_url.duckdb-smoke-1781568746' group_id='duckdb-smoke-1781568746-sink' db_path='data/duckdb-smoke-1781568746.duckdb' auto.offset.reset=earliest
sank pageview 1: topic=mockingbird.pageviews.by_url.duckdb-smoke-1781568746 partition=2 offset=0 page_url='https://example.com/products'
sank pageview 2: topic=mockingbird.pageviews.by_url.duckdb-smoke-1781568746 partition=2 offset=1 page_url='https://example.com/about'
sank pageview 3: topic=mockingbird.pageviews.by_url.duckdb-smoke-1781568746 partition=1 offset=0 page_url='https://example.com/contact'
sank pageview 4: topic=mockingbird.pageviews.by_url.duckdb-smoke-1781568746 partition=1 offset=1 page_url='https://example.com/contact'
Consumed 4 message(s). Rows before=0 after=4 inserted_or_existing_delta=4.
```

Observed summary query output excerpt:

```text
┌─────────────────────────────┬───────────┬──────────┬───────┬────────────────────────────┬────────────────────────────┐
│          page_url           │ pageviews │ sessions │ users │         first_seen         │         last_seen          │
├─────────────────────────────┼───────────┼──────────┼───────┼────────────────────────────┼────────────────────────────┤
│ https://example.com/contact │         2 │        2 │     2 │ 2026-06-15 17:12:28.559-07 │ 2026-06-15 17:12:28.962-07 │
│ https://example.com/about   │         1 │        1 │     1 │ 2026-06-15 17:12:28.356-07 │ 2026-06-15 17:12:28.356-07 │
│ https://example.com/product │         1 │        1 │     1 │ 2026-06-15 17:12:28.761-07 │ 2026-06-15 17:12:28.761-07 │
│ s                           │           │          │       │                            │                            │
└─────────────────────────────┴───────────┴──────────┴───────┴────────────────────────────┴────────────────────────────┘
```

Observed referrer query output excerpt:

```text
┌──────────────────────┬───────────┐
│       referrer       │ pageviews │
├──────────────────────┼───────────┤
│ direct               │         2 │
│ https://facebook.com │         2 │
└──────────────────────┴───────────┘
```

Review posture:

- A bounded `reviewer` subagent review was attempted for the DuckDB diff and timed out after 180 seconds without returning usable findings.
- Parent self-review checked idempotent insert behavior, generated-data ignore rules, README replay/idempotency explanation, Makefile targets, static tests, and runtime query output.

## What this supports or challenges

Supports all acceptance criteria in `.loom/tickets/done/2026-06-15-duckdb-analytics-sink.md`:

- `make test` passes.
- Smoke test writes 4 Kafka-derived pageview events into DuckDB.
- Example SQL returns non-empty page URL and referrer analytics.
- README explains offset/replay/idempotency behavior.
- Commands, row counts, query outputs, and limits are recorded here.

## Limits

- The sink currently targets the derived pageviews topic, not raw source events. This was intentional because the derived topic is better suited for analytics.
- The DuckDB database is a local generated artifact under `data/` and is ignored by git.
- The summary table display wraps long URLs in the terminal output.
- No successful independent reviewer findings were obtained because the reviewer run timed out.
