Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-python-stream-processor.md, .loom/specs/next-level-streaming-platform.md

# Python Stream Processor Evidence

## What was observed

A Quix Streams processor was implemented and validated.

Changed implementation surfaces:

- `src/streaming_tutorial/processor.py` adds `kafka-process`, a Quix Streams app that reads valid Web Analytics events and writes derived `pageview_by_url` events.
- `src/streaming_tutorial/test_processor.py` covers deterministic transformation and output key behavior.
- `src/streaming_tutorial/admin.py`, `src/streaming_tutorial/config.py`, and `Makefile` add derived topic defaults and creation/consume/process commands.
- `pyproject.toml`/`uv.lock` add the open-source `quixstreams` dependency and `kafka-process` script.
- `.gitignore` ignores Quix local `state/` directories.
- `README.md` documents the stream-processing chapter with copy/pasteable commands.
- `.loom/research/2026-06-15-quix-streams-processor.md` records the Quix API facts used.

## Procedure

Static validation:

```bash
uv lock
make test
```

Observed static validation output:

```text
Resolved 48 packages in 1.08s
Added quixstreams v3.24.0
uv run pytest
collected 14 items
src/streaming_tutorial/test_processor.py ..                              [ 14%]
src/streaming_tutorial/test_producer.py ......                           [ 57%]
src/streaming_tutorial/test_registry.py ..                               [ 71%]
src/streaming_tutorial/test_validation.py ....                           [100%]
14 passed in 0.83s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Runtime smoke, using unique topics to avoid destructive cleanup:

```bash
STAMP=$(date +%s)
TOPIC="mockingbird.events.processor-smoke-$STAMP"
DLQ="mockingbird.events.processor-smoke-$STAMP.dlq"
OUT="mockingbird.pageviews.by_url.processor-smoke-$STAMP"
GROUP="processor-smoke-$STAMP"
uv run kafka-admin create-topics --topic "$TOPIC" --dlq-topic "$DLQ" --partitions 3 --dlq-partitions 1
uv run kafka-admin create-derived-topics --pageviews-topic "$OUT" --partitions 3
uv run kafka-produce --topic "$TOPIC" --dlq-topic "$DLQ" --template "Web Analytics Starter Kit" --eps 5 --limit 3
uv run kafka-process --input-topic "$TOPIC" --output-topic "$OUT" --group-id "$GROUP-processor" --from-beginning --max-outputs 3 --timeout 30
uv run kafka-consume --topic "$OUT" --group-id "$GROUP-derived" --from-beginning --max-messages 3
```

Observed runtime output excerpts:

```text
Created topic 'mockingbird.events.processor-smoke-1781568240' with 3 partition(s) and replication factor 1.
Created topic 'mockingbird.events.processor-smoke-1781568240.dlq' with 1 partition(s) and replication factor 1.
Created topic 'mockingbird.pageviews.by_url.processor-smoke-1781568240' with 3 partition(s) and replication factor 1.
Finished. seen=3 main=3 dlq=0 delivered=3 errors=0
Processing input_topic='mockingbird.events.processor-smoke-1781568240' output_topic='mockingbird.pageviews.by_url.processor-smoke-1781568240' group_id='processor-smoke-1781568240-processor' auto_offset_reset=earliest
[quixstreams] : APP STOP CONDITIONS SET: timeout=30.0 seconds OR count=3 records
derived pageview: {'event_type': 'pageview_by_url', 'page_url': 'https://example.com/contact', 'source_timestamp': '2026-06-16T00:04:02.404Z', 'session_id': '81daebdf-fd8e-4fb7-a49c-6d0fe2a2d654', 'user_id': '2eb2a94f-44ac-4997-ac74-135f949ac3b7', 'referrer': 'https://google.com', 'pageview_count': 1, 'processed_at': '2026-06-16T00:04:07.197196Z'}
[quixstreams] : Count of 3 records reached.
Processor stopped.
```

Derived topic consume excerpt:

```text
--- message 1 topic=mockingbird.pageviews.by_url.processor-smoke-1781568240 partition=2 offset=0 key='https://example.com/products'
{
  "event_type": "pageview_by_url",
  "page_url": "https://example.com/products",
  "pageview_count": 1,
  "processed_at": "2026-06-16T00:04:07.198276Z",
  "referrer": "direct",
  "session_id": "7dbb7274-b53a-4a6c-9616-154c586e1980",
  "source_timestamp": "2026-06-16T00:04:02.202Z",
  "user_id": "baaf8335-5bbc-4bcc-bb03-97b73cc54ef6"
}
```

Review posture:

- A bounded `reviewer` subagent review was attempted for the stream processor diff and timed out after 180 seconds without returning usable findings.
- Parent self-review checked ticket acceptance, source/destination topic commands, derived event traceability, Quix state directory handling, and README copy/paste commands.

## What this supports or challenges

Supports all acceptance criteria in `.loom/tickets/done/2026-06-15-python-stream-processor.md`:

- `make test` passes.
- Runtime smoke demonstrates produce source events → run processor → consume derived events.
- Derived messages preserve `page_url`, `session_id`, `user_id`, `source_timestamp`, and `referrer`, and add `event_type`, `pageview_count`, and `processed_at`.
- README has copy/pasteable stream-processing commands and expected output semantics.
- Validation outputs and limits are recorded here.

## Limits

- The first processor is a deterministic one-to-one transform, not a stateful/windowed aggregate. This was intentional to keep the first processing chapter teachable.
- Quix Streams created local state under `state/<consumer-group>` during runtime; `.gitignore` now ignores `state/`.
- Adding Quix Streams changed the locked `confluent-kafka` version from 2.14.2 to 2.11.1 due dependency resolution; tests and runtime smoke passed with the resolved version.
- No successful independent reviewer findings were obtained because the reviewer run timed out.
