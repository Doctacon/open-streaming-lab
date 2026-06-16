Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-schema-validation-dlq.md, .loom/specs/next-level-streaming-platform.md

# Schema Validation and DLQ Evidence

## What was observed

The JSON Schema validation and dead-letter topic implementation was added and validated.

Changed implementation surfaces:

- `schemas/web_analytics_v1.json` defines the local Web Analytics event contract.
- `src/streaming_tutorial/validation.py` loads schemas, validates events, builds DLQ envelopes, and supports deterministic demo corruption.
- `src/streaming_tutorial/producer.py` routes valid events to the main topic and invalid events to the configured DLQ topic.
- `src/streaming_tutorial/admin.py`, `src/streaming_tutorial/config.py`, and `Makefile` support default main+DLQ topic management.
- `src/streaming_tutorial/test_validation.py` and `src/streaming_tutorial/test_producer.py` cover validation and routing behavior.
- `README.md` documents topic creation, valid production, invalid/DLQ production, and DLQ inspection commands.
- `pyproject.toml`/`uv.lock` add the open-source `jsonschema` dependency.

## Procedure

Static validation commands:

```bash
uv lock --check
make test
```

Observed static validation output:

```text
Resolved 14 packages in 7ms
uv run pytest
collected 10 items
src/streaming_tutorial/test_producer.py ......                           [ 60%]
src/streaming_tutorial/test_validation.py ....                           [100%]
10 passed in 0.09s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Generated-event schema sample check:

```bash
TMP=$(mktemp)
node tools/mockingbird-jsonl.mjs --template "Web Analytics Starter Kit" --eps 100 --limit 50 > "$TMP"
wc -l "$TMP"
uv run python - "$TMP" <<'PY'
import json, sys
from pathlib import Path
from streaming_tutorial.validation import load_event_schema, validate_event
schema = load_event_schema('web_analytics_v1')
count = 0
for line in Path(sys.argv[1]).read_text().splitlines():
    if not line.strip(): continue
    count += 1
    event = json.loads(line)
    validate_event(event, schema)
print(f'validated {count} generated events')
PY
rm "$TMP"
```

Observed generated-event check output:

```text
50 /var/folders/.../tmp.IolCIlnJZZ
validated 50 generated events
```

Runtime smoke command, using unique smoke topics to avoid relying on or deleting existing topic data:

```bash
STAMP=$(date +%s)
TOPIC="mockingbird.events.schema-smoke-$STAMP"
DLQ="mockingbird.events.schema-smoke-$STAMP.dlq"
GROUP="schema-smoke-$STAMP"
uv run kafka-admin create-topics --topic "$TOPIC" --dlq-topic "$DLQ" --partitions 3 --dlq-partitions 1
uv run kafka-produce --topic "$TOPIC" --dlq-topic "$DLQ" --template "Web Analytics Starter Kit" --eps 5 --limit 2 --invalid-every 2
uv run kafka-consume --topic "$TOPIC" --group-id "$GROUP-main" --from-beginning --max-messages 1
uv run kafka-consume --topic "$DLQ" --group-id "$GROUP-dlq" --from-beginning --max-messages 1
```

Observed runtime smoke output excerpts:

```text
Created topic 'mockingbird.events.schema-smoke-1781567192' with 3 partition(s) and replication factor 1.
Created topic 'mockingbird.events.schema-smoke-1781567192.dlq' with 1 partition(s) and replication factor 1.
Using schema web_analytics_v1 (web_analytics v1); DLQ topic='mockingbird.events.schema-smoke-1781567192.dlq'
DLQ demo enabled: every 2 event(s) will be intentionally corrupted.
queued valid event 1: topic=mockingbird.events.schema-smoke-1781567192 ...
delivered topic=mockingbird.events.schema-smoke-1781567192 partition=0 offset=0 ...
queued invalid event 2: topic=mockingbird.events.schema-smoke-1781567192.dlq ... validation_error="'timestamp' is a required property at <root>"
delivered topic=mockingbird.events.schema-smoke-1781567192.dlq partition=0 offset=0 ...
Finished. seen=2 main=1 dlq=1 delivered=2 errors=0
```

Main topic consume excerpt:

```text
--- message 1 topic=mockingbird.events.schema-smoke-1781567192 partition=0 offset=0 ...
{
  "page_url": "https://example.com/about",
  "referrer": "https://facebook.com",
  "session_id": "342b0113-67bd-453f-b7ac-86827cc0123c",
  "timestamp": "2026-06-15T23:46:33.306Z",
  "user_id": "eb51b66d-66a4-484c-ada0-f3886206352f"
}
Consumed 1 message(s).
```

DLQ consume excerpt:

```text
--- message 1 topic=mockingbird.events.schema-smoke-1781567192.dlq partition=0 offset=0 ...
{
  "original_event": {
    "page_url": "https://example.com/contact",
    "referrer": "https://google.com",
    "session_id": "d9afc111-f6f3-47ca-b6ac-390defb518f3",
    "user_id": "bd04a9da-29d0-41a8-8f34-227fd2a30f16"
  },
  "rejected_at": "2026-06-15T23:46:33.510931Z",
  "schema_id": "web_analytics_v1",
  "schema_name": "web_analytics",
  "schema_version": "1",
  "target_topic": "mockingbird.events.schema-smoke-1781567192",
  "validation_error": "'timestamp' is a required property at <root>",
  "validation_path": "<root>"
}
Consumed 1 message(s).
```

Review posture:

- A bounded `reviewer` subagent review was attempted for the schema/DLQ diff and timed out after 300 seconds without returning usable findings.
- Parent self-review checked README command coherence, producer/admin routing, tests, smoke output, and scope boundaries. One documentation issue was fixed before evidence: the Stock Prices partition exercise now uses `--schema none`, and the README now includes copy/pasteable DLQ demo commands.

## What this supports or challenges

Supports all acceptance criteria in `.loom/tickets/done/2026-06-15-schema-validation-dlq.md`:

- `make test` passes.
- Admin tooling creates main+DLQ topics via `create-topics`.
- Runtime smoke proves valid event delivery to a main topic.
- Runtime smoke proves intentionally invalid event delivery to a DLQ topic.
- DLQ messages include original payload, validation error, target topic, schema id/name/version, validation path, and rejection timestamp.
- README includes copy/pasteable commands for topic creation, valid production, DLQ production, and DLQ consumption.

## Limits

- Smoke topics were unique validation topics rather than the default `mockingbird.events` and `mockingbird.events.dlq`, to avoid destructive cleanup or dependence on existing default topic data.
- No successful independent subagent review was obtained; the reviewer run timed out.
- The Web Analytics schema is intentionally narrow for the default Mockingbird template. Other templates require `--schema none` until future schemas are added.
