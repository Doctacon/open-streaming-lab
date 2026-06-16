Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-apicurio-schema-registry-chapter.md, .loom/specs/next-level-streaming-platform.md

# Apicurio Schema Registry Chapter Evidence

## What was observed

The optional Apicurio Registry chapter was implemented and validated.

Changed implementation surfaces:

- `compose.yaml` adds `apicurio-registry` using `apicurio/apicurio-registry:3.3.0` exposed on host port `8081`.
- `src/streaming_tutorial/registry.py` adds a `schema-registry` CLI for registering and fetching JSON Schema artifacts through Apicurio Registry Core API v3.
- `src/streaming_tutorial/producer.py` adds `--schema-source registry` plus registry URL/group/version flags, while preserving local schema files and `--schema none` fallback.
- `src/streaming_tutorial/config.py`, `Makefile`, and `pyproject.toml` add registry defaults and commands.
- `src/streaming_tutorial/test_registry.py` covers URL construction and Apicurio JSON artifact request shape.
- `README.md` documents registering/fetching schemas and producing with a registry-fetched schema.

## Procedure

Source facts used:

- Apicurio Registry Docker docs state the container image can run locally and that default embedded H2/dev storage is suitable for development/testing only.
- Apicurio Registry REST API docs show Core API v3 artifact creation through `POST /apis/registry/v3/groups/{groupId}/artifacts` and content retrieval through `GET /apis/registry/v3/groups/{groupId}/artifacts/{artifactId}/versions/{version}/content`.

Static validation:

```bash
uv lock
make test
```

Observed static validation output:

```text
Resolved 14 packages in 10ms
uv run pytest
collected 12 items
src/streaming_tutorial/test_producer.py ......                           [ 50%]
src/streaming_tutorial/test_registry.py ..                               [ 66%]
src/streaming_tutorial/test_validation.py ....                           [100%]
12 passed in 0.15s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Registry startup/reachability:

```bash
docker-compose config >/dev/null
docker-compose up -d apicurio-registry
curl -fsS http://127.0.0.1:8081/apis/registry/v3/system/info
```

Observed registry info:

```json
{"name":"Apicurio Registry (SQL)","description":"High performance, runtime registry for schemas and API designs.","version":"3.3.0","builtOn":"2026-06-08T17:52:38Z"}
```

Schema registration and fetch:

```bash
uv run schema-registry register >/tmp/osl-registry-register.json
uv run schema-registry fetch --validate-schema > /tmp/osl-registry-fetch.json
```

Observed registration excerpt:

```json
{
  "artifact": {
    "artifactId": "web_analytics_v1",
    "artifactType": "JSON",
    "groupId": "open-streaming-lab"
  },
  "version": {
    "artifactId": "web_analytics_v1",
    "artifactType": "JSON",
    "globalId": 1,
    "groupId": "open-streaming-lab",
    "state": "ENABLED",
    "version": "1"
  }
}
```

Observed fetch excerpt showed the same Web Analytics schema content with `$schema`, `$id`, required fields, `x-schema-name`, and `x-schema-version`.

Registry-backed producer runtime smoke, using unique topics to avoid destructive cleanup:

```bash
STAMP=$(date +%s)
TOPIC="mockingbird.events.registry-smoke-$STAMP"
DLQ="mockingbird.events.registry-smoke-$STAMP.dlq"
GROUP="registry-smoke-$STAMP"
uv run kafka-admin create-topics --topic "$TOPIC" --dlq-topic "$DLQ" --partitions 3 --dlq-partitions 1
uv run kafka-produce --topic "$TOPIC" --dlq-topic "$DLQ" --schema-source registry --registry-url http://127.0.0.1:8081 --registry-group open-streaming-lab --registry-version 1 --limit 2 --invalid-every 2 --eps 5
uv run kafka-consume --topic "$TOPIC" --group-id "$GROUP-main" --from-beginning --max-messages 1
uv run kafka-consume --topic "$DLQ" --group-id "$GROUP-dlq" --from-beginning --max-messages 1
```

Observed producer excerpt:

```text
Created topic 'mockingbird.events.registry-smoke-1781567904' with 3 partition(s) and replication factor 1.
Created topic 'mockingbird.events.registry-smoke-1781567904.dlq' with 1 partition(s) and replication factor 1.
Using registry schema web_analytics_v1 (web_analytics v1); DLQ topic='mockingbird.events.registry-smoke-1781567904.dlq'
queued valid event 1: topic=mockingbird.events.registry-smoke-1781567904 ...
delivered topic=mockingbird.events.registry-smoke-1781567904 partition=0 offset=0 ...
queued invalid event 2: topic=mockingbird.events.registry-smoke-1781567904.dlq ... validation_error="'timestamp' is a required property at <root>"
delivered topic=mockingbird.events.registry-smoke-1781567904.dlq partition=0 offset=0 ...
Finished. seen=2 main=1 dlq=1 delivered=2 errors=0
```

Observed DLQ consume excerpt retained expected envelope fields:

```text
"schema_id": "web_analytics_v1",
"schema_name": "web_analytics",
"schema_version": "1",
"target_topic": "mockingbird.events.registry-smoke-1781567904",
"validation_error": "'timestamp' is a required property at <root>",
"validation_path": "<root>"
```

Review:

```text
reviewer: pass
```

## What this supports or challenges

Supports all acceptance criteria in `.loom/tickets/done/2026-06-15-apicurio-schema-registry-chapter.md`:

- `docker-compose config` succeeds with the registry service included.
- Registry API is reachable locally at `http://127.0.0.1:8081`.
- The Web Analytics JSON Schema can be registered and fetched.
- Producer validation can use local file schemas or registry-fetched schemas.
- README explains local file validation versus registry-backed contracts and identifies Apicurio as the open-source schema registry choice.
- Reviewer returned `pass` for the Apicurio diff.

## Limits

- The tutorial uses Apicurio's development embedded SQL/H2 storage; schema artifacts should be re-registered after container recreation.
- Registry validation was performed through the Core API v3 helper, not through Confluent Schema Registry compatibility APIs.
- No authentication is configured because this is a local learner stack.
