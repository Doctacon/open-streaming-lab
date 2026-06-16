# Open Streaming Lab

Open Streaming Lab is a local, open-source Kafka learning sandbox and portfolio demo for job-search practice. It uses:

- **Apache Kafka** in KRaft mode, running locally with Docker.
- **Kafbat UI** to inspect topics, messages, partitions, and consumer groups.
- **Apicurio Registry** as an optional open-source schema registry.
- **Tinybird Mockingbird** as the fake streaming data source.
- **Python** producer/consumer scripts using the open-source `confluent-kafka` client.

No managed Kafka, Confluent Cloud, Tinybird account, cloud credentials, or SaaS services are required.

Portfolio docs:

- [`docs/architecture.md`](docs/architecture.md) — architecture diagram, services, CLI responsibilities, and open-source posture.
- [`docs/runbook.md`](docs/runbook.md) — operational troubleshooting for Kafka, schemas, DLQ, processing, and DuckDB.
- [`docs/interview-guide.md`](docs/interview-guide.md) — 30-second pitch, interview talking points, and demo script.

## What you will learn

By the end of the first pass you should be able to explain and demo:

- What a Kafka **topic** is.
- What a **producer** does.
- What a **consumer** does.
- How JSON Schema validation protects a stream.
- How a **dead-letter topic** keeps invalid events inspectable.
- How a Python **stream processor** derives new Kafka topics from validated events.
- How a local **DuckDB sink** persists Kafka output for SQL analytics.
- Why **consumer groups** matter.
- How **offsets** let consumers resume work.
- How **partitions** spread events and preserve per-key ordering.
- Why Kafka listener configuration matters in Docker.

## Prerequisites

Install these first:

- Docker plus either `docker-compose` or the `docker compose` plugin.
- Python 3.11+.
- [`uv`](https://github.com/astral-sh/uv) for Python dependency management.
- Node.js 18+ and npm, because Mockingbird is a TypeScript/Node package.

This machine currently has the standalone `docker-compose` command. If your machine uses the Docker plugin, replace `docker-compose` with `docker compose` in the commands below.

## Why is there a tiny Node helper in a Python tutorial?

Mockingbird is the data generator you found, but its built-in Kafka destination targets Confluent Cloud's REST API, not a plain local Apache Kafka broker. To keep the tutorial local and open-source:

1. `tools/mockingbird-jsonl.mjs` uses `@tinybirdco/mockingbird` to generate fake events as JSON Lines.
2. `src/streaming_tutorial/producer.py` starts that helper and publishes each JSON event to local Kafka with Python.

So the Kafka work is Python, while Mockingbird remains the source of the stream.

## 1. Install dependencies

```bash
uv sync --extra dev
npm install
```

You can also use:

```bash
make install
```

## 2. Start local Kafka

```bash
docker-compose up -d
```

Or with the Makefile:

```bash
make up
```

Check containers:

```bash
docker-compose ps
```

Kafka is exposed to your host at `127.0.0.1:9092`. Kafbat UI is exposed at <http://localhost:8080>. Apicurio Registry is exposed at <http://localhost:8081>. The Kafka examples use `127.0.0.1` instead of `localhost` to avoid noisy IPv6 `::1` connection attempts on some macOS/Colima setups.

## 3. Create the tutorial topics

Auto topic creation is disabled on purpose so you learn the topic lifecycle explicitly. Open Streaming Lab starts with two source-path topics and later adds a derived topic:

- `mockingbird.events` — valid Web Analytics events.
- `mockingbird.events.dlq` — invalid events wrapped in a diagnostic dead-letter envelope.
- `mockingbird.pageviews.by_url` — derived pageview events produced by the stream processor.

```bash
uv run kafka-admin create-topics \
  --topic mockingbird.events \
  --dlq-topic mockingbird.events.dlq \
  --partitions 3 \
  --dlq-partitions 1
uv run kafka-admin describe --topic mockingbird.events
uv run kafka-admin describe --topic mockingbird.events.dlq
```

Or:

```bash
make topics
make describe
make describe-dlq
```

Create the derived processor topic when you reach the stream-processing chapter:

```bash
uv run kafka-admin create-derived-topics --pageviews-topic mockingbird.pageviews.by_url
# or
make derived-topic
```

Expected idea: the main topic exists with 3 partitions, the DLQ exists with 1 partition, derived topics exist when needed, and all use replication factor 1 because this is a single local broker.

## 4. Start a consumer

Open terminal 1:

```bash
uv run kafka-consume --topic mockingbird.events --from-beginning
```

Leave it running. It will wait for events.

Useful flags:

```bash
uv run kafka-consume --help
uv run kafka-consume --group-id another-learning-group --from-beginning
uv run kafka-consume --max-messages 10 --from-beginning
```

## 5. Produce Mockingbird events

Open terminal 2:

```bash
uv run kafka-produce \
  --topic mockingbird.events \
  --template "Web Analytics Starter Kit" \
  --eps 2 \
  --limit 20
```

The producer validates each generated event against `schemas/web_analytics_v1.json` before sending it to Kafka. You should see delivery reports from the producer and JSON events in the consumer terminal.

### Produce invalid events into the DLQ

To demo failure handling, intentionally corrupt every second generated event by removing the required `timestamp` field before validation:

```bash
uv run kafka-produce \
  --topic mockingbird.events \
  --dlq-topic mockingbird.events.dlq \
  --template "Web Analytics Starter Kit" \
  --eps 2 \
  --limit 4 \
  --invalid-every 2
```

Then inspect the dead-letter topic:

```bash
uv run kafka-consume \
  --topic mockingbird.events.dlq \
  --group-id dlq-demo \
  --from-beginning \
  --max-messages 2
```

Expected idea: valid events still go to `mockingbird.events`, while invalid events go to `mockingbird.events.dlq` with `target_topic`, `schema_id`, `schema_name`, `schema_version`, `validation_error`, `validation_path`, `rejected_at`, and `original_event` fields.

### Optional: use Apicurio Registry instead of the local schema file

The default producer loads `schemas/web_analytics_v1.json` directly from disk. That is the simplest way to learn validation. A schema registry adds a shared API for publishing and fetching contracts so producers and consumers do not each carry private schema copies.

Open Streaming Lab uses **Apicurio Registry**, an open-source alternative to Confluent Schema Registry, on <http://localhost:8081>.

Register the local schema:

```bash
uv run schema-registry register
# or
make registry-register
```

Fetch it back from the registry:

```bash
uv run schema-registry fetch --validate-schema
# or
make registry-fetch
```

Produce using the registry-fetched schema instead of the local file:

```bash
uv run kafka-produce \
  --topic mockingbird.events \
  --dlq-topic mockingbird.events.dlq \
  --schema-source registry \
  --registry-url http://127.0.0.1:8081 \
  --registry-group open-streaming-lab \
  --registry-version 1 \
  --limit 5
```

The runtime validation behavior is the same: valid records go to the main topic, and invalid records go to the DLQ. The difference is where the producer reads the schema contract from.

## 6. Process valid events into a derived topic

The processor uses the open-source `quixstreams` Python library to read validated Web Analytics events from Kafka and write traceable derived events to `mockingbird.pageviews.by_url`.

Create the derived topic:

```bash
make derived-topic
```

Produce a small source batch:

```bash
uv run kafka-produce --limit 5 --eps 5
```

Run the processor until it emits 5 derived records:

```bash
uv run kafka-process \
  --input-topic mockingbird.events \
  --output-topic mockingbird.pageviews.by_url \
  --group-id pageviews-demo \
  --from-beginning \
  --max-outputs 5
```

Inspect the derived topic:

```bash
uv run kafka-consume \
  --topic mockingbird.pageviews.by_url \
  --group-id derived-demo \
  --from-beginning \
  --max-messages 5
```

Expected idea: each derived message keeps the original `page_url`, `session_id`, `user_id`, `source_timestamp`, and `referrer`, adds `event_type=pageview_by_url`, `pageview_count=1`, and `processed_at`, and uses `page_url` as the output key so same-page records have partition affinity.

## 7. Sink derived events into DuckDB

The DuckDB sink persists derived pageview events into `data/open_streaming_lab.duckdb` for local SQL analysis. The `data/` directory is ignored by git because it contains generated local data.

Run the sink against the derived topic:

```bash
uv run kafka-sink-duckdb \
  --topic mockingbird.pageviews.by_url \
  --group-id duckdb-demo \
  --db-path data/open_streaming_lab.duckdb \
  --from-beginning \
  --max-messages 5
```

Run an example analytics query:

```bash
uv run duckdb-analytics --db-path data/open_streaming_lab.duckdb --query summary
# or
make analytics
```

Expected idea: DuckDB shows pageviews, distinct sessions, distinct users, and first/last source timestamps grouped by `page_url`.

Replay/idempotency note: the sink uses Kafka `topic + partition + offset` as the table primary key and inserts with `INSERT OR IGNORE`. If you rerun the same sink group, Kafka resumes from committed offsets. If you use a new group with `--from-beginning`, Kafka replays the topic, but already-ingested Kafka coordinates are ignored instead of duplicated.

Try another Mockingbird template only after disabling this chapter's Web Analytics schema:

```bash
uv run kafka-produce --template "Stock Prices" --schema none --eps 2 --limit 20
```

List available Mockingbird templates:

```bash
npm run mockingbird:templates
```

Preview raw Mockingbird JSONL without Kafka:

```bash
npm run mockingbird:sample
node tools/mockingbird-jsonl.mjs --template "Stock Prices" --limit 3
```

## 8. Inspect Kafka in the UI

Open <http://localhost:8080> and inspect:

- **Topics** → `mockingbird.events`, `mockingbird.events.dlq`, and `mockingbird.pageviews.by_url` → messages and partitions.
- **Consumers** → your consumer group, usually `mockingbird-python-learners`.
- Partition and offset values as messages are consumed.

## Exercises

### Exercise A: Consumer groups

Start two consumers with the same group id in two terminals:

```bash
uv run kafka-consume --group-id same-group
uv run kafka-consume --group-id same-group
```

Then produce events:

```bash
uv run kafka-produce --limit 30 --eps 5
```

What to notice: Kafka splits partitions across consumers in the same group. Each message is processed by one member of that group.

Now start consumers with different group ids:

```bash
uv run kafka-consume --group-id group-a --from-beginning
uv run kafka-consume --group-id group-b --from-beginning
```

What to notice: different groups each get their own read position.

### Exercise B: Offsets

Run a short consumer:

```bash
uv run kafka-consume --group-id offsets-demo --from-beginning --max-messages 5
```

Run it again with the same group id:

```bash
uv run kafka-consume --group-id offsets-demo --from-beginning --max-messages 5
```

What to notice: once a consumer group commits offsets, Kafka resumes from the committed position instead of replaying everything.

### Exercise C: Partitions and keys

The Python producer chooses a Kafka message key from fields like `user_id`, `session_id`, or `stock_symbol` when they exist. Kafka uses the key to choose a partition.

Stock prices have a small set of `stock_symbol` values, so this is a good template for seeing key/partition affinity:

```bash
uv run kafka-produce --template "Stock Prices" --schema none --limit 50 --eps 10
```

In the producer delivery logs and UI, check whether the same stock symbol tends to land on the same partition.

## Common commands

```bash
# Start stack
make up

# Create main + DLQ topics
make topics

# Produce 20 valid events at 2 events/sec
make produce

# Produce 4 events, corrupting every 2nd event into the DLQ
make produce-invalid

# Register/fetch the Web Analytics schema in Apicurio Registry
make registry-register
make registry-fetch

# Produce using the registry-fetched schema
make produce-registry

# Create and run the derived pageviews processor
make derived-topic
uv run kafka-process --from-beginning --max-outputs 5

# Sink derived pageviews to DuckDB and run SQL analytics
uv run kafka-sink-duckdb --from-beginning --max-messages 5
make analytics

# Consume from the beginning for the default group
make consume

# Consume DLQ messages
make consume-dlq

# Consume derived pageview messages
make consume-derived

# Run local checks
make test

# Stop containers and remove the local Kafka container data
make down

# Stop containers and remove any project volumes too
make reset
```

## Troubleshooting

### `docker: unknown command: docker compose`

Use `docker-compose` instead of `docker compose`, or install Docker's Compose plugin.

### Producer says Kafka is unavailable

Make sure Kafka is running and healthy:

```bash
docker-compose ps
docker-compose logs kafka --tail=80
```

Then recreate the topics:

```bash
uv run kafka-admin create-topics
```

### `error getting credentials ... docker-credential-desktop`

This usually means `~/.docker/config.json` still points Docker at Docker Desktop's credential helper even though you are using Colima. If you do not use Docker Desktop, back up that file and remove the `"credsStore": "desktop"` line:

```bash
cp ~/.docker/config.json ~/.docker/config.json.bak
python3 - <<'PY'
import json
from pathlib import Path
path = Path.home() / ".docker" / "config.json"
config = json.loads(path.read_text())
config.pop("credsStore", None)
config["currentContext"] = "colima"
path.write_text(json.dumps(config, indent=2) + "\n")
PY
```

### Colima says running, but Docker cannot connect

Check that Docker is using the same Colima context/profile that is actually running:

```bash
colima ls
docker context ls
docker context use colima
docker info
```

If `colima ls` says the default profile is stopped, start it:

```bash
colima start
```

### Producer says Mockingbird failed

Install Node dependencies:

```bash
npm install
npm run mockingbird:sample
```

### Apicurio Registry is unavailable

Make sure the Compose stack is running and the registry port is exposed:

```bash
docker-compose ps
open http://localhost:8081
```

The tutorial uses Apicurio's development storage. If you recreate the container, register the schema again:

```bash
make registry-register
```

### UI loads but no topic appears

Create the topic first, then refresh the UI:

```bash
uv run kafka-admin create-topic
```

### I want a clean slate

The tutorial keeps Kafka data only inside the local container. Removing the Compose stack clears messages and topics:

```bash
docker-compose down -v
```

## Next-level roadmap

This baseline is the foundation for a local, open-source streaming portfolio project. Planned next chapters are tracked in `.loom/tickets/2026-06-15-next-level-streaming-platform.md`:

1. Add windowed aggregations in Quix Streams.
2. Add Prometheus/Grafana after introducing long-running load tests.
3. Add ClickHouse as an optional OLAP service chapter.
