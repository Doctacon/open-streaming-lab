# Open Streaming Lab

Open Streaming Lab is a local, open-source Kafka learning sandbox for job-search practice. It uses:

- **Apache Kafka** in KRaft mode, running locally with Docker.
- **Kafbat UI** to inspect topics, messages, partitions, and consumer groups.
- **Tinybird Mockingbird** as the fake streaming data source.
- **Python** producer/consumer scripts using the open-source `confluent-kafka` client.

No managed Kafka, Confluent Cloud, Tinybird account, cloud credentials, or SaaS services are required.

## What you will learn

By the end of the first pass you should be able to explain and demo:

- What a Kafka **topic** is.
- What a **producer** does.
- What a **consumer** does.
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

Kafka is exposed to your host at `127.0.0.1:9092`. Kafbat UI is exposed at <http://localhost:8080>. The Kafka examples use `127.0.0.1` instead of `localhost` to avoid noisy IPv6 `::1` connection attempts on some macOS/Colima setups.

## 3. Create the tutorial topic

Auto topic creation is disabled on purpose so you learn the topic lifecycle explicitly.

```bash
uv run kafka-admin create-topic --topic mockingbird.events --partitions 3
uv run kafka-admin describe --topic mockingbird.events
```

Or:

```bash
make topic
make describe
```

Expected idea: the topic exists with 3 partitions and replication factor 1 because this is a single local broker.

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

You should see delivery reports from the producer and JSON events in the consumer terminal.

Try another Mockingbird template:

```bash
uv run kafka-produce --template "Stock Prices" --eps 2 --limit 20
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

## 6. Inspect Kafka in the UI

Open <http://localhost:8080> and inspect:

- **Topics** → `mockingbird.events` → messages and partitions.
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
uv run kafka-produce --template "Stock Prices" --limit 50 --eps 10
```

In the producer delivery logs and UI, check whether the same stock symbol tends to land on the same partition.

## Common commands

```bash
# Start stack
make up

# Create topic
make topic

# Produce 20 events at 2 events/sec
make produce

# Consume from the beginning for the default group
make consume

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

Then recreate the topic:

```bash
uv run kafka-admin create-topic
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

1. Add JSON Schema validation and a `mockingbird.events.dlq` dead-letter topic.
2. Add Apicurio Registry as the open-source schema registry chapter.
3. Add a Python stream processor that writes derived Kafka topics.
4. Persist processed events into DuckDB for local analytics.
5. Add observability, diagrams, screenshots, and interview talking points.
