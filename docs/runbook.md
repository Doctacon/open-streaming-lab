# Open Streaming Lab Runbook

This runbook covers common local failures and the fastest checks to diagnose them.

## 1. Docker or Kafka is unavailable

Symptoms:

- `docker-compose ps` cannot connect to Docker.
- Producer/consumer errors mention broker connection failures.
- Kafbat UI is unavailable.

Checks:

```bash
colima ls
docker context ls
docker-compose ps
docker-compose logs kafka --tail=80
```

Fixes:

```bash
colima start
docker context use colima
make up
```

If Docker still references Docker Desktop's missing credential helper, remove stale `"credsStore": "desktop"` from `~/.docker/config.json` after backing it up.

## 2. Topic is missing

Symptoms:

- Producer delivery errors mention `UNKNOWN_TOPIC_OR_PART`.
- Consumer waits forever with no messages.
- Kafbat UI does not list expected topics.

Fix:

```bash
make topics
make derived-topic
make describe
make describe-dlq
make describe-derived
```

Why this happens: auto topic creation is disabled intentionally so the topic lifecycle is explicit.

## 3. Schema validation fails unexpectedly

Symptoms:

- Valid-looking records go to `mockingbird.events.dlq`.
- Producer prints `validation_error=...`.

Checks:

```bash
uv run kafka-consume --topic mockingbird.events.dlq --group-id dlq-debug --from-beginning --max-messages 5
uv run schema-registry fetch --validate-schema
```

Fixes:

- For the default tutorial path, keep `--template "Web Analytics Starter Kit"`.
- For other Mockingbird templates, disable the Web Analytics schema until a matching schema exists:

```bash
uv run kafka-produce --template "Stock Prices" --schema none --limit 10
```

## 4. Apicurio Registry is empty after restart

Symptoms:

- `--schema-source registry` cannot fetch `web_analytics_v1`.
- `schema-registry fetch` returns an HTTP error.

Fix:

```bash
make registry-register
make registry-fetch
```

Why this happens: the lab uses Apicurio's development embedded storage; artifacts are not treated as durable production state.

## 5. Processor produces no derived records

Symptoms:

- `kafka-process` starts but does not print `derived pageview` messages.
- `mockingbird.pageviews.by_url` is empty.

Checks:

```bash
make describe
make describe-derived
uv run kafka-consume --topic mockingbird.events --group-id processor-debug --from-beginning --max-messages 5
```

Fixes:

- Produce source events first:

```bash
uv run kafka-produce --limit 5 --eps 5
```

- Use a fresh processor group with `--from-beginning` to replay existing source events:

```bash
uv run kafka-process --group-id pageviews-debug-$(date +%s) --from-beginning --max-outputs 5
```

## 6. DuckDB sink duplicates or misses rows

The sink table uses Kafka `topic + partition + offset` as its primary key and inserts with `INSERT OR IGNORE`.

Expected behavior:

- Same sink group: Kafka resumes from committed offsets, so old messages are not re-read.
- New sink group with `--from-beginning`: Kafka replays old messages, but previously inserted coordinates are ignored.

Checks:

```bash
uv run kafka-sink-duckdb --from-beginning --max-messages 5
uv run duckdb-analytics --query summary
```

If you want a clean analytics database:

```bash
rm -f data/open_streaming_lab.duckdb
uv run kafka-sink-duckdb --from-beginning --max-messages 20
```

## 7. Local generated files appear in git status

Generated files should stay out of commits:

- `node_modules/`
- `.venv/`
- `state/`
- `data/`
- `.pytest_cache/`

Check:

```bash
git status --short
```

If a generated file is not ignored, add a targeted ignore rule rather than deleting source files.
