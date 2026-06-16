# Open Streaming Lab Interview Guide

Use this guide to explain the project as a portfolio demo.

## 30-second pitch

Open Streaming Lab is a fully local streaming data pipeline. It uses Mockingbird to generate Web Analytics events, validates them with JSON Schema, sends valid records through Apache Kafka, routes invalid records to a DLQ, processes valid records with Quix Streams, persists derived pageviews into DuckDB, and exposes SQL summaries. It uses open-source tools only and requires no cloud credentials.

## Architecture talking points

- **Kafka topics:** `mockingbird.events` holds valid source events; `mockingbird.events.dlq` holds diagnostic envelopes for bad records; `mockingbird.pageviews.by_url` holds derived processor output.
- **Validation:** the producer validates before publishing, so downstream processors can assume the source topic contains contract-checked records.
- **DLQ:** bad data is not dropped. It is routed with original payload, schema metadata, validation error, and rejection timestamp.
- **Schema registry:** local file schemas are the simplest path; Apicurio Registry demonstrates how shared schema contracts work without Confluent Cloud.
- **Processing:** Quix Streams performs a deterministic one-to-one transform and keys derived output by `page_url` to demonstrate partition affinity.
- **Persistence:** DuckDB stores derived events for local analytics; idempotency is based on Kafka topic/partition/offset.
- **Observability:** Kafbat UI shows topics and consumer groups; CLI summaries expose counts; DuckDB queries prove sink results.

## Good questions to be ready for

### Why use a DLQ?

A streaming pipeline should not stop forever because one record is malformed. The DLQ keeps invalid records inspectable and replayable while allowing valid traffic to continue.

### Why validate in the producer?

This lab validates before publishing to the main topic so downstream processors can trust `mockingbird.events`. In larger systems, validation may also happen at ingestion gateways, consumers, or schema-registry serializers.

### Why key by `user_id` in the producer and `page_url` in the processor?

The source producer uses identity-like keys so records for the same user tend to land on the same partition. The derived pageview topic uses `page_url` because the downstream analytic question is page-centric.

### What delivery guarantee does this have?

The lab is best described as **at-least-once-oriented**. The DuckDB sink makes replay safe for already-seen Kafka coordinates by using `topic + partition + offset` as the idempotency key.

### Why DuckDB instead of ClickHouse?

DuckDB is embedded, fast, and easy for a local demo. ClickHouse is a good future step when the goal is service-style OLAP ingestion and concurrent dashboards.

### Why no managed services?

The project intentionally follows an open-source-first constraint: Apache Kafka, Kafbat UI, Apicurio Registry, Quix Streams, and DuckDB all run locally without vendor lock-in.

## Demo script

```bash
make up
make topics
make derived-topic
make registry-register
uv run kafka-produce --limit 5 --eps 5
uv run kafka-process --from-beginning --max-outputs 5
uv run kafka-sink-duckdb --from-beginning --max-messages 5
uv run duckdb-analytics --query summary
```

Then open:

- Kafka UI: <http://localhost:8080>
- Apicurio Registry: <http://localhost:8081>

## What to improve next

- Add windowed aggregations in Quix Streams.
- Add Prometheus/Grafana after introducing long-running load tests.
- Add ClickHouse as an optional OLAP service chapter.
- Add CI once this repository is pushed to a remote.
