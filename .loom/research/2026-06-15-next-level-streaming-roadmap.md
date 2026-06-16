Status: done
Created: 2026-06-15
Updated: 2026-06-15

# Next-Level Streaming Tutorial Roadmap Research

## Question

How should the existing local Python + Apache Kafka + Mockingbird tutorial evolve into a more job-ready streaming data project while preserving the project's open-source-first constraints?

## Sources and methods

Prior local context:

- `.loom/research/20260613-local-kafka-mockingbird-tutorial.md` - baseline research for the local Kafka + Mockingbird scaffold.
- `.loom/tickets/done/20260613-python-kafka-mockingbird-tutorial.md` - current scaffold ticket, still in review pending audit/closure decision.
- `README.md` - already lists future chapters: schema evolution, Kafka Connect, stream processing, and persistence into PostgreSQL or DuckDB.

Fresh source scan on 2026-06-15:

- Apicurio Registry docs: Docker installation and Confluent Schema Registry compatibility API. Apicurio is open source and supports registry use cases without Confluent Cloud.
  - https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-installing-registry-docker.html
  - https://www.apicur.io/registry/docs/apicurio-registry/3.3.x/getting-started/assembly-confluent-schema-registry-compatibility.html
  - https://github.com/Apicurio/apicurio-registry
- Quix Streams docs and repository. Quix Streams is an open-source Python stream-processing framework for Kafka with event-time windows and RocksDB-backed state.
  - https://github.com/quixio/quix-streams
  - https://quix.io/docs/quix-streams/quickstart.html
  - https://quix.io/docs/quix-streams/windowing.html
  - https://www.quix.io/docs/quix-streams/advanced/stateful-processing.html
- DLQ and schema-validation patterns. General Kafka pattern: validate before processing/committing; route malformed or semantically invalid records to a dead-letter topic so the pipeline keeps moving and bad records remain inspectable.
  - https://www.confluent.io/learn/kafka-dead-letter-queue/
  - https://github.com/OpenDQV/OpenDQV/blob/main/docs/kafka_integration.md
- Analytics sink options. ClickHouse has strong Kafka ingestion patterns but adds a server and more operational concepts; DuckDB is simpler for a local learning path and aligns with existing project context.
  - https://github.com/ClickHouse/clickhouse-docs/blob/main/knowledgebase/kafka-to-clickhouse-setup.mdx
  - https://github.com/ClickHouse/clickhouse-docs/blob/main/docs/integrations/data-ingestion/kafka/kafka-clickhouse-connect-sink.md

## Findings

- The highest-value next learning jump is not another producer/consumer demo. It is adding data contracts and failure handling: JSON Schema validation plus a DLQ. This teaches production streaming concerns while keeping the local stack small.
- Apicurio Registry is a good open-source target for a later schema-registry chapter. However, the first schema step can be local file-based JSON Schema validation to avoid front-loading registry APIs before the learner understands what validation is protecting.
- Quix Streams fits the Python-first direction better than Kafka Streams, because Kafka Streams is JVM-first. It can teach keyed processing, windowing, state stores, and derived topics while keeping the codebase Python-centered.
- DuckDB is the right first analytics sink for this tutorial because it is embedded, local, open source, and already present in project-wide tech context. ClickHouse is useful later when the goal shifts to service-style OLAP and dashboard concurrency.
- Observability should be staged. The existing Kafbat UI already covers topic/consumer visibility. Prometheus/Grafana should come after validation, DLQ, and processing exist, otherwise dashboards will monitor a toy pipeline with little operational signal.
- The current baseline scaffold should be closed or explicitly accepted before large next-level work starts. Its Loom ticket is still in `review`, and the directory is not currently a git repository.

## Recommended roadmap

1. Foundation hygiene: close/accept the baseline tutorial state, initialize source control if desired, and document the new roadmap.
2. Schema validation + DLQ: add file-based JSON Schema contracts, validate generated events, create a DLQ topic, and make bad events observable and testable.
3. Schema registry chapter: add Apicurio Registry to Compose and register/read schemas, after local validation concepts are understood.
4. Stream processing: add a Python Quix Streams processor that consumes valid events, performs keyed/session or URL-level aggregation, and writes derived topics.
5. Analytics sink: persist processed events or aggregates into DuckDB for local SQL analysis.
6. Observability: add metrics/logging and optional Prometheus/Grafana once there are real signals: throughput, lag, validation failures, DLQ count, processor output count.
7. Portfolio polish: diagrams, screenshots, architecture explanation, failure-mode walkthroughs, and interview prompts.

## Conclusions

Proceed with an incremental Python-first architecture: JSON Schema + DLQ first, Apicurio second, Quix Streams third, DuckDB fourth, observability and portfolio polish last. This maximizes learning value while preserving the local/open-source-only stance.

## Related records

- `.loom/specs/next-level-streaming-platform.md`
- `.loom/decisions/python-first-streaming-roadmap.md`
- `.loom/tickets/done/2026-06-15-next-level-streaming-platform.md`
