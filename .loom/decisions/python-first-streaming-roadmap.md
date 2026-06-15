Status: active
Created: 2026-06-15
Updated: 2026-06-15

# Decision: Evolve the Tutorial as a Python-First Local Streaming Platform

## Context

The current repository is a local Apache Kafka + Python + Mockingbird tutorial scaffold. The user asked how to take it to the next level, and accepted the recommendation to create a concrete implementation plan with tickets.

The project-wide rule is open-source-first: avoid managed Kafka, Confluent Cloud, Tinybird-hosted ingestion, proprietary SaaS, and vendor lock-in. The current tutorial already uses local Apache Kafka, Kafbat UI, Python `confluent-kafka`, and a small Node Mockingbird JSONL bridge.

The next step should improve job-search value without turning the tutorial into an overcomplicated production platform too early.

## Decision

Evolve the tutorial in this order:

1. Finish baseline hygiene and closure.
2. Add file-based JSON Schema validation and a Kafka dead-letter topic.
3. Add an Apicurio Registry chapter after validation concepts are clear.
4. Add a Python stream processor using Quix Streams.
5. Add DuckDB as the first local analytics sink.
6. Add observability and portfolio polish after the pipeline has meaningful operational signals.

Keep the project Python-first for Kafka application logic. Keep the existing Node helper only for Mockingbird event generation unless Mockingbird later provides a clean Python/local-Kafka path.

## Alternatives considered

- **Jump directly to Apicurio Registry.** Rejected for the next immediate slice because registry APIs add setup and compatibility concepts before the learner has seen concrete validation/DLQ behavior.
- **Use Confluent Schema Registry or Confluent Cloud.** Rejected because it conflicts with the open-source/self-hosted project rule.
- **Use Kafka Streams.** Deferred because Kafka Streams is JVM-first and would shift the tutorial away from its Python-centered learning goal.
- **Use Apache Flink immediately.** Deferred because Flink is highly job-relevant but operationally heavy for the next slice.
- **Use ClickHouse as the first sink.** Deferred. ClickHouse is open source and strong for real-time OLAP, but DuckDB is simpler for a local tutorial and aligns with existing project context.
- **Add Prometheus/Grafana first.** Deferred because the baseline currently lacks enough pipeline behavior to produce meaningful operational metrics beyond Kafka itself.

## Consequences

- The next implementation remains small enough to test locally but teaches production-grade concepts: contracts, failure routing, replayability, and inspectability.
- Later tickets can build naturally on the same topics and schemas rather than replacing the scaffold.
- The tutorial remains portfolio-friendly: each chapter demonstrates a distinct streaming skill.
- Some advanced resume keywords, especially Flink and ClickHouse, are intentionally delayed to avoid a brittle setup.

## Related records

- `.loom/research/2026-06-15-next-level-streaming-roadmap.md`
- `.loom/specs/next-level-streaming-platform.md`
- `.loom/tickets/2026-06-15-next-level-streaming-platform.md`
