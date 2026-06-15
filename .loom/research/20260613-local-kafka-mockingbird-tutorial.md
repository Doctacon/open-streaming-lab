# Local Apache Kafka + Mockingbird Tutorial Architecture

ID: research:20260613-local-kafka-mockingbird-tutorial
Type: Research
Status: completed
Created: 2026-06-13
Updated: 2026-06-13

## Summary

Investigated how to scaffold a local, open-source streaming-data tutorial using Apache Kafka and Tinybird Mockingbird. The recommended path is a Docker Compose Apache Kafka KRaft broker plus Kafbat UI, with a small TypeScript KafkaJS producer that uses Mockingbird as a library to generate fake events into a local Kafka topic and a TypeScript consumer that prints and explains consumed records.

## Question

Which local tutorial architecture best teaches practical Kafka streaming concepts using Mockingbird while respecting the project's open-source-first constraint and avoiding managed/proprietary dependencies?

## Scope

Covers a beginner-friendly local tutorial scaffold for this empty `streaming-tutorial` folder. Includes local Kafka runtime, fake streaming data generation, producer/consumer code, observability/UI, and learning sequence. Excludes managed Kafka services, Confluent Cloud, Tinybird SaaS ingestion, production-hardening, multi-broker Kafka, schema registry, Kafka Connect, Flink/Spark, and deployment.

Assumptions:

- The tutorial should run on a developer laptop with Docker and Node.js.
- The learner wants job-search-relevant Kafka basics: topics, producers, consumers, consumer groups, offsets, partitions, and observable message flow.
- Open-source-first means avoiding managed/proprietary Kafka services and using Apache Kafka locally.

## Method And Sources

- `https://github.com/tinybirdco/mockingbird` - Mockingbird README and repository structure show it is a FOSS TypeScript mock streaming data generator with CLI, UI, library, preset schemas, and destination plugins.
- `https://raw.githubusercontent.com/tinybirdco/mockingbird/main/packages/mockingbird/src/generators/BaseGenerator.ts` - Source inspection showed `BaseGenerator` can generate rows from a schema and delegates delivery through `sendData(rows)`, making it straightforward to subclass/wrap for KafkaJS.
- `https://raw.githubusercontent.com/tinybirdco/mockingbird/main/packages/mockingbird/src/generators/ConfluentCloudKafkaGenerator.ts` - Source inspection showed Mockingbird's built-in Kafka destination targets Confluent Cloud Kafka REST APIs, not a plain local Apache Kafka broker.
- `https://raw.githubusercontent.com/tinybirdco/mockingbird/main/apps/cli/subcommands.js` and `utils.js` - Source inspection showed the CLI has a `log` generator and Confluent Cloud Kafka command, but no local Apache Kafka broker command.
- `https://kafka.apache.org/quickstart/` - Official Apache Kafka quickstart documents Docker image usage, creating topics, producing, consuming, and the core event/topic model.
- `https://raw.githubusercontent.com/apache/kafka/trunk/docker/examples/README.md` and `docker-compose-files/single-node/plaintext/docker-compose.yml` - Official Apache Kafka Docker examples document single-node KRaft Compose configuration and host/container listener setup.
- `https://docs.docker.com/guides/kafka/` - Docker guide explains local Kafka with KRaft, host vs Docker advertised listeners, and using KafkaJS from Node.
- `https://kafka.js.org/docs/2.1.0/getting-started` and related KafkaJS docs - KafkaJS examples show Node producer/consumer setup, `send`, `subscribe`, and `eachMessage` usage.
- `https://github.com/kafbat/kafka-ui` and `https://ui.docs.kafbat.io/overview/getting-started` - Kafbat UI is an Apache-2.0 open-source Kafka UI suitable for local troubleshooting.

## Findings

- Apache Kafka now supports simpler local Docker setups without ZooKeeper through KRaft; official quickstart and Docker examples support running a single local broker.
- Kafka clients connecting from the host and from Docker containers need correct advertised listeners; a tutorial should make this explicit because it is a common beginner failure mode.
- Mockingbird is useful as a fake event generator, but its built-in Kafka destination is specifically `confluent-cloud-kafka` via Confluent's REST endpoint, which would introduce managed/proprietary service assumptions.
- Mockingbird's library API and `BaseGenerator` make local Kafka integration feasible by generating rows in-process and sending them with a normal Kafka client.
- KafkaJS is a lightweight open-source Node client with clear producer and consumer examples. It fits this repository's TypeScript/Node preference and the Mockingbird library's ecosystem.
- Kafbat UI is an Apache-2.0 Kafka UI that can show topics, messages, consumer groups, offsets, and partitions during the tutorial.

## Tradeoffs

- Mockingbird CLI `confluent-cloud-kafka` destination - Fast path if using Confluent Cloud, but violates the local/open-source-first learning goal and requires cloud credentials.
- Mockingbird CLI `log` piped into another producer - Keeps CLI usage visible but is brittle because the CLI logs arrays/batches rather than a clean local Kafka destination contract.
- Mockingbird as TypeScript library + KafkaJS producer - Slightly more code, but fully local/open-source, teaches real producer code, and remains close to the Mockingbird source model. Recommended.
- Apache Kafka official image vs third-party Kafka-compatible brokers - Official image is most resume-relevant and fully open-source; alternatives may be easier or faster but can obscure Apache Kafka-specific operations.
- Include schema registry immediately vs JSON first - Schema registry is job-relevant, but adding it on day one increases setup complexity. JSON first gives a reliable baseline; schema registry can be a later tutorial chapter using an open-source registry such as Apicurio if needed.

## Rejected Paths And Null Results

- Use Confluent Cloud Kafka as the primary destination - rejected because the user asked for local learning and the project policy prioritizes open-source/self-hosted tools over managed/proprietary services.
- Use Tinybird hosted ingestion as the main sink - rejected because it teaches ingestion into a SaaS analytics platform more than Kafka fundamentals.
- Start with multi-broker Kafka, Connect, and schema registry - rejected for the first scaffold because it front-loads operational complexity before the learner has producer/consumer/offset mental models.

## Conclusions

The most appropriate initial tutorial is an open-source-only local stack:

1. Docker Compose starts a single Apache Kafka KRaft broker with host and Docker listeners.
2. Docker Compose also starts Kafbat UI for visual inspection.
3. A TypeScript `producer` uses `@tinybirdco/mockingbird` preset schemas to generate JSON events and `kafkajs` to publish to a `mockingbird.events` topic.
4. A TypeScript `consumer` reads from the topic, logs partition/offset/key/value, and explains consumer-group behavior.
5. README lessons progress from manual Kafka CLI checks to Mockingbird-generated event streams and then to consumer groups/offsets/partitions.

Confidence is high for a beginner tutorial scaffold. Recheck trigger: if Mockingbird adds a native local Apache Kafka destination, the producer wrapper may be simplified.

## Recommendations

Use this research to create a bounded implementation ticket or lightweight plan for the tutorial scaffold after operator approval. The first implementation slice should create:

- `README.md` with prerequisites and step-by-step lessons.
- `compose.yaml` for Apache Kafka and Kafbat UI.
- `package.json`, `tsconfig.json`, and TypeScript scripts.
- `src/producer.ts`, `src/consumer.ts`, and shared config/schema helpers.
- npm scripts for `docker:up`, `topic:create`, `produce`, `consume`, `ui`, and cleanup.

## Open Questions

- Whether the operator prefers a TypeScript/Node tutorial, a Python tutorial, or a more data-engineering-oriented stack after the Kafka basics.
- Whether to add a second chapter for schema registry and typed schemas after the first local Kafka baseline works.

## Related Records

- `/Users/crlough/Code/personal/CLAUDE.md` - project-wide open-source-first and research-first architecture constraints.
