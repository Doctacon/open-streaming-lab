Status: recorded
Created: 2026-06-15
Updated: 2026-06-15
Relates-To: .loom/tickets/done/2026-06-15-observability-and-portfolio-polish.md, .loom/specs/next-level-streaming-platform.md

# Observability and Portfolio Polish Evidence

## What was observed

Portfolio and operational documentation was added after the pipeline gained validation, registry, processing, and DuckDB persistence.

Changed documentation surfaces:

- `docs/architecture.md` includes a Mermaid data-flow diagram, runtime service table, CLI responsibility/operational-signal table, open-source posture, and a rationale for deferring Prometheus/Grafana.
- `docs/runbook.md` covers Docker/Kafka failures, missing topics, schema/DLQ debugging, Apicurio re-registration after restart, processor no-output debugging, DuckDB replay/idempotency behavior, and generated-file git hygiene.
- `docs/interview-guide.md` includes a 30-second pitch, architecture talking points, likely interview questions, a demo script, and future improvements.
- `README.md` now presents Open Streaming Lab as a local open-source streaming portfolio demo and links to the architecture, runbook, and interview guide.

## Procedure

Static validation:

```bash
make test
```

Observed output:

```text
uv run pytest
collected 15 items
src/streaming_tutorial/test_duckdb_sink.py .                             [  6%]
src/streaming_tutorial/test_processor.py ..                              [ 20%]
src/streaming_tutorial/test_producer.py ......                           [ 60%]
src/streaming_tutorial/test_registry.py ..                               [ 73%]
src/streaming_tutorial/test_validation.py ....                           [100%]
15 passed in 0.29s
node --check tools/mockingbird-jsonl.mjs
docker-compose config >/dev/null
```

Documentation inspection:

- Verified `docs/architecture.md` contains a Mermaid `flowchart LR` diagram showing Mockingbird → producer/schema/registry → Kafka main/DLQ → Quix processor → derived topic → DuckDB sink → SQL analytics, plus Kafbat UI inspection paths.
- Verified `docs/runbook.md` covers failures observed in prior evidence: Colima/Docker unavailable, Docker Desktop credential helper, topic creation, schema mismatch/DLQ inspection, Apicurio empty-after-restart, processor no output, DuckDB replay duplicates/misses, and generated file hygiene.
- Verified `docs/interview-guide.md` explains DLQ, validation, schema registry, keys/partition affinity, at-least-once/idempotency, DuckDB vs ClickHouse, and open-source-first choices.
- Verified README links the three docs from the introduction.

Review posture:

- A bounded `reviewer` subagent review was attempted for the portfolio polish diff and timed out after 180 seconds without returning usable findings.
- Parent self-review checked the acceptance criteria directly and confirmed no managed/proprietary service was introduced.

## What this supports or challenges

Supports all acceptance criteria in `.loom/tickets/done/2026-06-15-observability-and-portfolio-polish.md`:

- README clearly presents the project as a portfolio demo with architecture/setup/learning pointers.
- At least one diagram explains the full data flow (`docs/architecture.md`).
- Runbook material covers prior failures plus schema/DLQ/processor/sink failures.
- Optional metrics/dashboard work was considered and deferred with rationale: current CLI/Kafka UI/Apicurio/DuckDB signals are enough; Prometheus/Grafana should wait for long-running load tests.
- No proprietary SaaS dependency was introduced.

## Limits

- No screenshots were captured. The acceptance criteria required a diagram, not binary screenshot artifacts.
- Prometheus/Grafana were intentionally deferred.
- No successful independent reviewer findings were obtained because the reviewer run timed out.
