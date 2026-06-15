Status: open
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/2026-06-15-duckdb-analytics-sink.md

# Observability and Portfolio Polish

## Scope

Make the project operationally understandable and portfolio-ready after the pipeline has validation, processing, and persistence.

In scope:

- Add structured logging conventions across producer, consumer, processor, and sink.
- Add lightweight metrics where useful: produced count, consumed count, validation failure count, DLQ count, processor output count, sink row count.
- Consider Prometheus/Grafana only if the added complexity is justified by real signals.
- Add architecture diagrams, screenshots, and interview talking points.
- Add a `runbook` or troubleshooting section: Kafka unavailable, topic missing, schema mismatch, DLQ inspection, consumer lag, sink replay duplicates.
- Polish README into a chaptered tutorial.

Out of scope:

- Production alerting/SLOs.
- Kubernetes/Helm.
- Managed observability services.

## Acceptance criteria

- README clearly presents the project as a portfolio demo with architecture, setup, and learning outcomes.
- At least one diagram explains the data flow.
- Troubleshooting/runbook material covers the failures encountered in prior evidence plus new validation/DLQ/processor/sink failures.
- Optional metrics/dashboard work has evidence and remains locally runnable.
- No proprietary SaaS dependency is introduced.

## Progress and notes

- 2026-06-15: Ticket opened as final polish milestone after core pipeline functionality.

## Blockers

- Should wait until the earlier tickets create real pipeline behavior worth observing and explaining.
