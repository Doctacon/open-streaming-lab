Status: open
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/done/2026-06-15-baseline-closure-and-repo-hygiene.md, .loom/specs/next-level-streaming-platform.md

# Add JSON Schema Validation and Dead-Letter Topic

## Scope

Add the first production-style streaming feature: validate generated events before publication and route bad events to a DLQ.

In scope:

- Add versioned local JSON Schema files under a clear directory such as `schemas/`.
- Cover at least the default `Web Analytics Starter Kit` event shape.
- Add Python validation helpers, likely using the open-source `jsonschema` package.
- Update producer flow:
  - valid event → `mockingbird.events`,
  - invalid event → `mockingbird.events.dlq` with diagnostic envelope.
- Add admin support and Makefile targets for creating/describing the DLQ topic.
- Add a deliberate invalid-event test path for learning, for example a CLI flag that corrupts or drops a required field every N events.
- Add unit tests for valid validation, invalid validation, DLQ envelope construction, and producer branching behavior where practical.
- Update README with a chapter explaining schema validation and DLQ behavior.
- Record validation evidence.

Out of scope:

- Apicurio Registry.
- Avro or Protobuf.
- Kafka Connect.
- Stream processing and analytics sinks.

## Acceptance criteria

- `make test` passes.
- Admin tooling can create both `mockingbird.events` and `mockingbird.events.dlq`.
- A runtime smoke test proves at least one valid event reaches the main topic.
- A runtime smoke test proves at least one intentionally invalid event reaches the DLQ.
- DLQ messages include original payload, validation error, target topic, schema identifier/version, and rejection timestamp.
- README has copy/pasteable commands for the validation/DLQ chapter.

## Progress and notes

- 2026-06-15: Ticket opened as the recommended first next-level implementation slice.

## Blockers

- Requires baseline closure/hygiene ticket to establish a clean starting point.
