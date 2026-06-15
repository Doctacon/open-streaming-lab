Status: open
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/2026-06-15-schema-validation-dlq.md

# Add Apicurio Schema Registry Chapter

## Scope

Add an optional schema-registry chapter using the open-source Apicurio Registry.

In scope:

- Add Apicurio Registry to Compose in a learner-friendly local configuration.
- Register the existing JSON Schema artifact.
- Add commands or scripts for registering and fetching schema artifacts.
- Update producer/consumer validation to optionally read schemas from the registry while preserving file-based fallback.
- Document the difference between local file validation and registry-backed contracts.
- Include troubleshooting for registry startup and connectivity.

Out of scope:

- Confluent Cloud or proprietary registry services.
- Full enterprise schema governance.
- Avro/Protobuf unless explicitly chosen later.

## Acceptance criteria

- `docker-compose config` succeeds with the registry service included.
- Registry UI/API is reachable locally.
- A schema can be registered and fetched with documented commands.
- Producer validation can use either local file schema or registry-fetched schema.
- README chapter explains why a schema registry matters and what Apicurio replaces in a Confluent-centric architecture.
- Evidence records commands and outputs.

## Progress and notes

- 2026-06-15: Ticket opened as second schema milestone after local validation/DLQ.

## Blockers

- Requires schema validation/DLQ ticket so the learner already understands schema enforcement before adding a registry.
