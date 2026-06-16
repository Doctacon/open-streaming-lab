Status: done
Created: 2026-06-15
Updated: 2026-06-15
Parent: .loom/tickets/done/2026-06-15-next-level-streaming-platform.md
Depends-On: .loom/tickets/done/2026-06-15-schema-validation-dlq.md

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

## Current State

Done. Apicurio Registry is available in Compose, schemas can be registered/fetched through the `schema-registry` CLI, and the producer can validate with a registry-fetched schema while retaining local and disabled-schema modes.

## Progress and notes

- 2026-06-15: Ticket opened as second schema milestone after local validation/DLQ.
- 2026-06-15: Set Status to `active` for autonomous Loom-driver execution after closing `.loom/tickets/done/2026-06-15-schema-validation-dlq.md`.
- 2026-06-15: Implemented Apicurio Registry Compose service, `schema-registry` CLI, registry-backed producer schema loading, Makefile/README docs, tests, runtime evidence, and reviewer pass. Evidence recorded in `.loom/evidence/2026-06-15-apicurio-schema-registry.md`.

## Blockers

None.
