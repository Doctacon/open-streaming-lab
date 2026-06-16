Status: done
Created: 2026-06-15
Updated: 2026-06-15
Parent: none
Depends-On: .loom/tickets/done/20260613-python-kafka-mockingbird-tutorial.md

# Plan: Next-Level Local Streaming Platform

## Scope

Coordinate the staged upgrade from a baseline Kafka tutorial into a portfolio-quality local streaming data project.

This parent ticket is orchestration only. Child tickets are the executable units.

## Related records

- `.loom/specs/next-level-streaming-platform.md`
- `.loom/decisions/python-first-streaming-roadmap.md`
- `.loom/research/2026-06-15-next-level-streaming-roadmap.md`
- `.loom/tickets/done/20260613-python-kafka-mockingbird-tutorial.md`

## Child tickets and sequencing

1. `.loom/tickets/done/2026-06-15-baseline-closure-and-repo-hygiene.md`
   - Close/accept the existing baseline state, initialize git if desired, and make sure the project starts from a known clean foundation.
2. `.loom/tickets/done/2026-06-15-schema-validation-dlq.md`
   - First major feature. Add local JSON Schema validation and a DLQ topic.
3. `.loom/tickets/done/2026-06-15-apicurio-schema-registry-chapter.md`
   - Add an optional registry-backed chapter using Apicurio Registry.
4. `.loom/tickets/done/2026-06-15-python-stream-processor.md`
   - Add a Python stream-processing app and derived topics.
5. `.loom/tickets/done/2026-06-15-duckdb-analytics-sink.md`
   - Persist events or aggregates to DuckDB and add SQL examples.
6. `.loom/tickets/done/2026-06-15-observability-and-portfolio-polish.md`
   - Add metrics/logging polish, diagrams, screenshots, and interview material.

## Acceptance criteria

- Each child ticket has evidence before closure.
- The README remains runnable chapter-by-chapter from a clean checkout.
- No managed/proprietary services are introduced.
- The platform remains explainable as a local learning and portfolio project.

## Progress and notes

- 2026-06-15: Created parent plan and six child tickets after user asked for a concrete next-level implementation plan.
- 2026-06-15: Closed baseline scaffold/hygiene tickets, initialized local git repository, and updated README roadmap.
- 2026-06-15: Project name selected as Open Streaming Lab; updated README title, Python distribution name, Node helper package name, lockfiles, and `.loom/knowledge/project-name.md`.
- 2026-06-15: Closed schema validation/DLQ ticket with static and runtime smoke evidence in `.loom/evidence/2026-06-15-schema-validation-dlq.md`.
- 2026-06-15: Closed Apicurio schema registry ticket with API/register/fetch/producer smoke evidence in `.loom/evidence/2026-06-15-apicurio-schema-registry.md`.
- 2026-06-15: Closed Python stream processor ticket with Quix transform and derived-topic smoke evidence in `.loom/evidence/2026-06-15-python-stream-processor.md`.
- 2026-06-15: Closed DuckDB analytics sink ticket with row-count and query-output smoke evidence in `.loom/evidence/2026-06-15-duckdb-analytics-sink.md`.
- 2026-06-15: Closed observability/portfolio polish ticket with documentation evidence in `.loom/evidence/2026-06-15-observability-portfolio-polish.md`.
- 2026-06-15: All child tickets in this parent plan are closed.

## Blockers

None. All child tickets are closed.

## Current State

Done. All child tickets have been closed with evidence. The worktree contains uncommitted implementation, documentation, Loom record, dependency lockfile, and generated ignore-rule changes ready for operator review/commit.
