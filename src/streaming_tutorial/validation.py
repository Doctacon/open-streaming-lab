"""JSON Schema validation and DLQ helpers for tutorial events."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError

DEFAULT_SCHEMA_ID = "web_analytics_v1"


@dataclass(frozen=True)
class EventSchema:
    """Loaded JSON Schema plus metadata used in DLQ diagnostics."""

    schema_id: str
    name: str
    version: str
    schema: dict[str, Any]
    validator: Draft202012Validator


class EventValidationError(ValueError):
    """Raised when an event does not match the configured event schema."""

    def __init__(self, message: str, *, path: str, schema_id: str) -> None:
        super().__init__(message)
        self.path = path
        self.schema_id = schema_id


def project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def build_event_schema(*, schema_id: str, raw_schema: dict[str, Any]) -> EventSchema:
    """Build a validated EventSchema from a JSON Schema dictionary."""

    Draft202012Validator.check_schema(raw_schema)
    return EventSchema(
        schema_id=schema_id,
        name=str(raw_schema.get("x-schema-name") or raw_schema.get("title") or schema_id),
        version=str(raw_schema.get("x-schema-version") or "unknown"),
        schema=raw_schema,
        validator=Draft202012Validator(raw_schema, format_checker=FormatChecker()),
    )


def load_event_schema(schema_id: str = DEFAULT_SCHEMA_ID, *, root: Path | None = None) -> EventSchema:
    """Load a local versioned JSON Schema from the repository's schemas directory."""

    root = root or project_root()
    schema_path = root / "schemas" / f"{schema_id}.json"
    try:
        raw_schema = json.loads(schema_path.read_text())
    except FileNotFoundError as exc:
        raise FileNotFoundError(
            f"Could not find schema {schema_id!r} at {schema_path}. "
            "Use --schema web_analytics_v1 or add the schema file first."
        ) from exc

    return build_event_schema(schema_id=schema_id, raw_schema=raw_schema)


def validate_event(event: dict[str, Any], event_schema: EventSchema) -> None:
    """Validate an event or raise a compact, user-friendly validation error."""

    try:
        event_schema.validator.validate(event)
    except ValidationError as exc:
        path = ".".join(str(part) for part in exc.absolute_path) or "<root>"
        raise EventValidationError(
            f"{exc.message} at {path}",
            path=path,
            schema_id=event_schema.schema_id,
        ) from exc


def build_dlq_envelope(
    *,
    original_event: dict[str, Any],
    target_topic: str,
    validation_error: EventValidationError,
    event_schema: EventSchema,
    rejected_at: datetime | None = None,
) -> dict[str, Any]:
    """Build the diagnostic payload published to the dead-letter topic."""

    rejected_at = rejected_at or datetime.now(UTC)
    return {
        "rejected_at": rejected_at.isoformat().replace("+00:00", "Z"),
        "target_topic": target_topic,
        "schema_id": event_schema.schema_id,
        "schema_name": event_schema.name,
        "schema_version": event_schema.version,
        "validation_error": str(validation_error),
        "validation_path": validation_error.path,
        "original_event": original_event,
    }


def corrupt_event_for_demo(event: dict[str, Any], *, event_number: int, invalid_every: int) -> dict[str, Any]:
    """Return an intentionally invalid copy every N events for DLQ demos.

    The corruption removes the required `timestamp` field from Web Analytics
    events. Keeping this deterministic makes the demo easy to explain and test.
    """

    if invalid_every <= 0 or event_number % invalid_every != 0:
        return event

    corrupted = dict(event)
    corrupted.pop("timestamp", None)
    return corrupted
