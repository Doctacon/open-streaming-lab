from __future__ import annotations

from datetime import UTC, datetime

import pytest

from streaming_tutorial.validation import (
    build_dlq_envelope,
    corrupt_event_for_demo,
    load_event_schema,
    validate_event,
    EventValidationError,
)


VALID_WEB_ANALYTICS_EVENT = {
    "timestamp": "2026-06-15T23:32:14.243Z",
    "session_id": "fc7fb8e4-58f2-46d9-a74d-605729432c7d",
    "user_id": "ea876f36-e768-4897-80bd-9e1bdb641a46",
    "page_url": "https://example.com/contact",
    "referrer": "https://facebook.com",
}


def test_web_analytics_schema_accepts_valid_event() -> None:
    event_schema = load_event_schema("web_analytics_v1")

    validate_event(dict(VALID_WEB_ANALYTICS_EVENT), event_schema)


def test_web_analytics_schema_rejects_missing_required_field() -> None:
    event_schema = load_event_schema("web_analytics_v1")
    invalid_event = dict(VALID_WEB_ANALYTICS_EVENT)
    invalid_event.pop("timestamp")

    with pytest.raises(EventValidationError, match="'timestamp' is a required property") as exc_info:
        validate_event(invalid_event, event_schema)

    assert exc_info.value.path == "<root>"
    assert exc_info.value.schema_id == "web_analytics_v1"


def test_dlq_envelope_contains_diagnostics() -> None:
    event_schema = load_event_schema("web_analytics_v1")
    invalid_event = dict(VALID_WEB_ANALYTICS_EVENT)
    invalid_event.pop("timestamp")

    with pytest.raises(EventValidationError) as exc_info:
        validate_event(invalid_event, event_schema)

    envelope = build_dlq_envelope(
        original_event=invalid_event,
        target_topic="mockingbird.events",
        validation_error=exc_info.value,
        event_schema=event_schema,
        rejected_at=datetime(2026, 6, 15, 23, 45, tzinfo=UTC),
    )

    assert envelope == {
        "rejected_at": "2026-06-15T23:45:00Z",
        "target_topic": "mockingbird.events",
        "schema_id": "web_analytics_v1",
        "schema_name": "web_analytics",
        "schema_version": "1",
        "validation_error": "'timestamp' is a required property at <root>",
        "validation_path": "<root>",
        "original_event": invalid_event,
    }


def test_corrupt_event_for_demo_removes_timestamp_every_nth_event() -> None:
    first_event = corrupt_event_for_demo(dict(VALID_WEB_ANALYTICS_EVENT), event_number=1, invalid_every=2)
    second_event = corrupt_event_for_demo(dict(VALID_WEB_ANALYTICS_EVENT), event_number=2, invalid_every=2)

    assert first_event == VALID_WEB_ANALYTICS_EVENT
    assert "timestamp" not in second_event
    assert second_event["session_id"] == VALID_WEB_ANALYTICS_EVENT["session_id"]
