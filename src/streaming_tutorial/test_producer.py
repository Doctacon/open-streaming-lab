from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from streaming_tutorial.producer import (
    build_generator_command,
    choose_message_key,
    parse_event_line,
    route_event,
)
from streaming_tutorial.validation import load_event_schema
from streaming_tutorial.test_validation import VALID_WEB_ANALYTICS_EVENT


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[dict[str, Any]] = []

    def produce(
        self,
        topic: str,
        *,
        key: str | None = None,
        value: bytes | None = None,
        on_delivery: Any = None,
    ) -> None:
        self.messages.append({"topic": topic, "key": key, "value": value})

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float | None = None) -> int:
        return 0


def noop_delivery(_error: Any, _message: Any) -> None:
    return None


def test_choose_message_key_prefers_identity_fields() -> None:
    event = {"user_id": "user-123", "session_id": "session-456", "stock_symbol": "APG:XNYS"}

    assert choose_message_key(event) == "user-123"


def test_choose_message_key_falls_back_to_domain_identifier() -> None:
    event = {"stock_symbol": "APG:XNYS", "amount": 42.5}

    assert choose_message_key(event) == "APG:XNYS"


def test_parse_event_line_requires_json_object() -> None:
    event = parse_event_line(json.dumps({"some_int": 1}))

    assert event == {"some_int": 1}

    with pytest.raises(ValueError, match="Expected a JSON object"):
        parse_event_line("[1, 2, 3]")


def test_build_generator_command_points_at_mockingbird_helper() -> None:
    command = build_generator_command(
        root=Path("/tutorial"),
        template="Stock Prices",
        eps=2.0,
        limit=5,
    )

    assert command == [
        "node",
        "/tutorial/tools/mockingbird-jsonl.mjs",
        "--template",
        "Stock Prices",
        "--eps",
        "2.0",
        "--limit",
        "5",
    ]


def test_route_event_sends_valid_event_to_main_topic() -> None:
    producer = FakeProducer()
    event_schema = load_event_schema("web_analytics_v1")

    result = route_event(
        producer,
        dict(VALID_WEB_ANALYTICS_EVENT),
        event_number=1,
        topic="mockingbird.events",
        dlq_topic="mockingbird.events.dlq",
        event_schema=event_schema,
        invalid_every=0,
        on_delivery=noop_delivery,
    )

    assert result.valid is True
    assert result.topic == "mockingbird.events"
    assert producer.messages[0]["topic"] == "mockingbird.events"
    assert json.loads(producer.messages[0]["value"].decode("utf-8")) == VALID_WEB_ANALYTICS_EVENT


def test_route_event_sends_intentionally_invalid_event_to_dlq() -> None:
    producer = FakeProducer()
    event_schema = load_event_schema("web_analytics_v1")

    result = route_event(
        producer,
        dict(VALID_WEB_ANALYTICS_EVENT),
        event_number=2,
        topic="mockingbird.events",
        dlq_topic="mockingbird.events.dlq",
        event_schema=event_schema,
        invalid_every=2,
        on_delivery=noop_delivery,
    )

    assert result.valid is False
    assert result.topic == "mockingbird.events.dlq"
    assert result.validation_error == "'timestamp' is a required property at <root>"
    assert producer.messages[0]["topic"] == "mockingbird.events.dlq"

    envelope = json.loads(producer.messages[0]["value"].decode("utf-8"))
    assert envelope["target_topic"] == "mockingbird.events"
    assert envelope["schema_id"] == "web_analytics_v1"
    assert envelope["schema_name"] == "web_analytics"
    assert envelope["schema_version"] == "1"
    assert envelope["validation_error"] == "'timestamp' is a required property at <root>"
    assert "timestamp" not in envelope["original_event"]
    assert envelope["original_event"]["user_id"] == VALID_WEB_ANALYTICS_EVENT["user_id"]
