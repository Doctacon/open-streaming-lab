from __future__ import annotations

import json
from pathlib import Path

import pytest

from streaming_tutorial.producer import (
    build_generator_command,
    choose_message_key,
    parse_event_line,
)


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
