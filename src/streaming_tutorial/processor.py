"""Quix Streams processor that derives pageview events from valid web analytics events."""

from __future__ import annotations

import argparse
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import Any

from streaming_tutorial.config import DEFAULT_PAGEVIEWS_TOPIC, DEFAULT_TOPIC, defaults_from_env

DEFAULT_PROCESSOR_GROUP_ID = "open-streaming-lab-pageviews-processor"


def build_pageview_by_url_event(event: Mapping[str, Any], *, processed_at: datetime | None = None) -> dict[str, Any]:
    """Build a traceable derived pageview event from a valid Web Analytics event."""

    processed_at = processed_at or datetime.now(UTC)
    page_url = str(event["page_url"])
    return {
        "event_type": "pageview_by_url",
        "page_url": page_url,
        "source_timestamp": event["timestamp"],
        "session_id": event["session_id"],
        "user_id": event["user_id"],
        "referrer": event.get("referrer"),
        "pageview_count": 1,
        "processed_at": processed_at.isoformat().replace("+00:00", "Z"),
    }


def pageview_key(event: Mapping[str, Any]) -> str:
    """Key derived events by URL so same-page records share partition affinity."""

    return str(event["page_url"])


def build_parser() -> argparse.ArgumentParser:
    defaults = defaults_from_env()
    parser = argparse.ArgumentParser(
        prog="kafka-process",
        description="Process valid Web Analytics events into derived pageview-by-URL events using Quix Streams.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=defaults.bootstrap_servers,
        help=f"Kafka bootstrap servers. Default: {defaults.bootstrap_servers}",
    )
    parser.add_argument("--input-topic", default=defaults.topic, help=f"Input topic. Default: {DEFAULT_TOPIC}")
    parser.add_argument(
        "--output-topic",
        default=defaults.pageviews_topic,
        help=f"Derived output topic. Default: {DEFAULT_PAGEVIEWS_TOPIC}",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_PROCESSOR_GROUP_ID,
        help=f"Processor consumer group id. Default: {DEFAULT_PROCESSOR_GROUP_ID}",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start at earliest offsets for a new processor group.",
    )
    parser.add_argument(
        "--max-outputs",
        type=int,
        default=-1,
        help="Stop after this many output records. Use -1 to run until Ctrl-C. Default: -1",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=0.0,
        help="Optional Quix run timeout in seconds. Default: 0 means no timeout.",
    )
    return parser


def process_events(
    *,
    bootstrap_servers: str,
    input_topic: str,
    output_topic: str,
    group_id: str,
    from_beginning: bool,
    max_outputs: int,
    timeout: float,
) -> int:
    if max_outputs == 0 or max_outputs < -1:
        print("--max-outputs must be -1 or a positive integer")
        return 2
    if timeout < 0:
        print("--timeout must be greater than or equal to 0")
        return 2

    try:
        from quixstreams import Application
    except ImportError as exc:  # pragma: no cover - exercised only with broken installs
        raise RuntimeError("quixstreams is not installed. Run `uv sync --extra dev` or `make install`.") from exc

    app = Application(
        broker_address=bootstrap_servers,
        consumer_group=group_id,
        auto_offset_reset="earliest" if from_beginning else "latest",
    )
    source = app.topic(input_topic, value_deserializer="json", key_deserializer="string")
    output = app.topic(output_topic, value_serializer="json", key_serializer="string")

    sdf = app.dataframe(source)
    sdf = sdf.apply(lambda event: build_pageview_by_url_event(event))
    sdf = sdf.update(lambda event: print(f"derived pageview: {event}"))
    sdf = sdf.to_topic(output, key=pageview_key)

    print(
        f"Processing input_topic={input_topic!r} output_topic={output_topic!r} "
        f"group_id={group_id!r} auto_offset_reset={'earliest' if from_beginning else 'latest'}"
    )
    print("Tip: produce source events with `uv run kafka-produce --limit 10 --eps 2`.")

    app.run(timeout=timeout, count=0 if max_outputs == -1 else max_outputs)
    print("Processor stopped.")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return process_events(
            bootstrap_servers=args.bootstrap_servers,
            input_topic=args.input_topic,
            output_topic=args.output_topic,
            group_id=args.group_id,
            from_beginning=args.from_beginning,
            max_outputs=args.max_outputs,
            timeout=args.timeout,
        )
    except RuntimeError as exc:
        print(str(exc))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
