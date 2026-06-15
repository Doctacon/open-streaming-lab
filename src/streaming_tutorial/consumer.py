"""Consume tutorial events from local Kafka."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Sequence
from typing import Any

from confluent_kafka import Consumer, KafkaException

from streaming_tutorial.config import DEFAULT_GROUP_ID, DEFAULT_TOPIC, defaults_from_env


def build_parser() -> argparse.ArgumentParser:
    defaults = defaults_from_env()
    parser = argparse.ArgumentParser(
        prog="kafka-consume",
        description="Consume and print events from the local Kafka tutorial topic.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=defaults.bootstrap_servers,
        help=f"Kafka bootstrap servers. Default: {defaults.bootstrap_servers}",
    )
    parser.add_argument("--topic", default=defaults.topic, help=f"Topic name. Default: {DEFAULT_TOPIC}")
    parser.add_argument(
        "--group-id",
        default=defaults.group_id,
        help=f"Consumer group id. Default: {DEFAULT_GROUP_ID}",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start at the earliest available offset for a new consumer group.",
    )
    parser.add_argument(
        "--max-messages",
        type=int,
        default=-1,
        help="Stop after this many messages. Use -1 to run until Ctrl-C. Default: -1",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=1.0,
        help="Consumer poll timeout in seconds. Default: 1.0",
    )
    return parser


def format_value(raw_value: bytes | None) -> str:
    if raw_value is None:
        return "<null>"

    text = raw_value.decode("utf-8", errors="replace")
    try:
        parsed: Any = json.loads(text)
    except json.JSONDecodeError:
        return text

    return json.dumps(parsed, indent=2, sort_keys=True, default=str)


def consume_events(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    from_beginning: bool,
    max_messages: int,
    timeout: float,
) -> int:
    if max_messages == 0 or max_messages < -1:
        print("--max-messages must be -1 or a positive integer", file=sys.stderr)
        return 2

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
            "client.id": "mockingbird-python-consumer",
        }
    )

    print(
        f"Subscribing to topic={topic!r} group_id={group_id!r} "
        f"auto.offset.reset={'earliest' if from_beginning else 'latest'}"
    )
    print("Tip: open another terminal and run `uv run kafka-produce --limit 10 --eps 2`.")

    consumed = 0
    try:
        consumer.subscribe([topic])
        while max_messages == -1 or consumed < max_messages:
            message = consumer.poll(timeout)
            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            consumed += 1
            key = message.key().decode("utf-8", errors="replace") if message.key() else None
            print(
                "\n--- message "
                f"{consumed} topic={message.topic()} partition={message.partition()} "
                f"offset={message.offset()} key={key!r} timestamp={message.timestamp()}"
            )
            print(format_value(message.value()))

    except KeyboardInterrupt:
        print("\nStopping consumer...", file=sys.stderr)
    except KafkaException as exc:
        print(
            f"Kafka consumer error: {exc}\n"
            "Is Kafka running, and does the topic exist? Try:\n"
            "  docker-compose up -d\n"
            "  uv run kafka-admin create-topic",
            file=sys.stderr,
        )
        return 1
    finally:
        consumer.close()

    print(f"Consumed {consumed} message(s).")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return consume_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        from_beginning=args.from_beginning,
        max_messages=args.max_messages,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
