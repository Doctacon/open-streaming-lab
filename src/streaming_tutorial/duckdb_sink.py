"""Persist derived Kafka events into a local DuckDB analytics database."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

import duckdb
from confluent_kafka import Consumer, KafkaException

from streaming_tutorial.config import DEFAULT_PAGEVIEWS_TOPIC, defaults_from_env

DEFAULT_DUCKDB_PATH = "data/open_streaming_lab.duckdb"
DEFAULT_SINK_GROUP_ID = "open-streaming-lab-duckdb-sink"

CREATE_PAGEVIEWS_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS pageviews_by_url (
    kafka_topic TEXT NOT NULL,
    kafka_partition INTEGER NOT NULL,
    kafka_offset BIGINT NOT NULL,
    event_type TEXT NOT NULL,
    page_url TEXT NOT NULL,
    source_timestamp TIMESTAMPTZ NOT NULL,
    session_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    referrer TEXT,
    pageview_count INTEGER NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL,
    ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (kafka_topic, kafka_partition, kafka_offset)
)
"""

INSERT_PAGEVIEW_SQL = """
INSERT OR IGNORE INTO pageviews_by_url (
    kafka_topic,
    kafka_partition,
    kafka_offset,
    event_type,
    page_url,
    source_timestamp,
    session_id,
    user_id,
    referrer,
    pageview_count,
    processed_at
)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""


def ensure_database(path: Path) -> duckdb.DuckDBPyConnection:
    """Open the database and create analytics tables if needed."""

    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(path))
    connection.execute(CREATE_PAGEVIEWS_TABLE_SQL)
    return connection


def pageview_row_from_message(
    *,
    topic: str,
    partition: int,
    offset: int,
    payload: Mapping[str, Any],
) -> tuple[Any, ...]:
    """Convert a derived pageview Kafka message into the DuckDB row tuple."""

    return (
        topic,
        partition,
        offset,
        payload["event_type"],
        payload["page_url"],
        payload["source_timestamp"],
        payload["session_id"],
        payload["user_id"],
        payload.get("referrer"),
        int(payload["pageview_count"]),
        payload["processed_at"],
    )


def insert_pageview(
    connection: duckdb.DuckDBPyConnection,
    *,
    topic: str,
    partition: int,
    offset: int,
    payload: Mapping[str, Any],
) -> None:
    """Insert one pageview event idempotently by Kafka topic/partition/offset."""

    connection.execute(
        INSERT_PAGEVIEW_SQL,
        pageview_row_from_message(topic=topic, partition=partition, offset=offset, payload=payload),
    )


def build_parser() -> argparse.ArgumentParser:
    defaults = defaults_from_env()
    parser = argparse.ArgumentParser(
        prog="kafka-sink-duckdb",
        description="Persist derived pageview Kafka events into a local DuckDB database.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=defaults.bootstrap_servers,
        help=f"Kafka bootstrap servers. Default: {defaults.bootstrap_servers}",
    )
    parser.add_argument(
        "--topic",
        default=defaults.pageviews_topic,
        help=f"Derived pageviews topic. Default: {DEFAULT_PAGEVIEWS_TOPIC}",
    )
    parser.add_argument(
        "--group-id",
        default=DEFAULT_SINK_GROUP_ID,
        help=f"Sink consumer group id. Default: {DEFAULT_SINK_GROUP_ID}",
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DUCKDB_PATH,
        help=f"DuckDB database path. Default: {DEFAULT_DUCKDB_PATH}",
    )
    parser.add_argument(
        "--from-beginning",
        action="store_true",
        help="Start at earliest offsets for a new sink group.",
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
        help="Kafka poll timeout in seconds. Default: 1.0",
    )
    return parser


def sink_events(
    *,
    bootstrap_servers: str,
    topic: str,
    group_id: str,
    db_path: Path,
    from_beginning: bool,
    max_messages: int,
    timeout: float,
) -> int:
    if max_messages == 0 or max_messages < -1:
        print("--max-messages must be -1 or a positive integer", file=sys.stderr)
        return 2

    connection = ensure_database(db_path)
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest" if from_beginning else "latest",
            "enable.auto.commit": True,
            "client.id": "open-streaming-lab-duckdb-sink",
        }
    )

    print(
        f"Sinking topic={topic!r} group_id={group_id!r} db_path={str(db_path)!r} "
        f"auto.offset.reset={'earliest' if from_beginning else 'latest'}"
    )
    consumed = 0
    inserted_before = connection.execute("SELECT count(*) FROM pageviews_by_url").fetchone()[0]
    try:
        consumer.subscribe([topic])
        while max_messages == -1 or consumed < max_messages:
            message = consumer.poll(timeout)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value()
            if raw_value is None:
                print(f"Skipping null message at {message.topic()}:{message.partition()}:{message.offset()}")
                consumed += 1
                continue

            payload = json.loads(raw_value.decode("utf-8"))
            insert_pageview(
                connection,
                topic=message.topic(),
                partition=message.partition(),
                offset=message.offset(),
                payload=payload,
            )
            consumed += 1
            print(
                f"sank pageview {consumed}: topic={message.topic()} partition={message.partition()} "
                f"offset={message.offset()} page_url={payload.get('page_url')!r}"
            )
    except KeyboardInterrupt:
        print("\nStopping DuckDB sink...", file=sys.stderr)
    except (KafkaException, json.JSONDecodeError, KeyError, duckdb.Error) as exc:
        print(f"DuckDB sink error: {exc}", file=sys.stderr)
        return 1
    finally:
        consumer.close()

    inserted_after = connection.execute("SELECT count(*) FROM pageviews_by_url").fetchone()[0]
    connection.close()
    print(
        f"Consumed {consumed} message(s). Rows before={inserted_before} "
        f"after={inserted_after} inserted_or_existing_delta={inserted_after - inserted_before}."
    )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return sink_events(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        group_id=args.group_id,
        db_path=Path(args.db_path),
        from_beginning=args.from_beginning,
        max_messages=args.max_messages,
        timeout=args.timeout,
    )


if __name__ == "__main__":
    raise SystemExit(main())
