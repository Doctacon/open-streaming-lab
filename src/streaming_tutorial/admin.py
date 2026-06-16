"""Small Kafka admin CLI for the tutorial."""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence

from confluent_kafka import KafkaError, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

from streaming_tutorial.config import DEFAULT_DLQ_TOPIC, DEFAULT_PAGEVIEWS_TOPIC, DEFAULT_TOPIC, defaults_from_env


def build_parser() -> argparse.ArgumentParser:
    defaults = defaults_from_env()
    parser = argparse.ArgumentParser(
        prog="kafka-admin",
        description="Create and inspect local Kafka topics for Open Streaming Lab.",
    )
    parser.add_argument(
        "--bootstrap-servers",
        default=defaults.bootstrap_servers,
        help=f"Kafka bootstrap servers. Default: {defaults.bootstrap_servers}",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    create = subparsers.add_parser("create-topic", help="Create one topic if needed.")
    create.add_argument("--topic", default=defaults.topic, help=f"Topic name. Default: {DEFAULT_TOPIC}")
    create.add_argument("--partitions", type=int, default=3, help="Partition count. Default: 3")
    create.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor. Use 1 for the single local broker. Default: 1",
    )
    create.add_argument("--timeout", type=float, default=15.0, help="Admin timeout in seconds.")

    create_all = subparsers.add_parser(
        "create-topics",
        help="Create the main tutorial topic and its dead-letter topic if needed.",
    )
    create_all.add_argument("--topic", default=defaults.topic, help=f"Main topic name. Default: {DEFAULT_TOPIC}")
    create_all.add_argument(
        "--dlq-topic",
        default=defaults.dlq_topic,
        help=f"Dead-letter topic name. Default: {DEFAULT_DLQ_TOPIC}",
    )
    create_all.add_argument("--partitions", type=int, default=3, help="Main topic partition count. Default: 3")
    create_all.add_argument(
        "--dlq-partitions",
        type=int,
        default=1,
        help="DLQ partition count. Default: 1",
    )
    create_all.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor. Use 1 for the single local broker. Default: 1",
    )
    create_all.add_argument("--timeout", type=float, default=15.0, help="Admin timeout in seconds.")

    create_derived = subparsers.add_parser(
        "create-derived-topics",
        help="Create derived stream-processing output topics if needed.",
    )
    create_derived.add_argument(
        "--pageviews-topic",
        default=defaults.pageviews_topic,
        help=f"Pageviews-by-URL derived topic name. Default: {DEFAULT_PAGEVIEWS_TOPIC}",
    )
    create_derived.add_argument("--partitions", type=int, default=3, help="Derived topic partition count. Default: 3")
    create_derived.add_argument(
        "--replication-factor",
        type=int,
        default=1,
        help="Replication factor. Use 1 for the single local broker. Default: 1",
    )
    create_derived.add_argument("--timeout", type=float, default=15.0, help="Admin timeout in seconds.")

    describe = subparsers.add_parser("describe", help="Describe known topics.")
    describe.add_argument("--topic", default=defaults.topic, help=f"Topic name. Default: {DEFAULT_TOPIC}")
    describe.add_argument("--timeout", type=float, default=10.0, help="Admin timeout in seconds.")

    return parser


def create_topic(
    *,
    bootstrap_servers: str,
    topic: str,
    partitions: int,
    replication_factor: int,
    timeout: float,
) -> int:
    if partitions < 1:
        print("Partitions must be at least 1.", file=sys.stderr)
        return 2
    if replication_factor < 1:
        print("Replication factor must be at least 1.", file=sys.stderr)
        return 2

    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    new_topic = NewTopic(topic, num_partitions=partitions, replication_factor=replication_factor)

    futures = admin.create_topics([new_topic], request_timeout=timeout, operation_timeout=timeout)
    future = futures[topic]

    try:
        future.result(timeout=timeout)
    except KafkaException as exc:
        kafka_error = exc.args[0] if exc.args else None
        if kafka_error is not None and kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
            print(f"Topic already exists: {topic}")
            return 0
        print(
            f"Could not create topic {topic!r} on {bootstrap_servers}: {exc}\n"
            "Is Kafka running? Try `docker-compose up -d` and wait for the broker to become healthy.",
            file=sys.stderr,
        )
        return 1

    print(
        f"Created topic {topic!r} with {partitions} partition(s) "
        f"and replication factor {replication_factor}."
    )
    return 0


def create_tutorial_topics(
    *,
    bootstrap_servers: str,
    topic: str,
    dlq_topic: str,
    partitions: int,
    dlq_partitions: int,
    replication_factor: int,
    timeout: float,
) -> int:
    """Create the main events topic and the dead-letter topic."""

    main_result = create_topic(
        bootstrap_servers=bootstrap_servers,
        topic=topic,
        partitions=partitions,
        replication_factor=replication_factor,
        timeout=timeout,
    )
    dlq_result = create_topic(
        bootstrap_servers=bootstrap_servers,
        topic=dlq_topic,
        partitions=dlq_partitions,
        replication_factor=replication_factor,
        timeout=timeout,
    )
    return 0 if main_result == 0 and dlq_result == 0 else 1


def describe_topic(*, bootstrap_servers: str, topic: str, timeout: float) -> int:
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})

    try:
        metadata = admin.list_topics(topic=topic, timeout=timeout)
    except KafkaException as exc:
        print(
            f"Could not read topic metadata from {bootstrap_servers}: {exc}\n"
            "Is Kafka running? Try `docker-compose up -d` first.",
            file=sys.stderr,
        )
        return 1

    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        print(f"Topic not found: {topic}", file=sys.stderr)
        return 1

    print(f"Topic: {topic}")
    for partition_id, partition in sorted(topic_metadata.partitions.items()):
        print(
            f"  partition={partition_id} leader={partition.leader} "
            f"replicas={partition.replicas} isrs={partition.isrs}"
        )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.command == "create-topic":
        return create_topic(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            timeout=args.timeout,
        )

    if args.command == "create-topics":
        return create_tutorial_topics(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            dlq_topic=args.dlq_topic,
            partitions=args.partitions,
            dlq_partitions=args.dlq_partitions,
            replication_factor=args.replication_factor,
            timeout=args.timeout,
        )

    if args.command == "create-derived-topics":
        return create_topic(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.pageviews_topic,
            partitions=args.partitions,
            replication_factor=args.replication_factor,
            timeout=args.timeout,
        )

    if args.command == "describe":
        return describe_topic(
            bootstrap_servers=args.bootstrap_servers,
            topic=args.topic,
            timeout=args.timeout,
        )

    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
