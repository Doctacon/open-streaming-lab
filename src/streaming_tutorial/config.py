"""Shared configuration defaults for the local Kafka tutorial."""

from __future__ import annotations

import os
from dataclasses import dataclass

DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
DEFAULT_TOPIC = "mockingbird.events"
DEFAULT_DLQ_TOPIC = "mockingbird.events.dlq"
DEFAULT_PAGEVIEWS_TOPIC = "mockingbird.pageviews.by_url"
DEFAULT_GROUP_ID = "mockingbird-python-learners"
DEFAULT_TEMPLATE = "Web Analytics Starter Kit"
DEFAULT_EPS = 2.0
DEFAULT_LIMIT = 20
DEFAULT_REGISTRY_URL = "http://127.0.0.1:8081"
DEFAULT_REGISTRY_GROUP = "open-streaming-lab"
DEFAULT_REGISTRY_VERSION = "1"


@dataclass(frozen=True)
class KafkaDefaults:
    """Environment-aware defaults used by the CLI scripts."""

    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS
    topic: str = DEFAULT_TOPIC
    dlq_topic: str = DEFAULT_DLQ_TOPIC
    pageviews_topic: str = DEFAULT_PAGEVIEWS_TOPIC
    group_id: str = DEFAULT_GROUP_ID
    template: str = DEFAULT_TEMPLATE
    eps: float = DEFAULT_EPS
    limit: int = DEFAULT_LIMIT
    registry_url: str = DEFAULT_REGISTRY_URL
    registry_group: str = DEFAULT_REGISTRY_GROUP
    registry_version: str = DEFAULT_REGISTRY_VERSION


def defaults_from_env() -> KafkaDefaults:
    """Read non-secret local defaults from environment variables.

    These are intentionally safe to show in tutorial docs. Do not add credentials
    here; the tutorial uses plaintext local Kafka only.
    """

    return KafkaDefaults(
        bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS),
        topic=os.getenv("KAFKA_TOPIC", DEFAULT_TOPIC),
        dlq_topic=os.getenv("KAFKA_DLQ_TOPIC", DEFAULT_DLQ_TOPIC),
        pageviews_topic=os.getenv("KAFKA_PAGEVIEWS_TOPIC", DEFAULT_PAGEVIEWS_TOPIC),
        group_id=os.getenv("KAFKA_GROUP_ID", DEFAULT_GROUP_ID),
        template=os.getenv("MOCKINGBIRD_TEMPLATE", DEFAULT_TEMPLATE),
        eps=_float_from_env("EVENTS_PER_SECOND", DEFAULT_EPS),
        limit=_int_from_env("EVENT_LIMIT", DEFAULT_LIMIT),
        registry_url=os.getenv("APICURIO_REGISTRY_URL", DEFAULT_REGISTRY_URL),
        registry_group=os.getenv("APICURIO_REGISTRY_GROUP", DEFAULT_REGISTRY_GROUP),
        registry_version=os.getenv("APICURIO_REGISTRY_VERSION", DEFAULT_REGISTRY_VERSION),
    )


def _float_from_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number; received {value!r}") from exc


def _int_from_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer; received {value!r}") from exc
