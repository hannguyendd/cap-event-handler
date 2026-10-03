"""Settings and transport configuration.

Settings are frozen: every value is read at startup and never mutated, so a
running app cannot drift from what validation.py checked.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta
from typing import Literal

from cap_event_handler.errors import ConfigurationError

OnUnroutable = Literal["reject", "ack"]


@dataclass(frozen=True, slots=True)
class Settings:
    """Tuning for both pipelines. Defaults are the spec's defaults."""

    # storage
    schema_name: str | None = "cap"
    table_prefix: str = "cap_"
    auto_create_tables: bool = True

    # retention
    inbox_retention: timedelta = timedelta(days=15)
    outbox_retention: timedelta = timedelta(days=1)

    # publish
    publish_timeout: timedelta = timedelta(seconds=10)
    send_queue_maxsize: int = 1000
    max_retries: int = 50

    # consume
    group_concurrency: int = 1
    consumer_tasks: int = 1
    wait_seconds: int = 20
    max_messages: int = 10
    reject_backoff_seconds: int = 5
    on_unroutable: OnUnroutable = "reject"
    expected_max_handler_seconds: int = 30

    # processors
    sweeper_interval: timedelta = timedelta(seconds=30)
    sweeper_lookback: timedelta = timedelta(seconds=60)
    collector_interval: timedelta = timedelta(seconds=300)
    collector_batch: int = 1000

    # lifecycle
    provision: bool = True
    shutdown_timeout: timedelta = timedelta(seconds=30)

    def __post_init__(self) -> None:
        if self.on_unroutable not in ("reject", "ack"):
            raise ConfigurationError(
                f"on_unroutable must be 'reject' or 'ack', got {self.on_unroutable!r}"
            )
        if not 1 <= self.max_messages <= 10:
            raise ConfigurationError(
                f"max_messages must be between 1 and 10 (the SQS limit), got {self.max_messages}"
            )
        if not 0 <= self.wait_seconds <= 20:
            raise ConfigurationError(
                f"wait_seconds must be between 0 and 20 (the SQS limit), got {self.wait_seconds}"
            )
        if self.group_concurrency < 1:
            raise ConfigurationError("group_concurrency must be at least 1")
        if self.consumer_tasks < 1:
            raise ConfigurationError("consumer_tasks must be at least 1")
        if self.collector_batch < 1:
            raise ConfigurationError("collector_batch must be at least 1")


@dataclass(frozen=True, slots=True)
class SnsSqsConfig:
    """Where to reach SNS and SQS. endpoint_url targets LocalStack in tests."""

    region: str
    endpoint_url: str | None = None
    queue_name: str | None = None
    """Override the queue name derived from the consumer group."""


_NATS_NAME_RE = re.compile(r"^[A-Za-z0-9_-]+$")


@dataclass(frozen=True, slots=True)
class NatsConfig:
    """Where to reach NATS, and the shape of the JetStream streams and consumers.

    One stream (`stream`) captures every event subject under `subject_prefix`;
    dead letters go to `dlq_stream` under `dlq_subject_prefix`. Defaults pass
    startup validation against the default `Settings`.
    """

    servers: str | tuple[str, ...] = "nats://127.0.0.1:4222"
    stream: str = "CAP"
    subject_prefix: str = "cap"
    dlq_stream: str = "CAP_DLQ"
    dlq_subject_prefix: str = "cap-dlq"
    ack_wait: timedelta = timedelta(seconds=60)
    max_deliver: int = 5
    """Deliveries before the library dead-letters a failing message. At least 2."""
    max_age: timedelta = timedelta(days=7)
    """Stream retention. Zero means unlimited."""
    duplicate_window: timedelta = timedelta(minutes=5)
    num_replicas: int = 1

    def __post_init__(self) -> None:
        for name in ("stream", "subject_prefix", "dlq_stream", "dlq_subject_prefix"):
            value = getattr(self, name)
            if not _NATS_NAME_RE.match(value):
                raise ConfigurationError(
                    f"{name} must match {_NATS_NAME_RE.pattern} (one subject token), got {value!r}"
                )
        if self.dlq_stream == self.stream:
            raise ConfigurationError("dlq_stream must differ from stream")
        if self.dlq_subject_prefix == self.subject_prefix:
            raise ConfigurationError(
                "dlq_subject_prefix must differ from subject_prefix: "
                "a subject cannot belong to two streams"
            )
        if self.max_deliver < 2:
            raise ConfigurationError(
                f"max_deliver must be at least 2 so a failure is retried before it is "
                f"dead-lettered, got {self.max_deliver}"
            )
        if self.ack_wait <= timedelta(0):
            raise ConfigurationError("ack_wait must be positive")
        if self.duplicate_window <= timedelta(0):
            raise ConfigurationError("duplicate_window must be positive")
        if self.max_age < timedelta(0):
            raise ConfigurationError("max_age must be zero (unlimited) or positive")
        if self.max_age and self.duplicate_window > self.max_age:
            raise ConfigurationError(
                f"duplicate_window ({self.duplicate_window}) must not exceed max_age "
                f"({self.max_age}); NATS refuses such a stream"
            )
        if not 1 <= self.num_replicas <= 5:
            raise ConfigurationError(
                f"num_replicas must be between 1 and 5, got {self.num_replicas}"
            )
