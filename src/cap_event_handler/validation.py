"""Startup checks for configurations that silently weaken a guarantee.

Each check here corresponds to a way the system keeps working while quietly
losing a property someone is relying on. Fatal checks are the ones where no
correct behaviour is possible; the rest warn.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from cap_event_handler.errors import ConfigurationError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from cap_event_handler.config import Settings
    from cap_event_handler.registry import HandlerRegistry
    from cap_event_handler.transport.base import Transport


def validate_settings(settings: Settings) -> None:
    """Checks that need nothing but the settings. Called at construction."""
    lookback = settings.sweeper_lookback.total_seconds()
    timeout = settings.publish_timeout.total_seconds()
    if lookback <= timeout:
        raise ConfigurationError(
            f"sweeper_lookback ({lookback}s) must exceed publish_timeout ({timeout}s), "
            "or the sweeper will re-send messages that are still in flight"
        )


async def validate_runtime(
    *,
    settings: Settings,
    transport: Transport,
    queue_id: str,
    registry: HandlerRegistry,
    topic_ids: Mapping[str, str],
) -> list[str]:
    """Checks that need the broker. Returns warnings; raises on fatal problems."""
    warnings: list[str] = []
    attrs = await transport.queue_attributes(queue_id)

    inbox_seconds = settings.inbox_retention.total_seconds()
    if inbox_seconds <= attrs.message_retention:
        warnings.append(
            f"inbox_retention ({inbox_seconds:.0f}s) does not exceed the queue's message "
            f"retention ({attrs.message_retention}s; SQS MessageRetentionPeriod, NATS stream "
            "max_age): a message redelivered after its deduplication marker expires will be "
            "handled twice"
        )

    if attrs.visibility_timeout < settings.expected_max_handler_seconds:
        warnings.append(
            f"queue VisibilityTimeout ({attrs.visibility_timeout}s; ack_wait on NATS) is below "
            f"expected_max_handler_seconds ({settings.expected_max_handler_seconds}s): "
            "slow handlers will be redelivered while still running"
        )

    if not attrs.has_redrive:
        warnings.append(
            "queue has no redrive policy (on NATS: no dead-letter stream, or a consumer "
            "max_deliver below NatsConfig.max_deliver): a message that always fails will be "
            "retried forever or dropped instead of being dead-lettered"
        )

    if not settings.provision:
        missing = sorted(registry.topics() - set(topic_ids))
        if missing:
            raise ConfigurationError(
                "provision=False but no topic is configured for: " + ", ".join(missing)
            )
        unsubscribed = sorted(
            name for name in registry.topics() if topic_ids[name] not in attrs.subscribed_topics
        )
        if unsubscribed:
            raise ConfigurationError(
                "provision=False and the queue is not subscribed to the topics for: "
                + ", ".join(unsubscribed)
                + " -- these handlers can never fire"
            )

    return warnings
