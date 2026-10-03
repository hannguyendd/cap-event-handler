"""Transactional outbox/inbox messaging for PostgreSQL and Amazon SNS/SQS."""

from cap_event_handler.app import CapApp
from cap_event_handler.config import NatsConfig, Settings, SnsSqsConfig
from cap_event_handler.context import Context
from cap_event_handler.errors import (
    CapError,
    ConfigurationError,
    HandlerTransactionError,
    PayloadValidationError,
    UnroutableMessageError,
)
from cap_event_handler.transport.snssqs import SnsSqs

__version__ = "0.1.0"

__all__ = [
    "CapApp",
    "CapError",
    "ConfigurationError",
    "Context",
    "HandlerTransactionError",
    "NatsConfig",
    "PayloadValidationError",
    "Settings",
    "SnsSqs",
    "SnsSqsConfig",
    "UnroutableMessageError",
    "__version__",
]
