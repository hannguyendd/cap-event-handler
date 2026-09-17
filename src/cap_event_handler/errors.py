"""Exception hierarchy. Every exception the library raises descends from CapError."""


class CapError(Exception):
    """Base class for every error raised by cap_event_handler."""


class ConfigurationError(CapError):
    """Settings or AWS resources are configured in a way that breaks a guarantee."""


class UnroutableMessageError(CapError):
    """A received message names a topic no handler is registered for."""


class PayloadValidationError(CapError):
    """A received message body does not validate against the handler's model."""


class HandlerTransactionError(CapError):
    """A handler committed or closed the session the library owns."""
