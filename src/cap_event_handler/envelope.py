"""The wire envelope: header names, id generation, topic naming, encode/decode.

This module is the only place the `cap-*` header strings appear. Nothing else
in the library may hard-code them.
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any
from uuid import UUID

from cap_event_handler.errors import CapError

if TYPE_CHECKING:
    from collections.abc import Mapping

    from pydantic import BaseModel

HEADER_MSG_ID = "cap-msg-id"
HEADER_MSG_NAME = "cap-msg-name"
HEADER_MSG_TYPE = "cap-msg-type"
HEADER_SENT_TIME = "cap-sent-time"
HEADER_CORR_ID = "cap-corr-id"

# Set on dead-lettered messages by transports that dead-letter in the library.
HEADER_DLQ_REASON = "cap-dlq-reason"
HEADER_DLQ_GROUP = "cap-dlq-group"
HEADER_DLQ_DELIVERIES = "cap-dlq-deliveries"

_SNS_TOPIC_RE = re.compile(r"^[A-Za-z0-9_-]{1,256}$")


if sys.version_info >= (3, 14):  # pragma: no cover - depends on interpreter
    from uuid import uuid7 as _uuid7
else:  # pragma: no cover - depends on interpreter
    from uuid6 import uuid7 as _uuid7


def uuid7() -> UUID:
    """A version-7 UUID: time-ordered, so it indexes well as a primary key."""
    return _uuid7()


def normalize_topic(name: str) -> str:
    """Map an event name onto the character set SNS permits for topic names.

    `.` becomes `-` and `:` becomes `_`, matching CAP's TopicNormalizer. The
    un-normalised name is what travels in `cap-msg-name`; this is only for
    addressing SNS.
    """
    normalized = name.replace(".", "-").replace(":", "_")
    if not _SNS_TOPIC_RE.match(normalized):
        raise CapError(
            f"event name {name!r} cannot be used as an SNS topic: normalised form "
            f"{normalized!r} must match {_SNS_TOPIC_RE.pattern}"
        )
    return normalized


@dataclass(frozen=True, slots=True)
class Envelope:
    """A message as it is stored in the outbox and as it travels over SNS."""

    id: UUID
    name: str
    headers: dict[str, str] = field(compare=False)
    payload: dict[str, Any] = field(default_factory=dict)

    @property
    def sent_time(self) -> datetime:
        return datetime.fromisoformat(self.headers[HEADER_SENT_TIME])

    @property
    def correlation_id(self) -> UUID:
        return UUID(self.headers[HEADER_CORR_ID])

    def body(self) -> str:
        """The JSON text carried in the SNS `Message` field."""
        return json.dumps(self.payload, separators=(",", ":"))

    @classmethod
    def from_transport(cls, headers: Mapping[str, str], body: str) -> Envelope:
        """Rebuild an envelope from what came off the queue.

        Unknown headers are preserved so intermediaries can add their own.
        """
        for required in (HEADER_MSG_ID, HEADER_MSG_NAME, HEADER_SENT_TIME, HEADER_CORR_ID):
            if required not in headers:
                raise CapError(f"received message is missing the {required} header")
        try:
            payload = json.loads(body)
        except json.JSONDecodeError as exc:
            raise CapError(f"received message body is not valid JSON: {exc}") from exc
        if not isinstance(payload, dict):
            raise CapError("received message body must be a JSON object")
        try:
            msg_id = UUID(headers[HEADER_MSG_ID])
        except ValueError as exc:
            raise CapError(f"{HEADER_MSG_ID} is not a UUID: {headers[HEADER_MSG_ID]!r}") from exc
        return cls(
            id=msg_id,
            name=headers[HEADER_MSG_NAME],
            headers=dict(headers),
            payload=payload,
        )


def build_envelope(
    name: str,
    payload: BaseModel,
    *,
    correlation_id: UUID | None = None,
) -> Envelope:
    """Create an envelope for an outgoing event. Assigns the id exactly once."""
    normalize_topic(name)  # fail here, not at send time, if the name is unusable
    msg_id = uuid7()
    model_type = type(payload)
    headers = {
        HEADER_MSG_ID: str(msg_id),
        HEADER_MSG_NAME: name,
        HEADER_MSG_TYPE: f"{model_type.__module__}.{model_type.__qualname__}",
        HEADER_SENT_TIME: datetime.now(UTC).isoformat(),
        HEADER_CORR_ID: str(correlation_id if correlation_id is not None else uuid7()),
    }
    return Envelope(id=msg_id, name=name, headers=headers, payload=payload.model_dump(mode="json"))
