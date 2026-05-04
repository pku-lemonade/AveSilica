from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Sequence

from .models import NormalizedMessage


@dataclass(frozen=True)
class TurnMessage:
    message_id: int
    sender_email: str
    sender_full_name: str
    content: str
    timestamp: int | None
    received_at: str
    reactions: list[dict[str, Any]]
    conversation_type: str
    reply_required: bool
    directly_addressed: bool

    @classmethod
    def from_message(cls, message: NormalizedMessage) -> TurnMessage:
        return cls(
            message_id=message.message_id,
            sender_email=message.sender_email,
            sender_full_name=message.sender_full_name,
            content=message.content,
            timestamp=message.timestamp,
            received_at=message.received_at,
            reactions=message.reactions,
            conversation_type=message.conversation_type,
            reply_required=message.reply_required,
            directly_addressed=message.directly_addressed,
        )

    @property
    def sender_label(self) -> str:
        return self.sender_full_name or self.sender_email or "unknown"


@dataclass(frozen=True)
class ConversationContext:
    conversation_type: str = "stream"
    stream_id: int | None = None
    stream: str = ""
    topic: str = ""
    topic_hash: str = ""
    private_recipient_key: str | None = None
    reply_required: bool = False
    directly_addressed: bool = False

    @classmethod
    def from_messages(cls, messages: Sequence[NormalizedMessage]) -> ConversationContext:
        first = messages[0] if messages else None
        return cls(
            conversation_type=first.conversation_type if first else "stream",
            stream_id=first.stream_id if first else None,
            stream=first.stream if first else "",
            topic=first.topic if first else "",
            topic_hash=first.topic_hash if first else "",
            private_recipient_key=first.private_recipient_key if first else None,
            reply_required=any(message.reply_required for message in messages),
            directly_addressed=any(message.directly_addressed for message in messages),
        )


@dataclass(frozen=True)
class TurnContext:
    messages: list[TurnMessage]
    conversation: ConversationContext
    runtime_context: str = ""
    message_timezone: str | None = None

    @classmethod
    def from_messages(
        cls,
        messages: Sequence[NormalizedMessage],
        *,
        runtime_context: str = "",
        message_timezone: str | None = None,
    ) -> TurnContext:
        return cls(
            messages=[TurnMessage.from_message(message) for message in messages],
            conversation=ConversationContext.from_messages(messages),
            runtime_context=runtime_context,
            message_timezone=message_timezone,
        )
