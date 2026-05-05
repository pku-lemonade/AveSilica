from __future__ import annotations

from dataclasses import dataclass, field
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
        )

    @property
    def sender_label(self) -> str:
        return self.sender_full_name or self.sender_email or "unknown"


@dataclass(frozen=True)
class ConversationContext:
    kind: str = "stream"
    stream_id: int | None = None
    stream: str = ""
    topic: str = ""
    topic_hash: str = ""
    private_recipient_key: str | None = None

    @classmethod
    def from_messages(cls, messages: Sequence[NormalizedMessage]) -> ConversationContext:
        first = messages[0] if messages else None
        return cls(
            kind=first.conversation_type if first else "stream",
            stream_id=first.stream_id if first else None,
            stream=first.stream if first else "",
            topic=first.topic if first else "",
            topic_hash=first.topic_hash if first else "",
            private_recipient_key=first.private_recipient_key if first else None,
        )


@dataclass(frozen=True)
class WorkflowDeltas:
    reflection_context: str = ""
    posted_bot_updates: str = ""
    scheduling_context: str = ""
    current_schedules: str = ""
    mentionable_participants: str = ""
    skill_availability: str = ""
    same_turn_skill_changes: str = ""
    applied_changes: str = ""

    def sections_for_role(self, role: str) -> list[str]:
        if role == "reflections":
            return [self.reflection_context]
        if role == "skill":
            return [self.skill_availability]
        if role == "schedule":
            return [
                self.scheduling_context,
                self.current_schedules,
                self.mentionable_participants,
                self.skill_availability,
                self.same_turn_skill_changes,
            ]
        if role == "reply":
            return [self.posted_bot_updates, self.applied_changes]
        raise ValueError(f"unknown prompt role: {role}")


@dataclass(frozen=True)
class RenderContext:
    message_timezone: str | None = None


@dataclass(frozen=True)
class TurnContext:
    messages: list[TurnMessage]
    conversation: ConversationContext
    deltas: WorkflowDeltas = field(default_factory=WorkflowDeltas)
    render: RenderContext = field(default_factory=RenderContext)

    @classmethod
    def from_messages(
        cls,
        messages: Sequence[NormalizedMessage],
        *,
        deltas: WorkflowDeltas | None = None,
        render: RenderContext | None = None,
    ) -> TurnContext:
        return cls(
            messages=[TurnMessage.from_message(message) for message in messages],
            conversation=ConversationContext.from_messages(messages),
            deltas=deltas or WorkflowDeltas(),
            render=render or RenderContext(),
        )
