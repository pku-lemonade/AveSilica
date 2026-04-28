from __future__ import annotations

from dataclasses import dataclass

from .models import NormalizedMessage


@dataclass(frozen=True)
class PromptParts:
    recent_context: list[dict[str, object]]
    current_messages: list[NormalizedMessage]


class PromptBuilder:
    def build(self, parts: PromptParts) -> str:
        recent = "\n".join(self._format_record(record) for record in parts.recent_context)
        current = "\n".join(self._format_message(message) for message in parts.current_messages)
        conversation_type = self._conversation_type(parts)
        frame = self._conversation_frame(conversation_type)
        return f"""Zulip conversation update.

{frame}

# Recent Zulip Context

{recent or "(no recent context)"}

# New Zulip Message(s)

{current}
"""

    def _conversation_type(self, parts: PromptParts) -> str:
        for message in parts.current_messages:
            return message.conversation_type
        for record in parts.recent_context:
            return str(record.get("conversation_type") or "stream")
        return "stream"

    def _conversation_frame(self, conversation_type: str) -> str:
        if conversation_type == "private":
            return "This is a one-on-one private Zulip conversation. A concise direct reply is required."
        return "This is a public Zulip stream/topic conversation."

    def _format_record(self, record: dict[str, object]) -> str:
        sender = record.get("sender_full_name") or record.get("sender_email") or "unknown"
        message_id = record.get("message_id") or "?"
        content = str(record.get("content") or "").strip()
        return f"- [{message_id}] {sender}: {content}"

    def _format_message(self, message: NormalizedMessage) -> str:
        sender = message.sender_full_name or message.sender_email or "unknown"
        return f"- [{message.message_id}] {sender}: {message.content.strip()}"
