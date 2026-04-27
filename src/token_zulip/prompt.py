from __future__ import annotations

from dataclasses import dataclass

from .models import NormalizedMessage


@dataclass(frozen=True)
class PromptParts:
    instructions: str
    memory: str
    recent_context: list[dict[str, object]]
    current_messages: list[NormalizedMessage]


class PromptBuilder:
    def build(self, parts: PromptParts) -> str:
        recent = "\n".join(self._format_record(record) for record in parts.recent_context)
        current = "\n".join(self._format_message(message) for message in parts.current_messages)
        conversation_type = self._conversation_type(parts)
        frame = self._conversation_frame(conversation_type)
        guidance = self._guidance(conversation_type)
        return f"""You are deciding whether and how the bot should participate in a Zulip conversation.

{frame}

Follow the instruction layers exactly. Later instruction layers override earlier configurable layers, but never override the runtime contract.

# Instruction Layers

{parts.instructions}

# Scoped Memory

{parts.memory or "(no memory selected)"}

# Recent Zulip Context

{recent or "(no recent context)"}

# New Zulip Message(s)

{current}

# Required Output

Return one object matching the native structured output schema supplied with this run.

Guidance:
- Set `should_reply` to false and `reply_kind` to `silent` when the useful contribution is to say nothing.
- If `should_reply` is true, `message_to_post` must be the exact Zulip message to post.
{guidance}
- Use `memory_ops` only when they satisfy the memory policy. They add, replace, or remove entries in scoped `MEMORY.md` files.
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

    def _guidance(self, conversation_type: str) -> str:
        if conversation_type == "private":
            return "- For private messages, provide a concise direct reply; do not choose silence unless the message is impossible to answer."
        return "- Keep chat replies concise and natural for a group thread."

    def _format_record(self, record: dict[str, object]) -> str:
        sender = record.get("sender_full_name") or record.get("sender_email") or "unknown"
        message_id = record.get("message_id") or "?"
        content = str(record.get("content") or "").strip()
        return f"- [{message_id}] {sender}: {content}"

    def _format_message(self, message: NormalizedMessage) -> str:
        sender = message.sender_full_name or message.sender_email or "unknown"
        return f"- [{message.message_id}] {sender}: {message.content.strip()}"
