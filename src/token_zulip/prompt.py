from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from string import Template

from .models import NormalizedMessage
from .workspace import TURN_PROMPT_FILE


@dataclass(frozen=True)
class PromptParts:
    recent_context: list[dict[str, object]]
    current_messages: list[NormalizedMessage]


class PromptBuilder:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def build(self, parts: PromptParts) -> str:
        recent = "\n".join(self._format_record(record) for record in parts.recent_context)
        current = "\n".join(self._format_message(message) for message in parts.current_messages)
        template = Template(self._template_text())
        return template.safe_substitute(
            conversation_type=self._conversation_type(parts),
            reply_required=self._reply_required(parts),
            directly_addressed=self._directly_addressed(parts),
            recent_context=recent or "(no recent context)",
            current_messages=current,
        ).rstrip() + "\n"

    def _conversation_type(self, parts: PromptParts) -> str:
        for message in parts.current_messages:
            return message.conversation_type
        for record in parts.recent_context:
            return str(record.get("conversation_type") or "stream")
        return "stream"

    def _reply_required(self, parts: PromptParts) -> str:
        return str(any(message.reply_required for message in parts.current_messages)).lower()

    def _directly_addressed(self, parts: PromptParts) -> str:
        return str(any(message.directly_addressed for message in parts.current_messages)).lower()

    def _format_record(self, record: dict[str, object]) -> str:
        sender = record.get("sender_full_name") or record.get("sender_email") or "unknown"
        message_id = record.get("message_id") or "?"
        content = str(record.get("content") or "").strip()
        return self._with_reactions(f"- [{message_id}] {sender}: {content}", record.get("reactions"))

    def _format_message(self, message: NormalizedMessage) -> str:
        sender = message.sender_full_name or message.sender_email or "unknown"
        return self._with_reactions(f"- [{message.message_id}] {sender}: {message.content.strip()}", message.reactions)

    def _with_reactions(self, line: str, reactions: object) -> str:
        reaction_text = self._format_reactions(reactions)
        if not reaction_text:
            return line
        return f"{line} Reactions: {reaction_text}"

    def _format_reactions(self, reactions: object) -> str:
        if not isinstance(reactions, list):
            return ""
        parts: list[str] = []
        for item in reactions:
            if not isinstance(item, dict):
                continue
            user = str(
                item.get("user_full_name") or item.get("user_email") or item.get("user_key") or "unknown"
            ).strip()
            emoji = str(item.get("emoji_name") or item.get("emoji_code") or "reaction").strip()
            if emoji:
                parts.append(f"{user} {emoji}")
        return ", ".join(parts)

    def _template_text(self) -> str:
        path = self.root / TURN_PROMPT_FILE
        if not path.exists():
            raise FileNotFoundError(f"turn prompt file missing: {path}")
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            raise ValueError(f"turn prompt file is empty: {path}")
        return text
