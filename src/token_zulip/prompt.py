from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from string import Template

from .models import NormalizedMessage
from .workspace import REPLY_TURN_PROMPT_FILE


@dataclass(frozen=True)
class PromptParts:
    current_messages: list[NormalizedMessage]
    injected_context: str = ""


class PromptBuilder:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def build(self, parts: PromptParts, *, template_file: str = REPLY_TURN_PROMPT_FILE) -> str:
        current = "\n".join(self._format_message(message) for message in parts.current_messages)
        template = Template(self._template_text(template_file))
        return template.safe_substitute(
            conversation_type=self._conversation_type(parts),
            reply_required=self._reply_required(parts),
            directly_addressed=self._directly_addressed(parts),
            injected_context=parts.injected_context.strip(),
            current_messages=current,
        ).rstrip() + "\n"

    def render_template(self, template_file: str, values: dict[str, object]) -> str:
        template = Template(self._template_text(template_file))
        substitutions = {key: str(value).strip() for key, value in values.items()}
        return template.safe_substitute(substitutions).rstrip() + "\n"

    def _conversation_type(self, parts: PromptParts) -> str:
        for message in parts.current_messages:
            return message.conversation_type
        return "stream"

    def _reply_required(self, parts: PromptParts) -> str:
        return str(any(message.reply_required for message in parts.current_messages)).lower()

    def _directly_addressed(self, parts: PromptParts) -> str:
        return str(any(message.directly_addressed for message in parts.current_messages)).lower()

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

    def _template_text(self, template_file: str) -> str:
        path = self.root / template_file
        if not path.exists():
            raise FileNotFoundError(f"prompt template file missing: {path}")
        text = path.read_text(encoding="utf-8")
        if not text.strip():
            raise ValueError(f"prompt template file is empty: {path}")
        return text
