from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from string import Template
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .models import NormalizedMessage
from .workspace import REPLY_TURN_USER_PROMPT_FILE


@dataclass(frozen=True)
class PromptParts:
    current_messages: list[NormalizedMessage]
    injected_context: str = ""
    message_timezone: str | None = None


class PromptBuilder:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def build(self, parts: PromptParts, *, template_file: str = REPLY_TURN_USER_PROMPT_FILE) -> str:
        current = "\n".join(
            self._format_message(message, timezone_name=parts.message_timezone) for message in parts.current_messages
        )
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

    def _format_message(self, message: NormalizedMessage, *, timezone_name: str | None = None) -> str:
        sender = message.sender_full_name or message.sender_email or "unknown"
        timestamp = self._format_message_time(message, timezone_name=timezone_name)
        prefix = f"- [{message.message_id}]"
        if timestamp:
            prefix = f"{prefix} {timestamp}"
        return self._with_reactions(f"{prefix} {sender}: {message.content.strip()}", message.reactions)

    def _format_message_time(self, message: NormalizedMessage, *, timezone_name: str | None) -> str:
        if not timezone_name:
            return ""
        dt = self._message_datetime(message)
        if dt is None:
            return ""
        try:
            tz = ZoneInfo(timezone_name)
        except ZoneInfoNotFoundError:
            tz = timezone.utc
        return dt.astimezone(tz).isoformat(timespec="seconds")

    def _message_datetime(self, message: NormalizedMessage) -> datetime | None:
        if message.timestamp is not None:
            try:
                return datetime.fromtimestamp(int(message.timestamp), timezone.utc)
            except (OverflowError, OSError, TypeError, ValueError):
                pass
        text = message.received_at.strip()
        if not text:
            return None
        try:
            dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt

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
