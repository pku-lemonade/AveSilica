from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from string import Template
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .turn_context import TurnContext, TurnMessage
from .workspace import REPLY_TURN_USER_PROMPT_FILE


class PromptBuilder:
    def __init__(self, root: Path) -> None:
        self.root = root.expanduser().resolve()

    def build(self, turn: TurnContext, *, role: str, template_file: str = REPLY_TURN_USER_PROMPT_FILE) -> str:
        current = "\n".join(
            self._format_message(message, timezone_name=turn.render.message_timezone) for message in turn.messages
        )
        template = Template(self._template_text(template_file))
        return template.safe_substitute(
            workflow_context=self.render_sections(turn.deltas.sections_for_role(role)),
            current_messages=current,
        ).rstrip() + "\n"

    def render_template(self, template_file: str, values: dict[str, object]) -> str:
        template = Template(self._template_text(template_file))
        substitutions = {key: str(value).strip() for key, value in values.items()}
        return template.safe_substitute(substitutions).rstrip() + "\n"

    def render_sections(self, sections: list[str]) -> str:
        return "\n\n".join(section.strip() for section in sections if section.strip())

    def render_section(self, title: str, content: str, *, intro: str = "") -> str:
        body = content.strip()
        if not body:
            return ""
        parts = [f"# {title}", ""]
        if intro.strip():
            parts.extend([intro.strip(), ""])
        parts.append(body)
        return "\n".join(parts)

    def _format_message(self, message: TurnMessage, *, timezone_name: str | None = None) -> str:
        timestamp = self._format_message_time(message, timezone_name=timezone_name)
        prefix = f"- [{message.message_id}]"
        if timestamp:
            prefix = f"{prefix} {timestamp}"
        return self._with_reactions(f"{prefix} {message.sender_label}: {message.content.strip()}", message.reactions)

    def _format_message_time(self, message: TurnMessage, *, timezone_name: str | None) -> str:
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

    def _message_datetime(self, message: TurnMessage) -> datetime | None:
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
