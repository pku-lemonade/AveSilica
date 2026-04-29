from __future__ import annotations

import html
import re
from dataclasses import dataclass
from typing import Literal, Sequence, cast

from .models import NormalizedMessage


ControlCommandName = Literal["clear", "status"]
MENTION_RE = re.compile(r"@_?\*\*([^*|\n]+?)(?:\|\d+)?\*\*")
CONTROL_COMMANDS: set[ControlCommandName] = {"clear", "status"}
TRAILING_PUNCTUATION_RE = re.compile(r"[.!?]*\s*$")


@dataclass(frozen=True)
class ControlCommand:
    name: ControlCommandName


def parse_control_command(message: NormalizedMessage, aliases: Sequence[str]) -> ControlCommand | None:
    text = _normalize_control_text(message.content)
    if not text:
        return None

    command = _bare_command(text)
    if command is not None and (message.conversation_type == "private" or message.directly_addressed):
        return ControlCommand(command)

    command = _prefixed_command(text, aliases)
    if command is not None:
        return ControlCommand(command)

    return None


def _normalize_control_text(value: str) -> str:
    text = html.unescape(value or "")
    text = MENTION_RE.sub(lambda match: "@" + match.group(1).strip(), text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().casefold()


def _bare_command(text: str) -> ControlCommandName | None:
    normalized = TRAILING_PUNCTUATION_RE.sub("", text).strip()
    return cast(ControlCommandName, normalized) if normalized in CONTROL_COMMANDS else None


def _prefixed_command(text: str, aliases: Sequence[str]) -> ControlCommandName | None:
    for alias in aliases:
        normalized_alias = alias.strip().casefold()
        if not normalized_alias:
            continue
        pattern = re.compile(
            rf"^@?\s*{re.escape(normalized_alias)}(?:[\s:,;-]+)"
            rf"(?P<command>clear|status)\s*[.!?]*$"
        )
        match = pattern.match(text)
        if match:
            return cast(ControlCommandName, match.group("command"))
    return None
