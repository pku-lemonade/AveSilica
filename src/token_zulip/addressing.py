from __future__ import annotations

import html
import re
from typing import Any, Sequence


def is_directly_addressed(
    event: dict[str, Any],
    message: dict[str, Any],
    text: str,
    *,
    bot_user_id: int | None,
    bot_aliases: Sequence[str],
) -> bool:
    if _has_direct_mention_flag(event, message):
        return True
    content_html = str(message.get("content") or "")
    if bot_user_id is not None and _mentions_user_id(content_html, bot_user_id):
        return True
    return alias_is_directly_addressed(text, bot_aliases)


def alias_is_directly_addressed(text: str, aliases: Sequence[str]) -> bool:
    normalized = _normalize_alias_text(text)
    for alias in aliases:
        normalized_alias = alias.strip().casefold()
        if not normalized_alias:
            continue
        pattern = rf"(?<![a-z0-9_])@?\s*{re.escape(normalized_alias)}(?![a-z0-9_])"
        if re.search(pattern, normalized):
            return True
    return False


def _has_direct_mention_flag(event: dict[str, Any], message: dict[str, Any]) -> bool:
    flags: list[str] = []
    for source in [message.get("flags"), event.get("flags")]:
        if isinstance(source, list):
            flags.extend(str(item) for item in source)
    return "mentioned" in flags


def _mentions_user_id(content_html: str, bot_user_id: int) -> bool:
    return re.search(rf"data-user-id=[\"']{bot_user_id}[\"']", content_html) is not None


def _normalize_alias_text(text: str) -> str:
    value = html.unescape(text or "")
    value = re.sub(r"@\*\*([^*]+)\*\*", r"@\1", value)
    value = re.sub(r"\s+", " ", value)
    return value.casefold()
