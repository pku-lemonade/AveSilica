from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .models import (
    ReflectionOperation,
    SessionKey,
    scoped_private_dir,
    scoped_stream_dir,
    utc_now_iso,
)


REFLECTIONS_FILENAME = "REFLECTIONS.md"

_REFLECTIVE_MARKERS = (
    "avoid",
    "candidate",
    "consider",
    "correction",
    "failure",
    "future",
    "hypothesis",
    "lesson",
    "may",
    "might",
    "policy",
    "prefer",
    "risk",
    "seems",
    "should",
    "suggest",
)
_ARCHIVAL_PATTERNS = (
    re.compile(r"^\s*(?:on\s+\d{4}-\d{2}-\d{2},\s*)?the daily .* recommended\b", re.IGNORECASE),
    re.compile(
        r"^\s*(?:[\w .'\-]+?\s+)?(?:asked|reported|confirmed|said|shared|noted|requested)\b",
        re.IGNORECASE,
    ),
)


class ReflectionStore:
    def __init__(self, reflections_dir: Path) -> None:
        self.reflections_dir = reflections_dir.expanduser().resolve()
        self.reflections_dir.mkdir(parents=True, exist_ok=True)

    def apply_ops(
        self,
        session_key: SessionKey,
        ops: list[ReflectionOperation],
        source_message_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        applied: list[dict[str, Any]] = []
        for op in ops:
            scope, directory = self._destination(session_key, op.scope)
            result = self._append(directory, op, scope, source_message_ids or [])
            applied.append(result)
        return applied

    def _destination(self, session_key: SessionKey, requested_scope: str) -> tuple[str, Path]:
        if requested_scope == "global":
            return "global", self.reflections_dir
        if requested_scope != "source":
            raise ValueError(f"invalid reflection scope: {requested_scope!r}")
        if session_key.conversation_type == "private":
            return "private", scoped_private_dir(self.reflections_dir, session_key)
        return "channel", scoped_stream_dir(self.reflections_dir, session_key)

    def _append(
        self,
        directory: Path,
        op: ReflectionOperation,
        resolved_scope: str,
        source_message_ids: list[int],
    ) -> dict[str, Any]:
        content = op.content.strip()
        if self._looks_archival(content):
            return {
                "status": "skipped",
                "reason": "archival summary, not reflection",
                "scope": resolved_scope,
                "content": content,
            }
        path = directory / REFLECTIONS_FILENAME
        entry = self._format_entry(op, resolved_scope, source_message_ids)
        existing = path.read_text(encoding="utf-8") if path.exists() else ""
        text = existing.rstrip()
        next_text = f"{text}\n\n{entry}\n" if text else f"{entry}\n"
        self._write_text_atomic(path, next_text)
        result: dict[str, Any] = {
            "status": "applied",
            "scope": resolved_scope,
            "kind": op.kind,
            "suggested_target": op.suggested_target,
            "content": content,
            "path": str(path.relative_to(self.reflections_dir.parent)),
        }
        if source_message_ids:
            result["source_message_ids"] = source_message_ids
        return result

    def _format_entry(
        self,
        op: ReflectionOperation,
        resolved_scope: str,
        source_message_ids: list[int],
    ) -> str:
        timestamp = utc_now_iso()
        source = ", ".join(str(message_id) for message_id in source_message_ids) or "unknown"
        title = self._title(op.content)
        return "\n".join(
            [
                f"## {timestamp} - {title}",
                "- Status: proposed",
                f"- Scope: {resolved_scope}",
                f"- Source message IDs: {source}",
                f"- Kind: {op.kind}",
                f"- Suggested target: {op.suggested_target}",
                "",
                op.content.strip(),
            ]
        )

    def _title(self, content: str) -> str:
        first = content.strip().splitlines()[0].strip() if content.strip() else "Reflection"
        first = re.sub(r"\s+", " ", first)
        return first[:80].rstrip(" .") or "Reflection"

    def _looks_archival(self, content: str) -> bool:
        lowered = content.casefold()
        if any(marker in lowered for marker in _REFLECTIVE_MARKERS):
            return False
        return any(pattern.search(content) for pattern in _ARCHIVAL_PATTERNS)

    def _write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
