from __future__ import annotations

from contextlib import contextmanager
import re
import threading
from pathlib import Path
from typing import Any, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX fallback
    fcntl = None

from .models import (
    ReflectionOperation,
    SessionKey,
    scoped_private_dir,
    scoped_stream_dir,
    utc_now_iso,
)


REFLECTIONS_FILENAME = "REFLECTIONS.md"
_REFLECTION_APPEND_LOCKS: dict[Path, threading.Lock] = {}
_REFLECTION_APPEND_LOCKS_GUARD = threading.Lock()

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
        self._append_entry(path, entry)
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

    def _append_entry(self, path: Path, entry: str) -> None:
        with _locked_reflection_file(path):
            with path.open("a+", encoding="utf-8") as file:
                file.seek(0, 2)
                if file.tell() > 0:
                    file.write("\n\n")
                file.write(entry)
                file.write("\n")
                file.flush()


def _lock_for(path: Path) -> threading.Lock:
    with _REFLECTION_APPEND_LOCKS_GUARD:
        lock = _REFLECTION_APPEND_LOCKS.get(path)
        if lock is None:
            lock = threading.Lock()
            _REFLECTION_APPEND_LOCKS[path] = lock
        return lock


@contextmanager
def _locked_reflection_file(path: Path) -> Iterator[None]:
    path.parent.mkdir(parents=True, exist_ok=True)
    lock = _lock_for(path)
    with lock:
        if fcntl is None:
            yield
            return

        lock_path = path.with_suffix(path.suffix + ".lock")
        with lock_path.open("a+", encoding="utf-8") as lock_file:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
