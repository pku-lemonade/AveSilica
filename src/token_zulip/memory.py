from __future__ import annotations

from pathlib import Path
from typing import Any

from .models import (
    MemoryOperation,
    SessionKey,
    scoped_conversation_dir,
    scoped_stream_dir,
)


MEMORY_FILENAME = "MEMORY.md"
ENTRY_DELIMITER = "\n§\n"
DEFAULT_MEMORY_CHAR_LIMIT = 4_000


class MemoryStore:
    def __init__(self, memory_dir: Path, char_limit: int = DEFAULT_MEMORY_CHAR_LIMIT) -> None:
        self.memory_dir = memory_dir.expanduser().resolve()
        self.char_limit = char_limit
        self.ensure_files()

    @property
    def memory_path(self) -> Path:
        return self.memory_dir / MEMORY_FILENAME

    def ensure_files(self) -> None:
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        if not self.memory_path.exists():
            self.memory_path.write_text("", encoding="utf-8")

    def render_selected(self, session_key: SessionKey) -> str:
        sections: list[str] = []
        for label, directory in self._read_scope_dirs(session_key):
            text = self._read_memory_text(directory)
            if text:
                sections.append(f"## {label} memory\n\n{text}")
        return "\n\n".join(sections)

    def apply_ops(
        self,
        session_key: SessionKey,
        ops: list[MemoryOperation],
        source_message_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_files()
        applied: list[dict[str, Any]] = []
        for op in ops:
            scope = self._scope_for_op(session_key, op.scope)
            directory = self._dir_for_scope(session_key, scope)
            result = self._apply_op(directory, op)
            result["scope"] = scope
            result["path"] = str(
                (directory / MEMORY_FILENAME).relative_to(self.memory_dir.parent)
            )
            if source_message_ids:
                result["source_message_ids"] = source_message_ids
            applied.append(result)
        return applied

    def _apply_op(self, directory: Path, op: MemoryOperation) -> dict[str, Any]:
        path = directory / MEMORY_FILENAME
        entries = self._read_entries(path)
        if op.op == "add":
            return self._add_entry(path, entries, op.content)
        if op.op == "replace":
            return self._replace_entry(path, entries, op.old_text, op.content)
        if op.op == "remove":
            return self._remove_entry(path, entries, op.old_text)
        return {"op": op.op, "status": "rejected", "reason": "unsupported memory operation"}

    def _add_entry(self, path: Path, entries: list[str], content: str) -> dict[str, Any]:
        content = content.strip()
        if content in entries:
            return {
                "op": "add",
                "status": "skipped",
                "reason": "duplicate",
                "content": content,
            }
        next_entries = [*entries, content]
        error = self._limit_error(next_entries)
        if error:
            return {"op": "add", "status": "rejected", "reason": error, "content": content}
        self._write_entries(path, next_entries)
        return {"op": "add", "status": "applied", "content": content}

    def _replace_entry(self, path: Path, entries: list[str], old_text: str, content: str) -> dict[str, Any]:
        old_text = old_text.strip()
        content = content.strip()
        matches = self._matching_indexes(entries, old_text)
        if not matches:
            return {
                "op": "replace",
                "status": "rejected",
                "reason": "old_text not found",
                "old_text": old_text,
            }
        if len(matches) > 1:
            return {
                "op": "replace",
                "status": "rejected",
                "reason": "old_text matched multiple entries",
                "old_text": old_text,
            }
        next_entries = entries[:]
        next_entries[matches[0]] = content
        next_entries = self._dedupe_entries(next_entries)
        error = self._limit_error(next_entries)
        if error:
            return {
                "op": "replace",
                "status": "rejected",
                "reason": error,
                "old_text": old_text,
                "content": content,
            }
        self._write_entries(path, next_entries)
        return {"op": "replace", "status": "applied", "old_text": old_text, "content": content}

    def _remove_entry(self, path: Path, entries: list[str], old_text: str) -> dict[str, Any]:
        old_text = old_text.strip()
        matches = self._matching_indexes(entries, old_text)
        if not matches:
            return {
                "op": "remove",
                "status": "rejected",
                "reason": "old_text not found",
                "old_text": old_text,
            }
        if len(matches) > 1:
            return {
                "op": "remove",
                "status": "rejected",
                "reason": "old_text matched multiple entries",
                "old_text": old_text,
            }
        removed = entries[matches[0]]
        next_entries = [entry for index, entry in enumerate(entries) if index != matches[0]]
        self._write_entries(path, next_entries)
        return {"op": "remove", "status": "applied", "old_text": old_text, "content": removed}

    def _read_scope_dirs(self, session_key: SessionKey) -> list[tuple[str, Path]]:
        scopes: list[tuple[str, Path]] = [("global", self.memory_dir)]
        if session_key.conversation_type == "private":
            scopes.append(("private", self._private_dir(session_key)))
            return scopes
        scopes.append(("channel", self._stream_dir(session_key)))
        scopes.append(("conversation", self._topic_dir(session_key)))
        return scopes

    def _scope_for_op(self, session_key: SessionKey, scope: str) -> str:
        if scope == "global":
            return "global"
        if scope == "channel" and session_key.conversation_type != "private":
            return "channel"
        return "conversation"

    def _dir_for_scope(self, session_key: SessionKey, scope: str) -> Path:
        if scope == "global":
            return self.memory_dir
        if scope == "channel":
            return self._stream_dir(session_key)
        if session_key.conversation_type == "private":
            return self._private_dir(session_key)
        return self._topic_dir(session_key)

    def _stream_dir(self, session_key: SessionKey) -> Path:
        return scoped_stream_dir(self.memory_dir, session_key)

    def _topic_dir(self, session_key: SessionKey) -> Path:
        return scoped_conversation_dir(self.memory_dir, session_key)

    def _private_dir(self, session_key: SessionKey) -> Path:
        return scoped_conversation_dir(self.memory_dir, session_key)

    def _read_memory_text(self, directory: Path) -> str:
        path = directory / MEMORY_FILENAME
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def _read_entries(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        raw = path.read_text(encoding="utf-8").strip()
        if not raw:
            return []
        return [entry.strip() for entry in raw.split(ENTRY_DELIMITER) if entry.strip()]

    def _write_entries(self, path: Path, entries: list[str]) -> None:
        text = ENTRY_DELIMITER.join(entry.strip() for entry in entries if entry.strip())
        self._write_text_atomic(path, text + ("\n" if text else ""))

    def _matching_indexes(self, entries: list[str], old_text: str) -> list[int]:
        return [index for index, entry in enumerate(entries) if old_text and old_text in entry]

    def _limit_error(self, entries: list[str]) -> str | None:
        size = len(ENTRY_DELIMITER.join(entries))
        if size <= self.char_limit:
            return None
        return f"memory file would exceed {self.char_limit} chars ({size} chars)"

    def _dedupe_entries(self, entries: list[str]) -> list[str]:
        deduped: list[str] = []
        seen: set[str] = set()
        for entry in entries:
            normalized = entry.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            deduped.append(normalized)
        return deduped

    def _write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
