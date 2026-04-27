from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from .models import (
    MEMORY_STATUSES,
    MemoryItem,
    MemoryOperation,
    SessionKey,
    normalize_memory_content,
    private_memory_dir_name,
    stream_memory_dir_name,
    topic_memory_dir_name,
    utc_now_iso,
)


OMITTED_RENDER_STATUSES = {"answered", "archived", "done"}
SEEDS_FILENAME = "seeds.jsonl"
MEMORY_FILENAME = "MEMORY.md"


class MemoryStore:
    def __init__(self, memory_dir: Path) -> None:
        self.memory_dir = memory_dir.expanduser().resolve()
        self.ensure_files()

    @property
    def seeds_path(self) -> Path:
        return self.memory_dir / SEEDS_FILENAME

    @property
    def memory_path(self) -> Path:
        return self.memory_dir / MEMORY_FILENAME

    def ensure_files(self) -> None:
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        if not self.seeds_path.exists():
            self.seeds_path.write_text("", encoding="utf-8")
        if not self.memory_path.exists():
            self.memory_path.write_text("", encoding="utf-8")

    def render_selected(self, session_key: SessionKey) -> str:
        sections: list[str] = []
        for label, directory in self._read_scope_dirs(session_key):
            self._ensure_memory_file(directory)
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
        source_ids = source_message_ids or []
        applied: list[dict[str, Any]] = []
        touched_dirs: set[Path] = set()

        for op in ops:
            if op.op == "archive":
                archived = self._archive_seed(session_key, op)
                if archived is None:
                    continue
                item, directory = archived
                touched_dirs.add(directory)
                applied.append({"op": "archive", "id": item.id, "status": item.status})
                continue

            scope = self._scope_for_op(session_key, op)
            directory = self._dir_for_scope(session_key, scope)
            items = self._read_seed_file(directory / SEEDS_FILENAME)
            content = op.content.strip()
            if not content:
                continue
            item_id = (
                op.id
                or self._find_duplicate_id(items, scope, op.kind, content)
                or self._item_id(scope, op.kind, content)
            )
            existing = self._find_item(items, item_id)
            now = utc_now_iso()
            if existing is None:
                item = MemoryItem(
                    id=item_id,
                    scope=scope,
                    kind=op.kind,
                    status=op.status,
                    content=content,
                    source_session_id=session_key.value,
                    source_message_ids=source_ids,
                    created_at=now,
                    updated_at=now,
                )
                items.append(item)
            else:
                item = replace(
                    existing,
                    scope=scope,
                    kind=op.kind,
                    status=op.status if op.status in MEMORY_STATUSES else existing.status,
                    content=content,
                    source_session_id=session_key.value,
                    source_message_ids=source_ids,
                    updated_at=now,
                )
                self._replace_item(items, item)
            self._write_seed_file(directory / SEEDS_FILENAME, items)
            touched_dirs.add(directory)
            applied.append({"op": "upsert", **item.to_record()})

        for directory in touched_dirs:
            self._write_memory_file(directory, self._read_seed_file(directory / SEEDS_FILENAME))
        return applied

    def _read_scope_dirs(self, session_key: SessionKey) -> list[tuple[str, Path]]:
        scopes: list[tuple[str, Path]] = [("global", self.memory_dir)]
        if session_key.conversation_type == "private":
            scopes.append(("private", self._private_dir(session_key)))
            return scopes
        scopes.append(("channel", self._stream_dir(session_key)))
        scopes.append(("conversation", self._topic_dir(session_key)))
        return scopes

    def _scope_for_op(self, session_key: SessionKey, op: MemoryOperation) -> str:
        if op.scope == "global":
            return "global"
        if op.scope == "channel":
            if session_key.conversation_type == "private":
                return self._conversation_scope(session_key)
            return self._channel_scope(session_key)
        return self._conversation_scope(session_key)

    def _dir_for_scope(self, session_key: SessionKey, scope: str) -> Path:
        if scope == "global":
            return self.memory_dir
        if session_key.conversation_type != "private" and scope == self._channel_scope(session_key):
            return self._stream_dir(session_key)
        if scope == self._conversation_scope(session_key):
            if session_key.conversation_type == "private":
                return self._private_dir(session_key)
            return self._topic_dir(session_key)
        raise ValueError(f"unable to map memory scope to path: {scope!r}")

    def _archive_seed(self, session_key: SessionKey, op: MemoryOperation) -> tuple[MemoryItem, Path] | None:
        if not op.id:
            return None
        for _, directory in self._read_scope_dirs(session_key):
            items = self._read_seed_file(directory / SEEDS_FILENAME)
            item = self._find_item(items, op.id)
            if item is None:
                continue
            updated = replace(item, status="archived", updated_at=utc_now_iso())
            self._replace_item(items, updated)
            self._write_seed_file(directory / SEEDS_FILENAME, items)
            return updated, directory

        return None

    def _channel_scope(self, session_key: SessionKey) -> str:
        if session_key.conversation_type == "private":
            raise ValueError("private sessions do not have channel memory")
        return f"channel:zulip:{session_key.realm_id}:stream:{session_key.stream_id}"

    def _conversation_scope(self, session_key: SessionKey) -> str:
        return f"conversation:{session_key.value}"

    def _stream_dir(self, session_key: SessionKey) -> Path:
        return self.memory_dir / stream_memory_dir_name(session_key.stream_id, session_key.stream_slug)

    def _topic_dir(self, session_key: SessionKey) -> Path:
        return self._stream_dir(session_key) / topic_memory_dir_name(session_key.topic_hash)

    def _private_dir(self, session_key: SessionKey) -> Path:
        return self.memory_dir / private_memory_dir_name(session_key.private_user_key or session_key.topic_hash)

    def _item_id(self, scope: str, kind: str, content: str) -> str:
        digest = hashlib.sha256(f"{scope}\0{kind}\0{normalize_memory_content(content)}".encode("utf-8")).hexdigest()
        return f"mem_{digest[:16]}"

    def _find_duplicate_id(self, items: list[MemoryItem], scope: str, kind: str, content: str) -> str | None:
        normalized = normalize_memory_content(content)
        for item in items:
            if item.scope == scope and item.kind == kind and normalize_memory_content(item.content) == normalized:
                return item.id
        return None

    def _find_item(self, items: list[MemoryItem], item_id: str | None) -> MemoryItem | None:
        if not item_id:
            return None
        for item in items:
            if item.id == item_id:
                return item
        return None

    def _replace_item(self, items: list[MemoryItem], updated: MemoryItem) -> None:
        for index, item in enumerate(items):
            if item.id == updated.id:
                items[index] = updated
                return
        items.append(updated)

    def _read_memory_text(self, directory: Path) -> str:
        path = directory / MEMORY_FILENAME
        if not path.exists():
            return ""
        return path.read_text(encoding="utf-8").strip()

    def _ensure_memory_file(self, directory: Path) -> None:
        if self._read_memory_text(directory):
            return
        items = self._read_seed_file(directory / SEEDS_FILENAME)
        if any(item.status not in OMITTED_RENDER_STATUSES for item in items):
            self._write_memory_file(directory, items)

    def _write_memory_file(self, directory: Path, items: list[MemoryItem]) -> None:
        active = sorted(
            [item for item in items if item.status not in OMITTED_RENDER_STATUSES],
            key=lambda value: (value.kind, value.id),
        )
        lines: list[str] = []
        seen: set[tuple[str, str]] = set()
        for item in active:
            key = (item.kind, normalize_memory_content(item.content))
            if key in seen:
                continue
            seen.add(key)
            lines.append(f"- {item.kind} [{item.id}]: {item.content}")
        self._write_text_atomic(directory / MEMORY_FILENAME, "\n".join(lines) + ("\n" if lines else ""))

    def _read_seed_file(self, path: Path) -> list[MemoryItem]:
        if not path.exists():
            return []
        items: list[MemoryItem] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            try:
                items.append(MemoryItem.from_record(record))
            except (KeyError, TypeError, ValueError):
                continue
        return items

    def _write_seed_file(self, path: Path, items: list[MemoryItem]) -> None:
        records = [
            item.to_record()
            for item in sorted(items, key=lambda value: (value.scope, value.kind, value.id))
        ]
        text = "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records)
        self._write_text_atomic(path, text)

    def _write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
