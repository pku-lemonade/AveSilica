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
    utc_now_iso,
)


OMITTED_RENDER_STATUSES = {"answered", "archived", "done"}


class MemoryStore:
    def __init__(self, memory_dir: Path) -> None:
        self.memory_dir = memory_dir.expanduser().resolve()
        self.ensure_files()

    @property
    def items_path(self) -> Path:
        return self.memory_dir / "items.json"

    def ensure_files(self) -> None:
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        if not self.items_path.exists():
            self._write_items([])

    def render_selected(self, session_key: SessionKey) -> str:
        scopes = {"global", self._conversation_scope(session_key)}
        items = [
            item
            for item in self._read_items()
            if item.scope in scopes and item.status not in OMITTED_RENDER_STATUSES
        ]
        if not items:
            return ""

        sections: list[str] = []
        for scope in ["global", self._conversation_scope(session_key)]:
            scoped = [item for item in items if item.scope == scope]
            if not scoped:
                continue
            label = "global" if scope == "global" else "conversation"
            lines = [f"## {label} memory"]
            for item in sorted(scoped, key=lambda value: (value.kind, value.id)):
                lines.append(f"- [{item.id}] {item.kind}/{item.status}: {item.content}")
            sections.append("\n".join(lines))
        return "\n\n".join(sections)

    def apply_ops(
        self,
        session_key: SessionKey,
        ops: list[MemoryOperation],
        source_message_ids: list[int] | None = None,
    ) -> list[dict[str, Any]]:
        self.ensure_files()
        items = self._read_items()
        source_ids = source_message_ids or []
        applied: list[dict[str, Any]] = []

        for op in ops:
            if op.op == "archive":
                item = self._find_item(items, op.id)
                if item is None:
                    continue
                updated = replace(item, status="archived", updated_at=utc_now_iso())
                self._replace_item(items, updated)
                applied.append({"op": "archive", "id": updated.id, "status": updated.status})
                continue

            scope = self._scope_for_op(session_key, op)
            content = op.content.strip()
            if not content:
                continue
            item_id = op.id or self._find_duplicate_id(items, scope, op.kind, content) or self._item_id(scope, op.kind, content)
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
            applied.append({"op": "upsert", **item.to_record()})

        if applied:
            self._write_items(items)
        return applied

    def _scope_for_op(self, session_key: SessionKey, op: MemoryOperation) -> str:
        if op.scope == "global":
            return "global"
        return self._conversation_scope(session_key)

    def _conversation_scope(self, session_key: SessionKey) -> str:
        return f"conversation:{session_key.value}"

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

    def _read_items(self) -> list[MemoryItem]:
        if not self.items_path.exists():
            return []
        try:
            data = json.loads(self.items_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        if not isinstance(data, list):
            return []
        items: list[MemoryItem] = []
        for record in data:
            if not isinstance(record, dict):
                continue
            try:
                items.append(MemoryItem.from_record(record))
            except (KeyError, TypeError, ValueError):
                continue
        return items

    def _write_items(self, items: list[MemoryItem]) -> None:
        self.memory_dir.mkdir(parents=True, exist_ok=True)
        records = [item.to_record() for item in sorted(items, key=lambda value: (value.scope, value.kind, value.id))]
        tmp = self.items_path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(records, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(self.items_path)
