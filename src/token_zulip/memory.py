from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from .models import MEMORY_FILES, MemoryUpdate, SessionKey, utc_now_iso


class MemoryStore:
    def __init__(self, memory_dir: Path) -> None:
        self.memory_dir = memory_dir.expanduser().resolve()
        self.memory_dir.mkdir(parents=True, exist_ok=True)

    @property
    def index_path(self) -> Path:
        return self.memory_dir / "index.json"

    def ensure_files(self, memory_dir: Path | None = None) -> None:
        memory_dir = memory_dir or self.memory_dir
        memory_dir.mkdir(parents=True, exist_ok=True)
        for file_name in MEMORY_FILES:
            path = memory_dir / file_name
            if not path.exists():
                title = file_name.removesuffix(".md").replace("_", " ").title()
                path.write_text(f"# {title}\n\n", encoding="utf-8")
        index_path = self._index_path(memory_dir)
        if not index_path.exists():
            self._write_index(memory_dir, {"global": sorted(MEMORY_FILES), "sessions": {}})

    def render_selected(self, session_key: SessionKey) -> str:
        memory_dir = self._memory_dir_for_session(session_key)
        self.ensure_files(memory_dir)
        file_names = self._files_for_session(session_key, memory_dir)
        parts: list[str] = []
        for file_name in file_names:
            path = self._validated_path(memory_dir, file_name)
            content = path.read_text(encoding="utf-8").strip()
            label = self._memory_label(session_key, file_name)
            parts.append(f"## {label}\n\n{content or '(empty)'}")
        return "\n\n".join(parts)

    def apply_updates(self, session_key: SessionKey, updates: list[MemoryUpdate]) -> list[dict[str, Any]]:
        memory_dir = self._memory_dir_for_session(session_key)
        self.ensure_files(memory_dir)
        applied: list[dict[str, Any]] = []
        for update in updates:
            content = update.content.strip()
            if not content:
                continue
            path = self._validated_path(memory_dir, update.file)
            if update.mode == "replace":
                path.write_text(content + "\n", encoding="utf-8")
            else:
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(f"\n\n<!-- updated {utc_now_iso()} from {session_key.value} -->\n")
                    handle.write(content + "\n")
            applied.append(update.to_record())

        if applied:
            self._touch_session_index(memory_dir, session_key, sorted({item["file"] for item in applied}))
        return applied

    def _memory_dir_for_session(self, session_key: SessionKey) -> Path:
        if session_key.conversation_type == "private":
            return self.memory_dir / "private" / session_key.storage_id
        return self.memory_dir

    def _memory_label(self, session_key: SessionKey, file_name: str) -> str:
        if session_key.conversation_type == "private":
            return f"private-memory/{file_name}"
        return f"memory/{file_name}"

    def _files_for_session(self, session_key: SessionKey, memory_dir: Path) -> list[str]:
        index = self._read_index(memory_dir)
        selected: list[str] = []
        for file_name in index.get("global", []):
            if file_name in MEMORY_FILES and file_name not in selected:
                selected.append(file_name)

        session_entry = index.get("sessions", {}).get(session_key.value, {})
        session_files = session_entry.get("files", []) if isinstance(session_entry, dict) else []
        for file_name in session_files:
            if file_name in MEMORY_FILES and file_name not in selected:
                selected.append(file_name)

        return selected or sorted(MEMORY_FILES)

    def _validated_path(self, memory_dir: Path, file_name: str) -> Path:
        if file_name not in MEMORY_FILES:
            raise ValueError(f"invalid memory file: {file_name!r}")
        path = (memory_dir / file_name).resolve()
        path.relative_to(memory_dir)
        return path

    def _index_path(self, memory_dir: Path) -> Path:
        return memory_dir / "index.json"

    def _read_index(self, memory_dir: Path) -> dict[str, Any]:
        index_path = self._index_path(memory_dir)
        if not index_path.exists():
            return {"global": sorted(MEMORY_FILES), "sessions": {}}
        try:
            data = json.loads(index_path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {"global": sorted(MEMORY_FILES), "sessions": {}}
        if not isinstance(data, dict):
            return {"global": sorted(MEMORY_FILES), "sessions": {}}
        data.setdefault("global", sorted(MEMORY_FILES))
        data.setdefault("sessions", {})
        return data

    def _write_index(self, memory_dir: Path, index: dict[str, Any]) -> None:
        self._index_path(memory_dir).write_text(json.dumps(index, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _touch_session_index(self, memory_dir: Path, session_key: SessionKey, files: list[str]) -> None:
        index = self._read_index(memory_dir)
        sessions = index.setdefault("sessions", {})
        existing = sessions.get(session_key.value, {})
        existing_files = existing.get("files", []) if isinstance(existing, dict) else []
        merged = sorted({*existing_files, *files})
        sessions[session_key.value] = {
            "files": merged,
            "updated_at": utc_now_iso(),
        }
        self._write_index(memory_dir, index)
