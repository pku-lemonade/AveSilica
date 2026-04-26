from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import AgentDecision, NormalizedMessage, ScratchpadUpdate, SessionKey, utc_now_iso


@dataclass
class SessionMetadata:
    session_key: str
    realm_id: str
    stream_id: int
    topic_hash: str
    codex_thread_id: str | None = None
    last_processed_message_id: int | None = None
    updated_at: str = field(default_factory=utc_now_iso)

    @classmethod
    def create(cls, key: SessionKey) -> "SessionMetadata":
        return cls(
            session_key=key.value,
            realm_id=key.realm_id,
            stream_id=key.stream_id,
            topic_hash=key.topic_hash,
        )

    @classmethod
    def from_record(cls, record: dict[str, Any], key: SessionKey) -> "SessionMetadata":
        return cls(
            session_key=str(record.get("session_key") or key.value),
            realm_id=str(record.get("realm_id") or key.realm_id),
            stream_id=int(record.get("stream_id") or key.stream_id),
            topic_hash=str(record.get("topic_hash") or key.topic_hash),
            codex_thread_id=record.get("codex_thread_id"),
            last_processed_message_id=record.get("last_processed_message_id"),
            updated_at=str(record.get("updated_at") or utc_now_iso()),
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "session_key": self.session_key,
            "realm_id": self.realm_id,
            "stream_id": self.stream_id,
            "topic_hash": self.topic_hash,
            "codex_thread_id": self.codex_thread_id,
            "last_processed_message_id": self.last_processed_message_id,
            "updated_at": self.updated_at,
        }


class WorkspaceStorage:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir.expanduser().resolve()
        self.state_dir = self.workspace_dir / "state"
        self.raw_dir = self.state_dir / "raw"
        self.sessions_dir = self.state_dir / "sessions"
        self.errors_dir = self.state_dir / "errors"
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        for path in [self.state_dir, self.raw_dir, self.sessions_dir, self.errors_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def append_raw_event(self, event: dict[str, Any]) -> None:
        date = datetime.now(timezone.utc).date().isoformat()
        self._append_jsonl(self.raw_dir / f"{date}.jsonl", {"received_at": utc_now_iso(), "event": event})

    def append_transcript(self, message: NormalizedMessage) -> None:
        self._append_jsonl(self.session_path(message.session_key, "transcript.jsonl"), message.to_record())

    def read_recent_transcript(self, key: SessionKey, limit: int) -> list[dict[str, Any]]:
        path = self.session_path(key, "transcript.jsonl")
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        records: list[dict[str, Any]] = []
        for line in lines[-limit:]:
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
        return records

    def append_pending_messages(self, key: SessionKey, messages: list[NormalizedMessage]) -> None:
        path = self.session_path(key, "pending.jsonl")
        for message in messages:
            self._append_jsonl(path, message.to_record())

    def pop_pending_messages(self, key: SessionKey) -> list[NormalizedMessage]:
        path = self.session_path(key, "pending.jsonl")
        if not path.exists():
            return []
        lines = path.read_text(encoding="utf-8").splitlines()
        path.unlink()
        messages: list[NormalizedMessage] = []
        for line in lines:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict):
                messages.append(NormalizedMessage.from_record(record))
        return messages

    def load_metadata(self, key: SessionKey) -> SessionMetadata:
        path = self.session_path(key, "metadata.json")
        if not path.exists():
            return SessionMetadata.create(key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return SessionMetadata.create(key)
        if not isinstance(record, dict):
            return SessionMetadata.create(key)
        return SessionMetadata.from_record(record, key)

    def save_metadata(self, metadata: SessionMetadata) -> None:
        metadata.updated_at = utc_now_iso()
        key = SessionKey(metadata.realm_id, metadata.stream_id, metadata.topic_hash)
        self._write_json(self.session_path(key, "metadata.json"), metadata.to_record())

    def set_codex_thread_id(self, key: SessionKey, thread_id: str | None) -> None:
        metadata = self.load_metadata(key)
        metadata.codex_thread_id = thread_id
        self.save_metadata(metadata)

    def mark_processed(self, key: SessionKey, message_ids: list[int]) -> None:
        if not message_ids:
            return
        metadata = self.load_metadata(key)
        next_id = max(message_ids)
        if metadata.last_processed_message_id is None or next_id > metadata.last_processed_message_id:
            metadata.last_processed_message_id = next_id
            self.save_metadata(metadata)

    def log_outbound(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        decision: AgentDecision,
        codex_raw_text: str,
        post_result: dict[str, Any] | None,
        memory_applied: list[dict[str, Any]],
    ) -> None:
        self._append_jsonl(
            self.session_path(key, "outbound.jsonl"),
            {
                "created_at": utc_now_iso(),
                "message_ids": [message.message_id for message in messages],
                "decision": decision.to_record(),
                "codex_raw_text": codex_raw_text,
                "post_result": post_result,
                "memory_applied": memory_applied,
            },
        )

    def log_error(self, key: SessionKey | None, event: dict[str, Any]) -> None:
        name = f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
        record = {"created_at": utc_now_iso(), "session_key": key.value if key else None, **event}
        self._append_jsonl(self.errors_dir / name, record)

    def apply_scratchpad_updates(self, key: SessionKey, updates: list[ScratchpadUpdate]) -> list[dict[str, str]]:
        path = self.session_path(key, "scratchpad.md")
        applied: list[dict[str, str]] = []
        for update in updates:
            content = update.content.strip()
            if not content:
                continue
            if update.mode == "replace":
                path.write_text(content + "\n", encoding="utf-8")
            else:
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(f"\n\n<!-- updated {utc_now_iso()} -->\n{content}\n")
            applied.append(update.to_record())
        return applied

    def session_path(self, key: SessionKey, filename: str) -> Path:
        directory = self.sessions_dir / key.storage_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / filename

    def _append_jsonl(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=False) + "\n")

    def _write_json(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(path)

