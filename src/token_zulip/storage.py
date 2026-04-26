from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import AgentDecision, NormalizedMessage, ScratchpadOperation, SessionKey, utc_now_iso


@dataclass
class SessionMetadata:
    session_id: str
    session_key: str
    realm_id: str
    conversation_type: str
    stream_id: int | None
    stream: str
    stream_slug: str
    topic: str
    topic_hash: str
    private_user_key: str | None = None
    codex_thread_id: str | None = None
    last_processed_message_id: int | None = None
    created_at: str = field(default_factory=utc_now_iso)
    updated_at: str = field(default_factory=utc_now_iso)

    @classmethod
    def create(cls, message: NormalizedMessage) -> "SessionMetadata":
        key = message.session_key
        return cls(
            session_id=key.storage_id,
            session_key=key.value,
            realm_id=message.realm_id,
            conversation_type=message.conversation_type,
            stream_id=message.stream_id,
            stream=message.stream,
            stream_slug=message.stream_slug,
            topic=message.topic,
            topic_hash=message.topic_hash,
            private_user_key=message.private_user_key,
        )

    @classmethod
    def placeholder(cls, key: SessionKey) -> "SessionMetadata":
        return cls(
            session_id=key.storage_id,
            session_key=key.value,
            realm_id=key.realm_id,
            conversation_type=key.conversation_type,
            stream_id=key.stream_id,
            stream="private" if key.conversation_type == "private" else "unknown",
            stream_slug="private" if key.conversation_type == "private" else "unknown",
            topic="private" if key.conversation_type == "private" else key.topic_hash,
            topic_hash=key.topic_hash,
            private_user_key=key.private_user_key,
        )

    @classmethod
    def from_record(cls, record: dict[str, Any], key: SessionKey) -> "SessionMetadata":
        stream_id = record.get("stream_id", key.stream_id)
        private_key = record.get("private_user_key", key.private_user_key)
        last_processed = _optional_int(record.get("last_processed_message_id"))
        return cls(
            session_id=str(record.get("session_id") or key.storage_id),
            session_key=str(record.get("session_key") or key.value),
            realm_id=str(record.get("realm_id") or key.realm_id),
            conversation_type=str(record.get("conversation_type") or key.conversation_type),
            stream_id=int(stream_id) if stream_id is not None else None,
            stream=str(record.get("stream") or ("private" if key.conversation_type == "private" else "unknown")),
            stream_slug=str(record.get("stream_slug") or ("private" if key.conversation_type == "private" else "unknown")),
            topic=str(record.get("topic") or ("private" if key.conversation_type == "private" else key.topic_hash)),
            topic_hash=str(record.get("topic_hash") or key.topic_hash),
            private_user_key=str(private_key) if private_key is not None else None,
            codex_thread_id=record.get("codex_thread_id"),
            last_processed_message_id=last_processed,
            created_at=str(record.get("created_at") or utc_now_iso()),
            updated_at=str(record.get("updated_at") or utc_now_iso()),
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "session_key": self.session_key,
            "realm_id": self.realm_id,
            "conversation_type": self.conversation_type,
            "stream_id": self.stream_id,
            "stream": self.stream,
            "stream_slug": self.stream_slug,
            "topic": self.topic,
            "topic_hash": self.topic_hash,
            "private_user_key": self.private_user_key,
            "codex_thread_id": self.codex_thread_id,
            "last_processed_message_id": self.last_processed_message_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class WorkspaceStorage:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir.expanduser().resolve()
        self.state_dir = self.workspace_dir / "state"
        self.sessions_dir = self.state_dir / "sessions"
        self.errors_dir = self.state_dir / "errors"
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        for path in [self.state_dir, self.sessions_dir, self.errors_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def log_ignored_event(self, event: dict[str, Any], reason: str, key: SessionKey | None = None) -> None:
        message = event.get("message") if isinstance(event.get("message"), dict) else {}
        record = {
            "created_at": utc_now_iso(),
            "kind": "ignored_event",
            "reason": reason,
            "session_id": key.storage_id if key else None,
            "message_id": message.get("id") if isinstance(message, dict) else None,
            "message_type": message.get("type") if isinstance(message, dict) else None,
            "sender_email": message.get("sender_email") if isinstance(message, dict) else None,
        }
        self._append_jsonl(self._error_path(), record)

    def append_message(self, message: NormalizedMessage) -> None:
        self.ensure_session(message)
        path = self.session_path(message.session_key, "messages.jsonl")
        if message.message_id in self._message_ids(path):
            return
        self._append_jsonl(path, message.to_record())

    def read_recent_messages(
        self,
        key: SessionKey,
        limit: int,
        exclude_message_ids: set[int] | None = None,
    ) -> list[dict[str, Any]]:
        exclude = exclude_message_ids or set()
        path = self.session_path(key, "messages.jsonl")
        records = [
            record
            for record in self._read_jsonl(path)
            if self._optional_message_id(record) not in exclude
        ]
        return records[-limit:]

    def append_pending_messages(self, key: SessionKey, messages: list[NormalizedMessage]) -> None:
        for message in messages:
            self.append_message(message)
        path = self.session_path(key, "pending.json")
        existing = self._read_pending_ids(path)
        merged = existing[:]
        for message in messages:
            if message.message_id not in merged:
                merged.append(message.message_id)
        self._write_json(path, {"message_ids": merged})

    def pop_pending_messages(self, key: SessionKey) -> list[NormalizedMessage]:
        path = self.session_path(key, "pending.json")
        if not path.exists():
            return []
        pending_ids = self._read_pending_ids(path)
        path.unlink()
        if not pending_ids:
            return []
        metadata = self.load_metadata(key)
        by_id = {message.message_id: message for message in self._load_messages(key, metadata)}
        return [by_id[message_id] for message_id in pending_ids if message_id in by_id]

    def ensure_session(self, message: NormalizedMessage) -> SessionMetadata:
        path = self.session_path(message.session_key, "session.json")
        if path.exists():
            metadata = self.load_metadata(message.session_key)
            changed = False
            for field_name, value in {
                "realm_id": message.realm_id,
                "conversation_type": message.conversation_type,
                "stream_id": message.stream_id,
                "stream": message.stream,
                "stream_slug": message.stream_slug,
                "topic": message.topic,
                "topic_hash": message.topic_hash,
                "private_user_key": message.private_user_key,
            }.items():
                if getattr(metadata, field_name) != value:
                    setattr(metadata, field_name, value)
                    changed = True
            if changed:
                self.save_metadata(metadata)
            return metadata

        metadata = SessionMetadata.create(message)
        self.save_metadata(metadata)
        return metadata

    def load_metadata(self, key: SessionKey) -> SessionMetadata:
        path = self.session_path(key, "session.json")
        if not path.exists():
            return SessionMetadata.placeholder(key)
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return SessionMetadata.placeholder(key)
        if not isinstance(record, dict):
            return SessionMetadata.placeholder(key)
        return SessionMetadata.from_record(record, key)

    def save_metadata(self, metadata: SessionMetadata) -> None:
        metadata.updated_at = utc_now_iso()
        key = SessionKey(
            metadata.realm_id,
            metadata.stream_id,
            metadata.topic_hash,
            conversation_type=metadata.conversation_type,
            private_user_key=metadata.private_user_key,
        )
        self._write_json(self.session_path(key, "session.json"), metadata.to_record())

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

    def log_turn(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        decision: AgentDecision,
        post: dict[str, Any] | None,
        memory_applied: list[dict[str, Any]],
        scratchpad_applied: dict[str, str] | None,
    ) -> None:
        self._append_jsonl(
            self.session_path(key, "turns.jsonl"),
            {
                "created_at": utc_now_iso(),
                "message_ids": [message.message_id for message in messages],
                "decision": decision.to_record(),
                "post": post,
                "memory_applied": memory_applied,
                "scratchpad_applied": scratchpad_applied,
            },
        )

    def log_error(self, key: SessionKey | None, event: dict[str, Any]) -> None:
        record = {
            "created_at": utc_now_iso(),
            "session_id": key.storage_id if key else None,
            **event,
        }
        self._append_jsonl(self._error_path(), record)

    def apply_scratchpad_op(self, key: SessionKey, op: ScratchpadOperation) -> dict[str, str] | None:
        if op.op == "none":
            return None
        path = self.session_path(key, "scratchpad.md")
        if op.op == "clear":
            if path.exists():
                path.unlink()
            return op.to_record()
        content = op.content.strip()
        path.write_text(content + ("\n" if content else ""), encoding="utf-8")
        return op.to_record()

    def session_path(self, key: SessionKey, filename: str) -> Path:
        directory = self.sessions_dir / key.storage_id
        directory.mkdir(parents=True, exist_ok=True)
        return directory / filename

    def _load_messages(self, key: SessionKey, metadata: SessionMetadata) -> list[NormalizedMessage]:
        return [self._message_from_record(record, metadata) for record in self._read_jsonl(self.session_path(key, "messages.jsonl"))]

    def _message_from_record(self, record: dict[str, Any], metadata: SessionMetadata) -> NormalizedMessage:
        sender_id = record.get("sender_id")
        return NormalizedMessage(
            realm_id=metadata.realm_id,
            message_id=int(record["message_id"]),
            stream_id=metadata.stream_id,
            stream=metadata.stream,
            stream_slug=metadata.stream_slug,
            topic=metadata.topic,
            topic_hash=metadata.topic_hash,
            sender_email=str(record.get("sender_email") or ""),
            sender_full_name=str(record.get("sender_full_name") or ""),
            sender_id=int(sender_id) if sender_id is not None else None,
            content=str(record.get("content") or ""),
            timestamp=record.get("timestamp"),
            received_at=str(record.get("received_at") or utc_now_iso()),
            raw={},
            conversation_type=metadata.conversation_type,
            private_user_key=metadata.private_user_key,
            reply_required=bool(record.get("reply_required") or metadata.conversation_type == "private"),
            directly_addressed=bool(record.get("directly_addressed")),
        )

    def _message_ids(self, path: Path) -> set[int]:
        ids: set[int] = set()
        for record in self._read_jsonl(path):
            message_id = self._optional_message_id(record)
            if message_id is not None:
                ids.add(message_id)
        return ids

    def _optional_message_id(self, record: dict[str, Any]) -> int | None:
        try:
            return int(record["message_id"])
        except (KeyError, TypeError, ValueError):
            return None

    def _read_pending_ids(self, path: Path) -> list[int]:
        if not path.exists():
            return []
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        values = data.get("message_ids", []) if isinstance(data, dict) else []
        ids: list[int] = []
        for value in values:
            try:
                ids.append(int(value))
            except (TypeError, ValueError):
                continue
        return ids

    def _read_jsonl(self, path: Path) -> list[dict[str, Any]]:
        if not path.exists():
            return []
        records: list[dict[str, Any]] = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                value = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(value, dict):
                records.append(value)
        return records

    def _append_jsonl(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=False) + "\n")

    def _write_json(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(path)

    def _error_path(self) -> Path:
        name = f"{datetime.now(timezone.utc).date().isoformat()}.jsonl"
        return self.errors_dir / name


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
