from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import (
    AgentDecision,
    NormalizedMessage,
    NormalizedReaction,
    SessionKey,
    safe_slug,
    scoped_conversation_dir,
    utc_now_iso,
)


REACTION_EVENTS_CAP = 20


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
    topic_slug: str
    private_user_key: str | None = None
    codex_thread_id: str | None = None
    codex_instruction_mode: str | None = None
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
            topic_slug=safe_slug(message.topic),
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
            topic_slug=key.topic_slug or safe_slug("private" if key.conversation_type == "private" else key.topic_hash),
            private_user_key=key.private_user_key,
        )

    @classmethod
    def from_record(cls, record: dict[str, Any], key: SessionKey) -> "SessionMetadata":
        stream_id = record.get("stream_id", key.stream_id)
        private_key = record.get("private_user_key", key.private_user_key)
        last_processed = _optional_int(record.get("last_processed_message_id"))
        topic = str(record.get("topic") or ("private" if key.conversation_type == "private" else key.topic_hash))
        return cls(
            session_id=str(record.get("session_id") or key.storage_id),
            session_key=str(record.get("session_key") or key.value),
            realm_id=str(record.get("realm_id") or key.realm_id),
            conversation_type=str(record.get("conversation_type") or key.conversation_type),
            stream_id=int(stream_id) if stream_id is not None else None,
            stream=str(record.get("stream") or ("private" if key.conversation_type == "private" else "unknown")),
            stream_slug=str(record.get("stream_slug") or ("private" if key.conversation_type == "private" else "unknown")),
            topic=topic,
            topic_hash=str(record.get("topic_hash") or key.topic_hash),
            topic_slug=str(record.get("topic_slug") or key.topic_slug or safe_slug(topic)),
            private_user_key=str(private_key) if private_key is not None else None,
            codex_thread_id=str(record["codex_thread_id"]) if record.get("codex_thread_id") is not None else None,
            codex_instruction_mode=(
                str(record["codex_instruction_mode"]) if record.get("codex_instruction_mode") is not None else None
            ),
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
            "topic_slug": self.topic_slug,
            "private_user_key": self.private_user_key,
            "codex_thread_id": self.codex_thread_id,
            "codex_instruction_mode": self.codex_instruction_mode,
            "last_processed_message_id": self.last_processed_message_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class WorkspaceStorage:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir.expanduser().resolve()
        self.records_dir = self.workspace_dir / "records"
        self.errors_dir = self.records_dir / "errors"
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        for path in [self.records_dir, self.errors_dir]:
            path.mkdir(parents=True, exist_ok=True)

    def log_ignored_event(self, event: dict[str, Any], reason: str, key: SessionKey | None = None) -> None:
        message = event.get("message") if isinstance(event.get("message"), dict) else {}
        record = {
            "created_at": utc_now_iso(),
            "kind": "ignored_event",
            "reason": reason,
            "session_id": key.storage_id if key else None,
            "message_id": message.get("id") if isinstance(message, dict) else event.get("message_id"),
            "message_type": message.get("type") if isinstance(message, dict) else None,
            "sender_email": message.get("sender_email") if isinstance(message, dict) else event.get("user_email"),
        }
        self._append_jsonl(self._error_path(), record)

    def append_message(self, message: NormalizedMessage) -> None:
        self.ensure_session(message)
        path = self.session_path(message.session_key, "messages.jsonl")
        if message.message_id in self._message_ids(path):
            return
        self._append_jsonl(path, message.to_record())

    def update_message(self, message: NormalizedMessage) -> None:
        self.ensure_session(message)
        path = self.session_path(message.session_key, "messages.jsonl")
        records = self._read_jsonl(path)
        for index, record in enumerate(records):
            if self._optional_message_id(record) == message.message_id:
                updated = message.to_record()
                for field_name in ["reactions", "reaction_events"]:
                    if field_name not in updated and field_name in record:
                        updated[field_name] = record[field_name]
                records[index] = updated
                self._write_jsonl(path, records)
                return
        self._append_jsonl(path, message.to_record())

    def apply_reaction(self, reaction: NormalizedReaction) -> SessionKey | None:
        found = self._find_message_record(reaction.message_id)
        if found is None:
            return None

        path, records, index, key = found
        record = records[index]
        active = {
            self._reaction_record_key(item): item
            for item in record.get("reactions") or []
            if isinstance(item, dict)
        }
        if reaction.op == "add":
            active[reaction.active_key] = reaction.to_active_record()
        else:
            active.pop(reaction.active_key, None)

        active_records = sorted(active.values(), key=self._reaction_sort_key)
        if active_records:
            record["reactions"] = active_records
        else:
            record.pop("reactions", None)

        events = [item for item in record.get("reaction_events") or [] if isinstance(item, dict)]
        events.append(reaction.to_event_record())
        record["reaction_events"] = events[-REACTION_EVENTS_CAP:]
        records[index] = record
        self._write_jsonl(path, records)
        return key

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
                "topic_slug": safe_slug(message.topic),
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
            stream_slug=metadata.stream_slug,
            topic_slug=metadata.topic_slug,
        )
        self._write_json(self.session_path(key, "session.json"), metadata.to_record())

    def set_codex_thread_id(self, key: SessionKey, thread_id: str | None) -> None:
        metadata = self.load_metadata(key)
        metadata.codex_thread_id = thread_id
        self.save_metadata(metadata)

    def set_codex_thread_state(
        self,
        key: SessionKey,
        *,
        thread_id: str | None,
        instruction_mode: str | None,
    ) -> None:
        metadata = self.load_metadata(key)
        metadata.codex_thread_id = thread_id
        metadata.codex_instruction_mode = instruction_mode
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
    ) -> None:
        self._append_jsonl(
            self.session_path(key, "turns.jsonl"),
            {
                "created_at": utc_now_iso(),
                "message_ids": [message.message_id for message in messages],
                "decision": decision.to_record(),
                "post": post,
                "memory_applied": memory_applied,
            },
        )

    def log_error(self, key: SessionKey | None, event: dict[str, Any]) -> None:
        record = {
            "created_at": utc_now_iso(),
            "session_id": key.storage_id if key else None,
            **event,
        }
        self._append_jsonl(self._error_path(), record)

    def session_path(self, key: SessionKey, filename: str) -> Path:
        directory = self.session_dir(key)
        directory.mkdir(parents=True, exist_ok=True)
        return directory / filename

    def session_dir(self, key: SessionKey) -> Path:
        return scoped_conversation_dir(self.records_dir, key, readable_topic=True)

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
            uploads=list(record.get("uploads") or []),
            reactions=[item for item in record.get("reactions", []) if isinstance(item, dict)],
            reaction_events=[item for item in record.get("reaction_events", []) if isinstance(item, dict)],
        )

    def _find_message_record(
        self,
        message_id: int,
    ) -> tuple[Path, list[dict[str, Any]], int, SessionKey] | None:
        for path in sorted(self.records_dir.glob("**/messages.jsonl")):
            records = self._read_jsonl(path)
            for index, record in enumerate(records):
                if self._optional_message_id(record) != message_id:
                    continue
                key = self._key_from_session_file(path.parent / "session.json")
                if key is None:
                    return None
                return path, records, index, key
        return None

    def _key_from_session_file(self, path: Path) -> SessionKey | None:
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        if not isinstance(record, dict):
            return None

        realm_id = str(record.get("realm_id") or "")
        topic_hash = str(record.get("topic_hash") or "")
        if not realm_id or not topic_hash:
            return None
        conversation_type = str(record.get("conversation_type") or "stream")
        return SessionKey(
            realm_id=realm_id,
            stream_id=_optional_int(record.get("stream_id")),
            topic_hash=topic_hash,
            conversation_type=conversation_type,
            private_user_key=(
                str(record["private_user_key"]) if record.get("private_user_key") is not None else None
            ),
            stream_slug=str(record.get("stream_slug") or "") or None,
            topic_slug=str(record.get("topic_slug") or "") or None,
        )

    def _reaction_record_key(self, record: dict[str, Any]) -> tuple[str, str]:
        user_key = str(record.get("user_key") or "").strip()
        if not user_key:
            user_id = _optional_int(record.get("user_id"))
            user_key = (
                str(user_id)
                if user_id is not None
                else str(record.get("user_email") or "").strip().casefold()
            )
        return (user_key or "unknown", str(record.get("emoji_name") or ""))

    def _reaction_sort_key(self, record: dict[str, Any]) -> tuple[str, str]:
        user = str(record.get("user_full_name") or record.get("user_email") or record.get("user_key") or "")
        return (str(record.get("emoji_name") or ""), user.casefold())

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

    def _write_jsonl(self, path: Path, records: list[dict[str, Any]]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        text = "".join(json.dumps(record, ensure_ascii=False, sort_keys=False) + "\n" for record in records)
        path.write_text(text, encoding="utf-8")

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
