from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .models import (
    AgentDecision,
    NormalizedMessage,
    NormalizedMessageMove,
    NormalizedReaction,
    SessionKey,
    safe_slug,
    scoped_conversation_dir,
    utc_now_iso,
)


REACTION_EVENTS_CAP = 20
ENTRY_DELIMITER = "\n§\n"
POSTED_BOT_UPDATES_FILENAME = "posted_bot_updates.jsonl"


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
    last_injected_memory_hash: str | None = None
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
            last_injected_memory_hash=(
                str(record["last_injected_memory_hash"])
                if record.get("last_injected_memory_hash") is not None
                else None
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
            "last_injected_memory_hash": self.last_injected_memory_hash,
            "last_processed_message_id": self.last_processed_message_id,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


class WorkspaceStorage:
    def __init__(self, workspace_dir: Path) -> None:
        self.workspace_dir = workspace_dir.expanduser().resolve()
        self.records_dir = self.workspace_dir / "records"
        self.memory_dir = self.workspace_dir / "memory"
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

    def reconcile_message_paths(self, message: NormalizedMessage) -> None:
        if message.conversation_type == "private" or message.stream_id is None:
            return
        new_slug = safe_slug(message.stream)
        for root in [self.records_dir, self.memory_dir]:
            self._rename_stream_dirs(root, message.stream_id, new_slug)
        self._update_stream_metadata(message.stream_id, message.stream, new_slug)

    def apply_message_move(self, move: NormalizedMessageMove) -> dict[str, Any]:
        source_key = self._resolved_move_key(move.source_key)
        destination_key = self._resolved_move_key(move.destination_key)
        source_dir = self.session_dir(source_key)
        destination_dir = self.session_dir(destination_key)

        if not source_dir.exists():
            result = {
                "kind": "message_move",
                "status": "ignored",
                "reason": "source session not found",
                "message_ids": move.message_ids,
            }
            self.log_error(None, result)
            return result

        if source_dir == destination_dir:
            return {
                "kind": "message_move",
                "status": "skipped",
                "reason": "source and destination are the same session",
                "message_ids": move.message_ids,
            }

        if move.propagate_mode == "change_all":
            moved = self._move_or_merge_session(
                source_dir,
                destination_dir,
                destination_key,
                move,
                merge_memory=True,
            )
            return {
                "kind": "message_move",
                "status": "applied" if moved else "ignored",
                "reason": "full conversation move",
                "message_ids": move.message_ids,
                "session_key": destination_key.value,
            }

        moved_ids = self._move_message_records(source_dir, destination_dir, destination_key, move)
        if not moved_ids:
            result = {
                "kind": "message_move",
                "status": "ignored",
                "reason": "no matching message records found",
                "message_ids": move.message_ids,
            }
            self.log_error(source_key, result)
            return result
        return {
            "kind": "message_move",
            "status": "applied",
            "reason": "partial message move",
            "message_ids": moved_ids,
            "session_key": destination_key.value,
        }

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
        self.reconcile_message_paths(message)
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
        metadata.session_id = key.storage_id
        metadata.session_key = key.value
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

    def set_last_injected_memory_hash(self, key: SessionKey, memory_hash: str | None) -> None:
        metadata = self.load_metadata(key)
        metadata.last_injected_memory_hash = memory_hash
        self.save_metadata(metadata)

    def append_posted_bot_update(
        self,
        key: SessionKey,
        *,
        source: str,
        content: str,
        post: dict[str, Any],
        acknowledgement: str = "",
        message_ids: list[int] | None = None,
        job_id: str | None = None,
    ) -> dict[str, Any] | None:
        final_content = content.strip()
        if not final_content:
            return None
        record: dict[str, Any] = {
            "id": uuid.uuid4().hex,
            "created_at": utc_now_iso(),
            "source": source,
            "content": final_content,
            "post": post,
        }
        if acknowledgement.strip():
            record["acknowledgement"] = acknowledgement.strip()
        if message_ids:
            record["message_ids"] = list(message_ids)
        if job_id:
            record["job_id"] = job_id
        self._append_jsonl(self.session_path(key, POSTED_BOT_UPDATES_FILENAME), record)
        return record

    def read_pending_posted_bot_updates(self, key: SessionKey, limit: int = 20) -> list[dict[str, Any]]:
        records = self._read_jsonl(self.session_path(key, POSTED_BOT_UPDATES_FILENAME))
        if limit <= 0:
            return records
        return records[:limit]

    def consume_posted_bot_updates(self, key: SessionKey, updates: list[dict[str, Any]]) -> None:
        consumed_ids = {str(update.get("id")) for update in updates if update.get("id") is not None}
        if not consumed_ids:
            return
        path = self.session_path(key, POSTED_BOT_UPDATES_FILENAME)
        remaining = [
            record
            for record in self._read_jsonl(path)
            if str(record.get("id")) not in consumed_ids
        ]
        self._write_jsonl(path, remaining)

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
        skill_applied: list[dict[str, Any]] | None = None,
        schedule_applied: list[dict[str, Any]] | None = None,
        memory_acknowledgement: str = "",
        skill_acknowledgement: str = "",
        schedule_acknowledgement: str = "",
    ) -> None:
        record: dict[str, Any] = {
            "created_at": utc_now_iso(),
            "message_ids": [message.message_id for message in messages],
            "decision": decision.to_record(),
            "post": post,
            "memory_applied": memory_applied,
        }
        if skill_applied:
            record["skill_applied"] = skill_applied
        if schedule_applied:
            record["schedule_applied"] = schedule_applied
        if memory_acknowledgement:
            record["memory_acknowledgement"] = memory_acknowledgement
        if skill_acknowledgement:
            record["skill_acknowledgement"] = skill_acknowledgement
        if schedule_acknowledgement:
            record["schedule_acknowledgement"] = schedule_acknowledgement
        self._append_jsonl(self.session_path(key, "turns.jsonl"), record)

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

    def _memory_session_dir(self, key: SessionKey) -> Path:
        return scoped_conversation_dir(self.memory_dir, key, readable_topic=True)

    def _resolved_move_key(self, key: SessionKey) -> SessionKey:
        if key.conversation_type == "private" or key.stream_id is None:
            return key
        stream_slug = self._existing_stream_slug(key.stream_id) or key.stream_slug or "unknown"
        return SessionKey(
            realm_id=key.realm_id,
            stream_id=key.stream_id,
            topic_hash=key.topic_hash,
            conversation_type=key.conversation_type,
            private_user_key=key.private_user_key,
            stream_slug=stream_slug,
            topic_slug=key.topic_slug,
        )

    def _existing_stream_slug(self, stream_id: int) -> str | None:
        suffix = f"-{stream_id}"
        for root in [self.records_dir, self.memory_dir]:
            if not root.exists():
                continue
            for path in sorted(root.glob(f"stream-*{suffix}")):
                if path.is_dir() and path.name.endswith(suffix):
                    return path.name[len("stream-") : -len(suffix)]
        return None

    def _rename_stream_dirs(self, root: Path, stream_id: int, new_slug: str) -> None:
        if not root.exists():
            return
        suffix = f"-{stream_id}"
        destination = root / f"stream-{new_slug}-{stream_id}"
        for source in sorted(root.glob(f"stream-*{suffix}")):
            if not source.is_dir() or not source.name.endswith(suffix) or source == destination:
                continue
            if destination.exists():
                self._merge_directory(source, destination, old_base=source, new_base=destination, merge_memory=True)
            else:
                source.rename(destination)
                self._rewrite_message_paths(destination, old_base=source, new_base=destination)

    def _update_stream_metadata(self, stream_id: int, stream: str, stream_slug: str) -> None:
        for session_path in sorted(self.records_dir.glob(f"stream-*-{stream_id}/topic-*/session.json")):
            try:
                record = json.loads(session_path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            if not isinstance(record, dict):
                continue
            record["stream"] = stream
            record["stream_slug"] = stream_slug
            key = SessionKey(
                realm_id=str(record.get("realm_id") or "unknown"),
                stream_id=stream_id,
                topic_hash=str(record.get("topic_hash") or ""),
                conversation_type=str(record.get("conversation_type") or "stream"),
                private_user_key=(
                    str(record["private_user_key"]) if record.get("private_user_key") is not None else None
                ),
                stream_slug=stream_slug,
                topic_slug=str(record.get("topic_slug") or "") or None,
            )
            record["session_id"] = key.storage_id
            record["session_key"] = key.value
            self._write_json(session_path, record)

    def _move_or_merge_session(
        self,
        source_dir: Path,
        destination_dir: Path,
        destination_key: SessionKey,
        move: NormalizedMessageMove,
        *,
        merge_memory: bool,
    ) -> bool:
        memory_source = self._memory_session_dir(self._resolved_move_key(move.source_key))
        memory_destination = self._memory_session_dir(destination_key)
        if destination_dir.exists():
            self._merge_directory(source_dir, destination_dir, old_base=source_dir, new_base=destination_dir)
        else:
            destination_dir.parent.mkdir(parents=True, exist_ok=True)
            source_dir.rename(destination_dir)
            self._rewrite_message_paths(destination_dir, old_base=source_dir, new_base=destination_dir)
        self._save_destination_metadata(destination_dir, destination_key, move, preserve_existing_thread=True)

        if merge_memory and memory_source.exists() and memory_source != memory_destination:
            if memory_destination.exists():
                self._merge_directory(
                    memory_source,
                    memory_destination,
                    old_base=memory_source,
                    new_base=memory_destination,
                    merge_memory=True,
                )
            else:
                memory_destination.parent.mkdir(parents=True, exist_ok=True)
                memory_source.rename(memory_destination)
        return True

    def _move_message_records(
        self,
        source_dir: Path,
        destination_dir: Path,
        destination_key: SessionKey,
        move: NormalizedMessageMove,
    ) -> list[int]:
        source_path = source_dir / "messages.jsonl"
        records = self._read_jsonl(source_path)
        move_ids = set(move.message_ids)
        moved: list[dict[str, Any]] = []
        remaining: list[dict[str, Any]] = []
        for record in records:
            message_id = self._optional_message_id(record)
            if message_id is not None and message_id in move_ids:
                moved.append(self._rewrite_record_paths(record, old_base=source_dir, new_base=destination_dir))
            else:
                remaining.append(record)
        if not moved:
            return []

        destination_dir.mkdir(parents=True, exist_ok=True)
        self._write_jsonl(source_path, remaining)
        self._merge_message_records(destination_dir / "messages.jsonl", moved)
        self._move_pending_ids(source_dir, destination_dir, [int(record["message_id"]) for record in moved])
        self._move_upload_dirs(source_dir, destination_dir, [int(record["message_id"]) for record in moved])
        self._save_destination_metadata(destination_dir, destination_key, move, preserve_existing_thread=True)
        return [int(record["message_id"]) for record in moved]

    def _merge_directory(
        self,
        source: Path,
        destination: Path,
        *,
        old_base: Path,
        new_base: Path,
        merge_memory: bool = False,
    ) -> None:
        destination.mkdir(parents=True, exist_ok=True)
        for child in sorted(source.iterdir()):
            target = destination / child.name
            if not target.exists():
                was_dir = child.is_dir()
                child.rename(target)
                if was_dir or child.name == "messages.jsonl":
                    self._rewrite_message_paths(destination, old_base=old_base, new_base=new_base)
                continue
            if child.is_dir() and target.is_dir():
                self._merge_directory(child, target, old_base=old_base, new_base=new_base, merge_memory=merge_memory)
                continue
            if child.is_file() and target.is_file():
                self._merge_file(child, target, old_base=old_base, new_base=new_base, merge_memory=merge_memory)
        self._remove_empty_dir(source)

    def _merge_file(
        self,
        source: Path,
        destination: Path,
        *,
        old_base: Path,
        new_base: Path,
        merge_memory: bool,
    ) -> None:
        if source.name == "messages.jsonl":
            records = [
                self._rewrite_record_paths(record, old_base=old_base, new_base=new_base)
                for record in self._read_jsonl(source)
            ]
            self._merge_message_records(destination, records)
            source.unlink()
            return
        if source.name == "pending.json":
            self._write_pending_ids(destination, self._merge_pending_values(destination, source))
            source.unlink()
            return
        if source.name == "turns.jsonl":
            existing = destination.read_text(encoding="utf-8") if destination.exists() else ""
            extra = source.read_text(encoding="utf-8")
            self._write_text_atomic(destination, existing + extra)
            source.unlink()
            return
        if source.name == "MEMORY.md" and merge_memory:
            self._write_memory_entries(destination, self._merge_memory_entries(destination, source))
            source.unlink()
            return
        if source.read_bytes() == destination.read_bytes():
            source.unlink()
            return
        if source.name == "AGENTS.md":
            archive = destination.with_name(f"AGENTS.merged-{source.parent.name}.md")
            if not archive.exists():
                source.rename(archive)

    def _save_destination_metadata(
        self,
        destination_dir: Path,
        destination_key: SessionKey,
        move: NormalizedMessageMove,
        *,
        preserve_existing_thread: bool,
    ) -> None:
        path = destination_dir / "session.json"
        existing = self._read_json(path)
        if existing is None:
            existing = {}
        elif not preserve_existing_thread:
            existing.pop("codex_thread_id", None)
            existing.pop("codex_instruction_mode", None)

        metadata = SessionMetadata.from_record(existing, destination_key)
        metadata.realm_id = destination_key.realm_id
        metadata.conversation_type = "stream"
        metadata.stream_id = destination_key.stream_id
        metadata.stream = move.stream_name if move.new_stream_id == move.stream_id else (metadata.stream or "unknown")
        metadata.stream_slug = destination_key.stream_slug or safe_slug(metadata.stream)
        metadata.topic = move.subject
        metadata.topic_hash = destination_key.topic_hash
        metadata.topic_slug = destination_key.topic_slug or safe_slug(move.subject)
        self.save_metadata(metadata)

    def _merge_message_records(self, path: Path, records: list[dict[str, Any]]) -> None:
        merged: dict[int, dict[str, Any]] = {}
        without_id: list[dict[str, Any]] = []
        for record in [*self._read_jsonl(path), *records]:
            message_id = self._optional_message_id(record)
            if message_id is None:
                without_id.append(record)
            else:
                merged[message_id] = record
        self._write_jsonl(path, [*without_id, *[merged[key] for key in sorted(merged)]])

    def _rewrite_message_paths(self, directory: Path, *, old_base: Path, new_base: Path) -> None:
        for path in sorted(directory.glob("**/messages.jsonl")):
            records = self._read_jsonl(path)
            if records:
                self._write_jsonl(
                    path,
                    [self._rewrite_record_paths(record, old_base=old_base, new_base=new_base) for record in records],
                )

    def _rewrite_record_paths(self, record: dict[str, Any], *, old_base: Path, new_base: Path) -> dict[str, Any]:
        rewritten = dict(record)
        old_rel = old_base.relative_to(self.workspace_dir).as_posix()
        new_rel = new_base.relative_to(self.workspace_dir).as_posix()
        if isinstance(rewritten.get("content"), str):
            rewritten["content"] = rewritten["content"].replace(old_rel, new_rel)
        uploads: list[dict[str, Any]] = []
        changed_uploads = False
        for item in rewritten.get("uploads") or []:
            if not isinstance(item, dict):
                continue
            upload = dict(item)
            if isinstance(upload.get("local_path"), str):
                upload["local_path"] = upload["local_path"].replace(old_rel, new_rel)
                changed_uploads = True
            uploads.append(upload)
        if uploads and changed_uploads:
            rewritten["uploads"] = uploads
        return rewritten

    def _move_pending_ids(self, source_dir: Path, destination_dir: Path, message_ids: list[int]) -> None:
        source_path = source_dir / "pending.json"
        destination_path = destination_dir / "pending.json"
        source_ids = self._read_pending_ids(source_path)
        moved = [message_id for message_id in source_ids if message_id in message_ids]
        if not moved:
            return
        remaining = [message_id for message_id in source_ids if message_id not in message_ids]
        self._write_pending_ids(source_path, remaining)
        destination_ids = self._read_pending_ids(destination_path)
        self._write_pending_ids(destination_path, [*destination_ids, *[item for item in moved if item not in destination_ids]])

    def _move_upload_dirs(self, source_dir: Path, destination_dir: Path, message_ids: list[int]) -> None:
        source_uploads = source_dir / "uploads"
        if not source_uploads.exists():
            return
        destination_uploads = destination_dir / "uploads"
        destination_uploads.mkdir(parents=True, exist_ok=True)
        for message_id in message_ids:
            source = source_uploads / str(message_id)
            destination = destination_uploads / str(message_id)
            if not source.exists():
                continue
            if destination.exists():
                self._merge_directory(source, destination, old_base=source_dir, new_base=destination_dir)
            else:
                source.rename(destination)
        self._remove_empty_dir(source_uploads)

    def _merge_pending_values(self, destination: Path, source: Path) -> list[int]:
        merged: list[int] = []
        for message_id in [*self._read_pending_ids(destination), *self._read_pending_ids(source)]:
            if message_id not in merged:
                merged.append(message_id)
        return merged

    def _write_pending_ids(self, path: Path, message_ids: list[int]) -> None:
        if message_ids:
            self._write_json(path, {"message_ids": message_ids})
        elif path.exists():
            path.unlink()

    def _merge_memory_entries(self, destination: Path, source: Path) -> list[str]:
        entries: list[str] = []
        seen: set[str] = set()
        for path in [destination, source]:
            for entry in self._read_memory_entries(path):
                if entry in seen:
                    continue
                seen.add(entry)
                entries.append(entry)
        return entries

    def _read_memory_entries(self, path: Path) -> list[str]:
        if not path.exists():
            return []
        text = path.read_text(encoding="utf-8").strip()
        if not text:
            return []
        return [entry.strip() for entry in text.split(ENTRY_DELIMITER) if entry.strip()]

    def _write_memory_entries(self, path: Path, entries: list[str]) -> None:
        text = ENTRY_DELIMITER.join(entry.strip() for entry in entries if entry.strip())
        self._write_text_atomic(path, text + ("\n" if text else ""))

    def _read_json(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None
        return record if isinstance(record, dict) else None

    def _remove_empty_dir(self, path: Path) -> None:
        while path.exists() and path != self.records_dir and path != self.memory_dir:
            try:
                path.rmdir()
            except OSError:
                return
            path = path.parent

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
        self._write_text_atomic(path, text)

    def _write_json(self, path: Path, record: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        tmp.replace(path)

    def _write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
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
