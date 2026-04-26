from __future__ import annotations

from token_zulip.memory import MemoryStore
from token_zulip.models import MemoryUpdate, NormalizedMessage, SessionKey
from token_zulip.storage import WorkspaceStorage
from token_zulip.workspace import initialize_workspace


def _message(message_id: int = 1) -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=message_id,
        stream_id=10,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic123",
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content=f"message {message_id}",
        timestamp=None,
        received_at="now",
        raw={},
    )


def test_storage_tracks_transcript_pending_metadata_and_outbound(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _message(1)
    second = _message(2)
    key = first.session_key

    storage.append_raw_event({"type": "message"})
    storage.append_transcript(first)
    storage.append_pending_messages(key, [second])
    storage.set_codex_thread_id(key, "thread-1")
    storage.mark_processed(key, [1])

    assert storage.read_recent_transcript(key, 10)[0]["message_id"] == 1
    assert storage.pop_pending_messages(key)[0].message_id == 2
    assert storage.load_metadata(key).codex_thread_id == "thread-1"
    assert storage.load_metadata(key).last_processed_message_id == 1
    assert list((tmp_path / "state" / "raw").glob("*.jsonl"))


def test_memory_updates_are_validated_and_indexed(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    key = SessionKey("realm", 10, "topic123")

    applied = store.apply_updates(
        key,
        [MemoryUpdate(file="durable.md", mode="append", content="- Team prefers short replies")],
    )

    assert applied
    assert "Team prefers short replies" in (tmp_path / "memory" / "durable.md").read_text(encoding="utf-8")
    assert key.value in (tmp_path / "memory" / "index.json").read_text(encoding="utf-8")
    assert "durable.md" in store.render_selected(key)

