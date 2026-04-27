from __future__ import annotations

import json

from token_zulip.memory import MemoryStore
from token_zulip.models import AgentDecision, MemoryOperation, NormalizedMessage, ScratchpadOperation, SessionKey
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


def test_storage_uses_compact_session_messages_pending_and_turns(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _message(1)
    second = _message(2)
    key = first.session_key

    storage.append_message(first)
    storage.append_pending_messages(key, [second])
    storage.set_codex_thread_id(key, "thread-1")
    storage.mark_processed(key, [1])
    storage.log_turn(
        key,
        [first],
        AgentDecision(False, "silent", ""),
        post=None,
        memory_applied=[],
        scratchpad_applied=None,
    )

    assert storage.read_recent_messages(key, 10)[0]["message_id"] == 1
    assert storage.pop_pending_messages(key)[0].message_id == 2
    assert storage.load_metadata(key).codex_thread_id == "thread-1"
    assert storage.load_metadata(key).last_processed_message_id == 1

    message_record = json.loads(storage.session_path(key, "messages.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert "stream" not in message_record
    assert "topic_hash" not in message_record
    assert storage.session_path(key, "session.json").exists()
    assert storage.session_path(key, "turns.jsonl").exists()
    assert not (tmp_path / "state" / "raw").exists()


def test_read_recent_messages_excludes_current_message_ids(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _message(1)
    second = _message(2)
    key = first.session_key

    storage.append_message(first)
    storage.append_message(second)

    assert [record["message_id"] for record in storage.read_recent_messages(key, 10, exclude_message_ids={2})] == [1]


def test_memory_ops_are_deduplicated_archived_and_rendered_by_scope(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering")

    first = store.apply_ops(
        key,
        [MemoryOperation(op="upsert", scope="conversation", kind="fact", content="Team prefers short replies")],
        [1],
    )
    second = store.apply_ops(
        key,
        [MemoryOperation(op="upsert", scope="conversation", kind="fact", content="  Team prefers short replies  ")],
        [2],
    )

    assert first[0]["id"] == second[0]["id"]
    assert "Team prefers short replies" in store.render_selected(key)
    assert "conversation memory" in store.render_selected(key)
    seed_path = tmp_path / "memory" / "stream-10-engineering" / "topic-topic123" / "seeds.jsonl"
    assert len([line for line in seed_path.read_text(encoding="utf-8").splitlines() if line.strip()]) == 1

    store.apply_ops(key, [MemoryOperation(op="archive", id=first[0]["id"])], [3])

    assert "Team prefers short replies" not in store.render_selected(key)


def test_private_memory_is_session_local_and_not_rendered_for_streams(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    private_key = SessionKey(
        "realm",
        None,
        "3",
        conversation_type="private",
        private_user_key="3",
    )
    stream_key = SessionKey("realm", 10, "topic123", stream_slug="engineering")

    store.apply_ops(
        private_key,
        [MemoryOperation(op="upsert", scope="conversation", kind="preference", content="Alice likes brief DM replies")],
    )

    assert "Alice likes brief DM replies" in store.render_selected(private_key)
    assert "Alice likes brief DM replies" not in store.render_selected(stream_key)


def test_channel_memory_renders_for_sibling_topics_in_same_stream(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    first_topic = SessionKey("realm", 10, "topic123", stream_slug="engineering")
    second_topic = SessionKey("realm", 10, "topic456", stream_slug="engineering")
    other_stream = SessionKey("realm", 20, "topic123", stream_slug="research")

    store.apply_ops(
        first_topic,
        [MemoryOperation(op="upsert", scope="channel", kind="preference", content="Use concise architecture summaries")],
    )
    store.apply_ops(
        first_topic,
        [MemoryOperation(op="upsert", scope="conversation", kind="fact", content="Launch topic local fact")],
    )

    assert "Use concise architecture summaries" in store.render_selected(second_topic)
    assert "Launch topic local fact" not in store.render_selected(second_topic)
    assert "Use concise architecture summaries" not in store.render_selected(other_stream)
    assert (tmp_path / "memory" / "stream-10-engineering" / "seeds.jsonl").exists()


def test_scratchpad_operation_replaces_or_clears_snapshot(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    key = _message(1).session_key

    storage.apply_scratchpad_op(key, ScratchpadOperation(op="replace", content="notes"))
    assert storage.session_path(key, "scratchpad.md").read_text(encoding="utf-8") == "notes\n"

    storage.apply_scratchpad_op(key, ScratchpadOperation(op="clear", content=""))
    assert not storage.session_path(key, "scratchpad.md").exists()
