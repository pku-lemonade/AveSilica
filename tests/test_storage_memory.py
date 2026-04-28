from __future__ import annotations

import json

from token_zulip.memory import MemoryStore
from token_zulip.models import (
    AgentDecision,
    MemoryOperation,
    NormalizedMessage,
    NormalizedReaction,
    SessionKey,
    safe_slug,
)
from token_zulip.storage import REACTION_EVENTS_CAP, WorkspaceStorage
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


def _reaction(
    message_id: int = 1,
    *,
    op: str = "add",
    emoji_name: str = "100",
    user_id: int = 2,
) -> NormalizedReaction:
    return NormalizedReaction(
        realm_id="realm",
        message_id=message_id,
        op=op,
        emoji_name=emoji_name,
        emoji_code="1f4af",
        reaction_type="unicode_emoji",
        user_id=user_id,
        user_email=f"user{user_id}@example.com",
        user_full_name=f"User {user_id}",
        timestamp=None,
        received_at="now",
        raw={},
    )


def test_storage_uses_readable_session_messages_pending_and_turns(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _message(1)
    second = _message(2)
    key = first.session_key

    storage.append_message(first)
    storage.append_pending_messages(key, [second])
    storage.set_codex_thread_state(key, thread_id="thread-1", instruction_mode="developer-v1")
    storage.mark_processed(key, [1])
    storage.log_turn(
        key,
        [first],
        AgentDecision(False, "silent", ""),
        post=None,
        memory_applied=[],
    )

    assert storage.read_recent_messages(key, 10)[0]["message_id"] == 1
    assert storage.pop_pending_messages(key)[0].message_id == 2
    assert storage.load_metadata(key).codex_thread_id == "thread-1"
    assert storage.load_metadata(key).codex_instruction_mode == "developer-v1"
    assert storage.load_metadata(key).last_processed_message_id == 1

    message_record = json.loads(storage.session_path(key, "messages.jsonl").read_text(encoding="utf-8").splitlines()[0])
    assert "stream" not in message_record
    assert "topic_hash" not in message_record
    assert storage.session_path(key, "session.json").exists()
    assert storage.session_path(key, "turns.jsonl").exists()
    assert storage.session_path(key, "messages.jsonl").is_relative_to(
        tmp_path / "records" / "stream-engineering-10" / "topic-launch-topic123"
    )
    assert not (tmp_path / "records" / "sessions").exists()
    assert not (tmp_path / "state").exists()


def test_safe_slug_preserves_unicode_and_useful_punctuation():
    assert safe_slug("阿里服务器") == "阿里服务器"
    assert safe_slug("Dynamic.hls") == "dynamic.hls"
    assert safe_slug("gpt 5.5") == "gpt-5.5"
    assert safe_slug("") == "unnamed"


def test_read_recent_messages_excludes_current_message_ids(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _message(1)
    second = _message(2)
    key = first.session_key

    storage.append_message(first)
    storage.append_message(second)

    assert [record["message_id"] for record in storage.read_recent_messages(key, 10, exclude_message_ids={2})] == [1]


def test_apply_reaction_updates_target_message_record(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = _message(1)
    storage.append_message(message)

    key = storage.apply_reaction(_reaction(1))

    assert key == message.session_key
    record = storage.read_recent_messages(message.session_key, 1)[0]
    assert record["reactions"][0]["emoji_name"] == "100"
    assert record["reactions"][0]["user_full_name"] == "User 2"
    assert record["reaction_events"][0]["op"] == "add"


def test_apply_reaction_remove_clears_active_reaction_and_keeps_audit(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = _message(1)
    storage.append_message(message)

    storage.apply_reaction(_reaction(1, op="add"))
    storage.apply_reaction(_reaction(1, op="remove"))

    record = storage.read_recent_messages(message.session_key, 1)[0]
    assert "reactions" not in record
    assert [event["op"] for event in record["reaction_events"]] == ["add", "remove"]


def test_apply_reaction_caps_audit_events(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = _message(1)
    storage.append_message(message)

    for index in range(REACTION_EVENTS_CAP + 5):
        storage.apply_reaction(_reaction(1, emoji_name=f"emoji-{index}", user_id=index + 10))

    record = storage.read_recent_messages(message.session_key, 1)[0]
    assert len(record["reaction_events"]) == REACTION_EVENTS_CAP
    assert record["reaction_events"][0]["emoji_name"] == "emoji-5"
    assert record["reaction_events"][-1]["emoji_name"] == f"emoji-{REACTION_EVENTS_CAP + 4}"


def test_apply_reaction_unknown_message_returns_none(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)

    assert storage.apply_reaction(_reaction(404)) is None


def test_memory_ops_add_replace_remove_and_render_by_scope(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    first = store.apply_ops(
        key,
        [MemoryOperation(op="add", scope="conversation", content="Team prefers short replies")],
        [1],
    )
    second = store.apply_ops(
        key,
        [MemoryOperation(op="add", scope="conversation", content="Team prefers short replies")],
        [2],
    )

    assert first[0]["status"] == "applied"
    assert second[0]["status"] == "skipped"
    assert "Team prefers short replies" in store.render_selected(key)
    assert "conversation memory" in store.render_selected(key)
    memory_path = tmp_path / "memory" / "stream-engineering-10" / "topic-launch-topic123" / "MEMORY.md"
    assert memory_path.read_text(encoding="utf-8").count("Team prefers short replies") == 1

    replaced = store.apply_ops(
        key,
        [MemoryOperation(op="replace", scope="conversation", old_text="short replies", content="Team prefers concise replies")],
        [3],
    )
    assert replaced[0]["status"] == "applied"
    assert "Team prefers concise replies" in store.render_selected(key)
    assert "Team prefers short replies" not in store.render_selected(key)

    removed = store.apply_ops(
        key,
        [MemoryOperation(op="remove", scope="conversation", old_text="concise replies")],
        [4],
    )
    assert removed[0]["status"] == "applied"
    assert "Team prefers concise replies" not in store.render_selected(key)


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
    stream_key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    store.apply_ops(
        private_key,
        [MemoryOperation(op="add", scope="conversation", content="Alice likes brief DM replies")],
    )

    assert "Alice likes brief DM replies" in store.render_selected(private_key)
    assert "Alice likes brief DM replies" not in store.render_selected(stream_key)


def test_channel_memory_renders_for_sibling_topics_in_same_stream(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory")
    first_topic = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")
    second_topic = SessionKey("realm", 10, "topic456", stream_slug="engineering", topic_slug="release")
    other_stream = SessionKey("realm", 20, "topic123", stream_slug="research", topic_slug="launch")

    store.apply_ops(
        first_topic,
        [MemoryOperation(op="add", scope="channel", content="Use concise architecture summaries")],
    )
    store.apply_ops(
        first_topic,
        [MemoryOperation(op="add", scope="conversation", content="Launch topic local fact")],
    )

    assert "Use concise architecture summaries" in store.render_selected(second_topic)
    assert "Launch topic local fact" not in store.render_selected(second_topic)
    assert "Use concise architecture summaries" not in store.render_selected(other_stream)
    assert (tmp_path / "memory" / "stream-engineering-10" / "MEMORY.md").exists()


def test_memory_ops_reject_oversized_entries_without_blocking(tmp_path):
    initialize_workspace(tmp_path)
    store = MemoryStore(tmp_path / "memory", char_limit=12)
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    result = store.apply_ops(
        key,
        [MemoryOperation(op="add", scope="conversation", content="too long for this file")],
        [1],
    )

    assert result[0]["status"] == "rejected"
    assert "exceed" in result[0]["reason"]
    assert "too long" not in store.render_selected(key)
