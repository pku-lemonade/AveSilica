from __future__ import annotations

import json

from token_zulip.memory import MemoryStore
from token_zulip.models import (
    AgentDecision,
    MemoryOperation,
    NormalizedMessage,
    NormalizedMessageMove,
    NormalizedReaction,
    SessionKey,
    normalized_topic_hash,
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


def _topic_message(message_id: int, *, stream: str = "Engineering", topic: str = "Launch") -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=message_id,
        stream_id=10,
        stream=stream,
        stream_slug=safe_slug(stream),
        topic=topic,
        topic_hash=normalized_topic_hash(topic),
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content=f"message {message_id}",
        timestamp=None,
        received_at="now",
        raw={},
    )


def _move(
    message_ids: list[int],
    *,
    orig_subject: str = "Launch",
    subject: str = "Release",
    propagate_mode: str = "change_later",
) -> NormalizedMessageMove:
    return NormalizedMessageMove(
        realm_id="realm",
        message_id=message_ids[0],
        message_ids=message_ids,
        stream_id=10,
        stream_name="Engineering",
        orig_subject=orig_subject,
        new_stream_id=10,
        subject=subject,
        propagate_mode=propagate_mode,
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
    storage.set_last_injected_memory_hash(key, "memory-hash")
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
    assert storage.load_metadata(key).last_injected_memory_hash == "memory-hash"
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


def test_read_conversation_participants_extracts_silent_and_id_mentions(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = NormalizedMessage(
        realm_id="realm",
        message_id=1,
        stream_id=10,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic123",
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content="cc @_**Ariella Drake|26** and @**Bo Lin|27**; FYI @_**Alice**",
        timestamp=None,
        received_at="now",
        raw={},
    )

    storage.append_message(message)

    participants = storage.read_conversation_participants(message.session_key)
    assert {item["user_id"]: item["full_name"] for item in participants} == {
        1: "Alice",
        26: "Ariella Drake",
        27: "Bo Lin",
    }


def test_channel_rename_moves_records_and_memory_by_stream_id(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _topic_message(1, stream="Engineering", topic="Launch")
    storage.append_message(first)
    old_memory = tmp_path / "memory" / "stream-engineering-10" / f"topic-launch-{first.topic_hash}"
    old_memory.mkdir(parents=True)
    (old_memory / "MEMORY.md").write_text("channel fact\n", encoding="utf-8")

    renamed = _topic_message(2, stream="Platform", topic="Launch")
    storage.append_message(renamed)

    new_key = renamed.session_key
    assert not (tmp_path / "records" / "stream-engineering-10").exists()
    assert storage.read_recent_messages(new_key, 10)[0]["message_id"] == 1
    assert storage.load_metadata(new_key).stream == "Platform"
    assert (
        tmp_path / "memory" / "stream-platform-10" / f"topic-launch-{first.topic_hash}" / "MEMORY.md"
    ).read_text(encoding="utf-8") == "channel fact\n"
    assert "channel fact" in MemoryStore(tmp_path / "memory").render_selected(new_key)


def test_full_topic_rename_moves_records_memory_and_preserves_thread(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    storage.append_message(source)
    storage.set_codex_thread_state(source.session_key, thread_id="thread-1", instruction_mode="developer-v1")
    memory_dir = tmp_path / "memory" / "stream-engineering-10" / f"topic-launch-{source.topic_hash}"
    memory_dir.mkdir(parents=True)
    (memory_dir / "MEMORY.md").write_text("launch fact\n", encoding="utf-8")

    result = storage.apply_message_move(_move([1], propagate_mode="change_all"))
    destination = _topic_message(2, topic="Release").session_key

    assert result["status"] == "applied"
    assert storage.read_recent_messages(destination, 10)[0]["message_id"] == 1
    assert storage.load_metadata(destination).codex_thread_id == "thread-1"
    assert (tmp_path / "memory" / "stream-engineering-10" / f"topic-release-{destination.topic_hash}").exists()
    assert "launch fact" in MemoryStore(tmp_path / "memory").render_selected(destination)


def test_full_topic_rename_rewrites_message_upload_paths(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    old_rel = f"records/stream-engineering-10/topic-launch-{source.topic_hash}/uploads/1/01-figure.png"
    source = NormalizedMessage(
        **{
            **source.__dict__,
            "content": f"see ![figure]({old_rel})",
            "uploads": [{"local_path": old_rel}],
        }
    )
    storage.append_message(source)
    upload_dir = storage.session_dir(source.session_key) / "uploads" / "1"
    upload_dir.mkdir(parents=True)
    (upload_dir / "01-figure.png").write_bytes(b"image")

    storage.apply_message_move(_move([1], propagate_mode="change_all"))
    destination = _topic_message(2, topic="Release").session_key
    record = storage.read_recent_messages(destination, 1)[0]

    assert f"topic-release-{destination.topic_hash}/uploads/1/01-figure.png" in record["content"]
    assert f"topic-release-{destination.topic_hash}/uploads/1/01-figure.png" in record["uploads"][0]["local_path"]
    assert (storage.session_dir(destination) / "uploads" / "1" / "01-figure.png").exists()


def test_full_topic_rename_into_existing_destination_merges_messages_and_memory(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    destination_message = _topic_message(2, topic="Release")
    storage.append_message(source)
    storage.append_message(destination_message)
    storage.set_codex_thread_state(destination_message.session_key, thread_id="destination-thread", instruction_mode="developer-v1")
    source_memory = tmp_path / "memory" / "stream-engineering-10" / f"topic-launch-{source.topic_hash}"
    destination_memory = tmp_path / "memory" / "stream-engineering-10" / f"topic-release-{destination_message.topic_hash}"
    source_memory.mkdir(parents=True)
    destination_memory.mkdir(parents=True)
    (source_memory / "MEMORY.md").write_text("source fact\n", encoding="utf-8")
    (destination_memory / "MEMORY.md").write_text("destination fact\n", encoding="utf-8")

    storage.apply_message_move(_move([1], propagate_mode="change_all"))

    records = storage.read_recent_messages(destination_message.session_key, 10)
    assert [record["message_id"] for record in records] == [1, 2]
    assert storage.load_metadata(destination_message.session_key).codex_thread_id == "destination-thread"
    assert "source fact" in (destination_memory / "MEMORY.md").read_text(encoding="utf-8")
    assert "destination fact" in (destination_memory / "MEMORY.md").read_text(encoding="utf-8")


def test_partial_topic_move_moves_only_matching_message_records(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _topic_message(1, topic="Launch")
    second = _topic_message(2, topic="Launch")
    storage.append_message(first)
    storage.append_message(second)
    storage.append_pending_messages(first.session_key, [second])
    MemoryStore(tmp_path / "memory").apply_ops(
        first.session_key,
        [MemoryOperation(op="add", scope="conversation", content="Launch-only memory")],
    )

    result = storage.apply_message_move(_move([2], propagate_mode="change_one"))
    destination = _topic_message(3, topic="Release").session_key

    assert result["status"] == "applied"
    assert [record["message_id"] for record in storage.read_recent_messages(first.session_key, 10)] == [1]
    assert [record["message_id"] for record in storage.read_recent_messages(destination, 10)] == [2]
    assert [message.message_id for message in storage.pop_pending_messages(destination)] == [2]
    assert storage.load_metadata(destination).codex_thread_id is None
    assert "Launch-only memory" not in MemoryStore(tmp_path / "memory").render_selected(destination)
    storage.apply_reaction(_reaction(2))
    assert storage.read_recent_messages(destination, 10)[0]["reactions"][0]["emoji_name"] == "100"


def test_partial_topic_move_into_existing_destination_preserves_destination_thread(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    destination_message = _topic_message(2, topic="Release")
    storage.append_message(source)
    storage.append_message(destination_message)
    storage.set_codex_thread_state(destination_message.session_key, thread_id="destination-thread", instruction_mode="developer-v1")

    storage.apply_message_move(_move([1], propagate_mode="change_one"))

    assert storage.load_metadata(destination_message.session_key).codex_thread_id == "destination-thread"
    assert [record["message_id"] for record in storage.read_recent_messages(destination_message.session_key, 10)] == [1, 2]


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
        "1001",
        conversation_type="private",
        private_recipient_key="1001",
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
