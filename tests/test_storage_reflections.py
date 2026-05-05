from __future__ import annotations

import json
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta

from token_zulip.models import (
    AgentDecision,
    NormalizedMessage,
    NormalizedMessageMove,
    NormalizedReaction,
    ReflectionOperation,
    SessionKey,
    normalized_topic_hash,
    safe_slug,
)
from token_zulip.reflections import ReflectionStore
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
    storage.mark_processed(key, [1])
    storage.log_turn(
        key,
        [first],
        AgentDecision(False, "silent", ""),
        post=None,
        reflection_applied=[],
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


def test_trace_sidecars_are_pruneable_without_touching_conversation_history(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = _message(1)
    key = message.session_key
    storage.append_message(message)
    storage.log_turn(
        key,
        [message],
        AgentDecision(False, "silent", ""),
        post=None,
        reflection_applied=[],
    )

    manifest = storage.log_trace(
        key,
        "trace-1",
        source="conversation_turn",
        message_ids=[1],
        model="gpt-test",
        parent_thread_id="thread-1",
        roles=[
            {
                "role": "skill",
                "developer_instructions": "Skill Worker Policy",
                "prompt": "# Skill Availability\n\n# New Zulip Message(s)\n\n- [1] Alice: message 1",
                "output_schema_path": tmp_path / "references" / "skill" / "schema.json",
                "raw_output": '{"skill_ops":[]}',
                "decision": {"skill_ops": []},
                "thread_id": "thread-1-skill",
                "parent_thread_id": "thread-1",
                "worker_mode": "fork",
                "status": "ok",
                "developer_instructions_sent": True,
            }
        ],
    )
    trace_dir = storage.trace_dir(key, "trace-1")
    assert manifest["trace_id"] == "trace-1"
    assert (trace_dir / "skill" / "developer.md").read_text(encoding="utf-8") == "Skill Worker Policy"
    assert "Skill Availability" in (trace_dir / "skill" / "user.md").read_text(encoding="utf-8")
    assert (trace_dir / "skill" / "schema.json").exists()
    assert storage.list_traces()[0]["trace_id"] == "trace-1"

    manifest_path = trace_dir / "manifest.json"
    old_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    old_manifest["created_at"] = "2000-01-01T00:00:00+00:00"
    manifest_path.write_text(json.dumps(old_manifest), encoding="utf-8")

    summary = storage.cleanup_traces_older_than(timedelta(days=1))

    assert summary["deleted"] == 1
    assert not trace_dir.exists()
    assert storage.session_path(key, "messages.jsonl").exists()
    assert storage.session_path(key, "turns.jsonl").exists()


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


def test_channel_rename_moves_records_instructions_and_reflections_by_stream_id(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _topic_message(1, stream="Engineering", topic="Launch")
    storage.append_message(first)
    old_instructions = tmp_path / "instructions" / "stream-engineering-10" / f"topic-launch-{first.topic_hash}"
    old_instructions.mkdir(parents=True)
    (old_instructions / "AGENTS.md").write_text("topic rule\n", encoding="utf-8")
    old_reflections = tmp_path / "reflections" / "stream-engineering-10"
    old_reflections.mkdir(parents=True)
    (old_reflections / "REFLECTIONS.md").write_text("channel reflection\n", encoding="utf-8")

    renamed = _topic_message(2, stream="Platform", topic="Launch")
    storage.append_message(renamed)

    new_key = renamed.session_key
    assert not (tmp_path / "records" / "stream-engineering-10").exists()
    assert storage.read_recent_messages(new_key, 10)[0]["message_id"] == 1
    assert storage.load_metadata(new_key).stream == "Platform"
    assert (
        tmp_path / "instructions" / "stream-platform-10" / f"topic-launch-{first.topic_hash}" / "AGENTS.md"
    ).read_text(encoding="utf-8") == "topic rule\n"
    assert (
        tmp_path / "reflections" / "stream-platform-10" / "REFLECTIONS.md"
    ).read_text(encoding="utf-8") == "channel reflection\n"


def test_full_topic_rename_moves_records_instructions_and_preserves_thread(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    storage.append_message(source)
    storage.set_codex_thread_state(source.session_key, thread_id="thread-1", instruction_mode="developer-v1")
    instruction_dir = tmp_path / "instructions" / "stream-engineering-10" / f"topic-launch-{source.topic_hash}"
    instruction_dir.mkdir(parents=True)
    (instruction_dir / "AGENTS.md").write_text("launch rule\n", encoding="utf-8")

    result = storage.apply_message_move(_move([1], propagate_mode="change_all"))
    destination = _topic_message(2, topic="Release").session_key

    assert result["status"] == "applied"
    assert storage.read_recent_messages(destination, 10)[0]["message_id"] == 1
    assert storage.load_metadata(destination).codex_thread_id == "thread-1"
    assert (
        tmp_path / "instructions" / "stream-engineering-10" / f"topic-release-{destination.topic_hash}" / "AGENTS.md"
    ).read_text(encoding="utf-8") == "launch rule\n"


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


def test_full_topic_rename_into_existing_destination_merges_messages_and_archives_instruction_conflicts(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    source = _topic_message(1, topic="Launch")
    destination_message = _topic_message(2, topic="Release")
    storage.append_message(source)
    storage.append_message(destination_message)
    storage.set_codex_thread_state(destination_message.session_key, thread_id="destination-thread", instruction_mode="developer-v1")
    source_instructions = tmp_path / "instructions" / "stream-engineering-10" / f"topic-launch-{source.topic_hash}"
    destination_instructions = (
        tmp_path / "instructions" / "stream-engineering-10" / f"topic-release-{destination_message.topic_hash}"
    )
    source_instructions.mkdir(parents=True)
    destination_instructions.mkdir(parents=True)
    (source_instructions / "AGENTS.md").write_text("source rule\n", encoding="utf-8")
    (destination_instructions / "AGENTS.md").write_text("destination rule\n", encoding="utf-8")

    storage.apply_message_move(_move([1], propagate_mode="change_all"))

    records = storage.read_recent_messages(destination_message.session_key, 10)
    assert [record["message_id"] for record in records] == [1, 2]
    assert storage.load_metadata(destination_message.session_key).codex_thread_id == "destination-thread"
    assert (destination_instructions / "AGENTS.md").read_text(encoding="utf-8") == "destination rule\n"
    assert (destination_instructions / f"AGENTS.merged-{source_instructions.name}.md").read_text(
        encoding="utf-8"
    ) == "source rule\n"


def test_partial_topic_move_moves_only_matching_message_records(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    first = _topic_message(1, topic="Launch")
    second = _topic_message(2, topic="Launch")
    storage.append_message(first)
    storage.append_message(second)
    storage.append_pending_messages(first.session_key, [second])

    result = storage.apply_message_move(_move([2], propagate_mode="change_one"))
    destination = _topic_message(3, topic="Release").session_key

    assert result["status"] == "applied"
    assert [record["message_id"] for record in storage.read_recent_messages(first.session_key, 10)] == [1]
    assert [record["message_id"] for record in storage.read_recent_messages(destination, 10)] == [2]
    assert [message.message_id for message in storage.pop_pending_messages(destination)] == [2]
    assert storage.load_metadata(destination).codex_thread_id is None
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


def test_reflection_store_writes_global_channel_and_private_inboxes(tmp_path):
    initialize_workspace(tmp_path)
    store = ReflectionStore(tmp_path / "reflections")
    stream_key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")
    private_key = SessionKey(
        "realm",
        None,
        "1001",
        conversation_type="private",
        private_recipient_key="1001",
    )

    global_result = store.apply_ops(
        stream_key,
        [
            ReflectionOperation(
                scope="global",
                kind="policy_candidate",
                suggested_target="references/reply/system.md",
                content="User seems to dislike context-free public-thread suggestions; consider tightening reply policy.",
            )
        ],
        [1],
    )
    channel_result = store.apply_ops(
        stream_key,
        [
            ReflectionOperation(
                scope="source",
                kind="workflow_lesson",
                suggested_target="AGENTS.md",
                content="This channel may need concise architecture summaries for future review threads.",
            )
        ],
        [2],
    )
    private_result = store.apply_ops(
        private_key,
        [
            ReflectionOperation(
                scope="source",
                kind="style_preference",
                suggested_target="AGENTS.md",
                content="Private chats might need shorter operational replies when the user is scheduling reminders.",
            )
        ],
        [3],
    )

    assert global_result[0]["scope"] == "global"
    assert channel_result[0]["scope"] == "channel"
    assert private_result[0]["scope"] == "private"
    assert "context-free public-thread" in (tmp_path / "reflections" / "REFLECTIONS.md").read_text(encoding="utf-8")
    assert "concise architecture" in (
        tmp_path / "reflections" / "stream-engineering-10" / "REFLECTIONS.md"
    ).read_text(encoding="utf-8")
    assert "shorter operational replies" in (
        tmp_path / "reflections" / "private-recipient-1001" / "REFLECTIONS.md"
    ).read_text(encoding="utf-8")
    assert not (tmp_path / "reflections" / "stream-engineering-10" / "topic-launch-topic123").exists()


def test_reflection_store_serializes_concurrent_global_appends(tmp_path):
    initialize_workspace(tmp_path)
    reflections_dir = tmp_path / "reflections"
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    def append(index: int) -> list[dict]:
        store = ReflectionStore(reflections_dir)
        return store.apply_ops(
            key,
            [
                ReflectionOperation(
                    scope="global",
                    kind="workflow_lesson",
                    suggested_target="references/reply/system.md",
                    content=(
                        "Future reflection writes may need append serialization.\n"
                        f"Unique global candidate token {index:03d}."
                    ),
                )
            ],
            [1000 + index],
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(append, range(40)))

    text = (reflections_dir / "REFLECTIONS.md").read_text(encoding="utf-8")
    assert all(result[0]["status"] == "applied" for result in results)
    for index in range(40):
        assert text.count(f"Unique global candidate token {index:03d}.") == 1
    assert not (reflections_dir / "REFLECTIONS.md.tmp").exists()


def test_reflection_store_serializes_concurrent_channel_appends(tmp_path):
    initialize_workspace(tmp_path)
    reflections_dir = tmp_path / "reflections"
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    def append(index: int) -> list[dict]:
        store = ReflectionStore(reflections_dir)
        return store.apply_ops(
            key,
            [
                ReflectionOperation(
                    scope="source",
                    kind="policy_candidate",
                    suggested_target="AGENTS.md",
                    content=(
                        "Channel reflection writes should preserve every candidate.\n"
                        f"Unique channel candidate token {index:03d}."
                    ),
                )
            ],
            [2000 + index],
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        results = list(executor.map(append, range(40)))

    reflection_file = reflections_dir / "stream-engineering-10" / "REFLECTIONS.md"
    text = reflection_file.read_text(encoding="utf-8")
    assert all(result[0]["status"] == "applied" for result in results)
    for index in range(40):
        assert text.count(f"Unique channel candidate token {index:03d}.") == 1
    assert not reflection_file.with_suffix(reflection_file.suffix + ".tmp").exists()


def test_reflection_store_skips_archival_summaries(tmp_path):
    initialize_workspace(tmp_path)
    store = ReflectionStore(tmp_path / "reflections")
    key = SessionKey("realm", 10, "topic123", stream_slug="engineering", topic_slug="launch")

    result = store.apply_ops(
        key,
        [
            ReflectionOperation(
                scope="source",
                kind="summary",
                suggested_target="none",
                content="Feiyang reported the camera-ready edits are mostly done.",
            )
        ],
        [1],
    )

    assert result[0]["status"] == "skipped"
    assert not (tmp_path / "reflections" / "stream-engineering-10" / "REFLECTIONS.md").exists()
