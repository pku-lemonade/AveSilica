from __future__ import annotations

import asyncio
import json
from pathlib import Path

from token_zulip.codex_adapter import CodexRunResult
from token_zulip.config import BotConfig
from token_zulip.instructions import InstructionLoader
from token_zulip.loop import PRIVATE_REPLY_FALLBACK, AgentLoop
from token_zulip.memory import MemoryStore
from token_zulip.models import NormalizedMessage
from token_zulip.storage import WorkspaceStorage
from token_zulip.typing_status import TypingStatusManager
from token_zulip.workspace import initialize_workspace


def _config(workspace: Path, *, post_replies: bool = True) -> BotConfig:
    return BotConfig(
        workspace_dir=workspace,
        zulip_config_file=None,
        realm_id="realm",
        bot_email="bot@example.com",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
        role="default",
        codex_model="gpt-5.4",
        codex_reasoning_effort=None,
        codex_cwd=workspace,
        codex_sandbox="read-only",
        codex_approval_policy="never",
        max_recent_messages=20,
        queue_limit=8,
        worker_count=2,
        instruction_max_bytes=96_000,
        post_replies=post_replies,
        listen_all_public_streams=True,
        typing_enabled=True,
        typing_refresh_seconds=8.0,
    )


def _message(message_id: int, content: str = "hello", *, directly_addressed: bool = False) -> NormalizedMessage:
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
        content=content,
        timestamp=None,
        received_at="now",
        raw={},
        directly_addressed=directly_addressed,
    )


def _private_message(message_id: int, sender_id: int = 1, sender_email: str = "alice@example.com") -> NormalizedMessage:
    private_user_key = str(sender_id)
    return NormalizedMessage(
        realm_id="realm",
        message_id=message_id,
        stream_id=None,
        stream="private",
        stream_slug="private",
        topic="private",
        topic_hash=private_user_key,
        conversation_type="private",
        private_user_key=private_user_key,
        reply_required=True,
        sender_email=sender_email,
        sender_full_name=f"User {sender_id}",
        sender_id=sender_id,
        content="hi",
        timestamp=None,
        received_at="now",
        raw={},
    )


class BlockingCodex:
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            await self.release.wait()
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "scratchpad_op": {"op": "none", "content": ""},
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"thread-{self.calls}")


class FailingCodex:
    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        raise RuntimeError("codex failed")


class MemoryCheckingCodex:
    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        payload = {
            "should_reply": True,
            "reply_kind": "chat",
            "message_to_post": "Recorded.",
            "memory_ops": [
                {"op": "upsert", "scope": "conversation", "kind": "fact", "content": "Launch date is Friday"}
            ],
            "scratchpad_op": {"op": "none", "content": ""},
            "confidence": 0.8,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class SilentCodex:
    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "scratchpad_op": {"op": "none", "content": ""},
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class PromptCapturingCodex:
    def __init__(self) -> None:
        self.prompt = ""

    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        self.prompt = prompt
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "scratchpad_op": {"op": "none", "content": ""},
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class ThreadingCodex:
    def __init__(self) -> None:
        self.calls = 0

    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        self.calls += 1
        payload = {
            "should_reply": True,
            "reply_kind": "chat",
            "message_to_post": f"Reply {self.calls}",
            "memory_ops": [],
            "scratchpad_op": {"op": "none", "content": ""},
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"thread-{self.calls}")


class FakePoster:
    def __init__(self, memory_file: Path | None = None) -> None:
        self.posts: list[dict[str, str]] = []
        self.memory_file = memory_file

    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, str]:
        if self.memory_file is not None:
            assert "Launch date is Friday" in self.memory_file.read_text(encoding="utf-8")
        self.posts.append({"topic": message.topic, "content": content})
        return {"result": "success"}


class FailingPoster(FakePoster):
    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, str]:
        raise RuntimeError("post failed")


class FakeTypingNotifier:
    def __init__(self) -> None:
        self.events: list[tuple[str, int]] = []

    async def start(self, message: NormalizedMessage) -> None:
        self.events.append(("start", message.message_id))

    async def stop(self, message: NormalizedMessage) -> None:
        self.events.append(("stop", message.message_id))


def _typing(notifier: FakeTypingNotifier, *, enabled: bool = True) -> TypingStatusManager:
    return TypingStatusManager(notifier, enabled=enabled, refresh_seconds=60)


def test_typing_stop_waits_for_in_flight_refresh_start(tmp_path):
    class BlockingRefreshTypingNotifier:
        def __init__(self) -> None:
            self.events: list[tuple[str, int]] = []
            self.start_calls = 0
            self.refresh_started = asyncio.Event()
            self.release_refresh = asyncio.Event()

        async def start(self, message: NormalizedMessage) -> None:
            self.start_calls += 1
            self.events.append(("start", message.message_id))
            if self.start_calls == 2:
                self.refresh_started.set()
                await self.release_refresh.wait()

        async def stop(self, message: NormalizedMessage) -> None:
            self.events.append(("stop", message.message_id))

    async def scenario() -> None:
        notifier = BlockingRefreshTypingNotifier()
        typing = TypingStatusManager(notifier, refresh_seconds=0.01)

        async def use_typing() -> None:
            async with typing.active(_message(1)):
                await notifier.refresh_started.wait()

        task = asyncio.create_task(use_typing())
        await notifier.refresh_started.wait()
        await asyncio.sleep(0)

        assert notifier.events == [("start", 1), ("start", 1)]

        notifier.release_refresh.set()
        await task

        assert notifier.events == [("start", 1), ("start", 1), ("stop", 1)]

    asyncio.run(scenario())


def test_messages_for_active_topic_are_persisted_and_run_as_followup(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = BlockingCodex()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        first = asyncio.create_task(bot._handle_message(_message(1)))
        await codex.started.wait()
        await bot._handle_message(_message(2))
        pending_path = bot.storage.session_path(_message(1).session_key, "pending.json")
        assert pending_path.exists()
        codex.release.set()
        await first

        metadata = bot.storage.load_metadata(_message(1).session_key)
        assert codex.calls == 2
        assert metadata.last_processed_message_id == 2
        assert metadata.codex_thread_id == "thread-2"

    asyncio.run(scenario())


def test_private_message_starts_typing_before_blocked_codex_and_stops(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = BlockingCodex()
        typing = FakeTypingNotifier()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
            typing=_typing(typing),
        )

        task = asyncio.create_task(bot._handle_message(_private_message(1)))
        await codex.started.wait()
        assert typing.events == [("start", 1)]
        codex.release.set()
        await task

        assert typing.events[-1] == ("stop", 1)

    asyncio.run(scenario())


def test_stream_messages_type_for_directly_addressed_and_ordinary_messages(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        typing = FakeTypingNotifier()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=ThreadingCodex(),
            zulip=FakePoster(),
            typing=_typing(typing),
        )

        await bot._handle_message(_message(1, directly_addressed=True))
        await bot._handle_message(_message(2))

        assert typing.events == [("start", 1), ("stop", 1), ("start", 2), ("stop", 2)]

    asyncio.run(scenario())


def test_silent_stream_message_starts_and_stops_typing_without_posting(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        typing = FakeTypingNotifier()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=SilentCodex(),
            zulip=poster,
            typing=_typing(typing),
        )

        await bot._handle_message(_message(1))

        assert typing.events == [("start", 1), ("stop", 1)]
        assert poster.posts == []

    asyncio.run(scenario())


def test_dry_run_does_not_show_typing(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        typing = FakeTypingNotifier()
        bot = AgentLoop(
            config=_config(tmp_path, post_replies=False),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=ThreadingCodex(),
            zulip=FakePoster(),
            typing=_typing(typing),
        )

        await bot._handle_message(_private_message(1))

        assert typing.events == []

    asyncio.run(scenario())


def test_typing_stops_when_codex_or_posting_fails(tmp_path):
    async def codex_failure() -> list[tuple[str, int]]:
        workspace = tmp_path / "codex"
        initialize_workspace(workspace)
        typing = FakeTypingNotifier()
        bot = AgentLoop(
            config=_config(workspace),
            storage=WorkspaceStorage(workspace),
            instructions=InstructionLoader(workspace),
            memory=MemoryStore(workspace / "memory"),
            codex=FailingCodex(),
            zulip=FakePoster(),
            typing=_typing(typing),
        )

        await bot._handle_message(_private_message(1))
        return typing.events

    async def post_failure() -> list[tuple[str, int]]:
        workspace = tmp_path / "post"
        initialize_workspace(workspace)
        typing = FakeTypingNotifier()
        bot = AgentLoop(
            config=_config(workspace),
            storage=WorkspaceStorage(workspace),
            instructions=InstructionLoader(workspace),
            memory=MemoryStore(workspace / "memory"),
            codex=ThreadingCodex(),
            zulip=FailingPoster(),
            typing=_typing(typing),
        )

        await bot._handle_message(_private_message(1))
        return typing.events

    assert asyncio.run(codex_failure()) == [("start", 1), ("stop", 1)]
    assert asyncio.run(post_failure()) == [("start", 1), ("stop", 1)]


def test_pending_messages_preserve_direct_addressed(tmp_path):
    initialize_workspace(tmp_path)
    storage = WorkspaceStorage(tmp_path)
    message = _message(1, directly_addressed=True)

    storage.append_pending_messages(message.session_key, [message])

    assert storage.pop_pending_messages(message.session_key)[0].directly_addressed is True


def test_memory_ops_are_applied_before_posting(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        memory_file = tmp_path / "memory" / "stream-10-engineering" / "topic-topic123" / "MEMORY.md"
        poster = FakePoster(memory_file)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=MemoryCheckingCodex(),
            zulip=poster,
        )

        await bot._handle_message(_message(1, "remember this"))

        assert poster.posts == [{"topic": "Launch", "content": "Recorded."}]
        assert "Launch date is Friday" in memory_file.read_text(encoding="utf-8")

    asyncio.run(scenario())


def test_current_message_is_not_duplicated_in_rendered_prompt(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        storage = WorkspaceStorage(tmp_path)
        previous = _message(1, "previous context")
        current = _message(2, "current request")
        storage.append_message(previous)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(current)

        assert codex.prompt.count("current request") == 1
        assert "previous context" in codex.prompt

    asyncio.run(scenario())


def test_bot_authored_events_are_ignored_without_raw_event_storage(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        config = _config(tmp_path)
        bot = AgentLoop(
            config=config,
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=MemoryCheckingCodex(),
            zulip=FakePoster(),
        )
        event = {
            "type": "message",
            "message": {
                "id": 1,
                "type": "stream",
                "stream_id": 10,
                "display_recipient": "Engineering",
                "subject": "Launch",
                "sender_email": "bot@example.com",
                "sender_full_name": "Bot",
                "content": "self",
            },
        }

        result = await bot.enqueue_event(event)

        assert result.accepted is False
        assert bot.queue.empty()
        assert not (tmp_path / "state" / "raw").exists()
        assert "ignored bot-authored message" in next((tmp_path / "state" / "errors").glob("*.jsonl")).read_text(
            encoding="utf-8"
        )

    asyncio.run(scenario())


def test_private_message_posts_fallback_when_codex_is_silent(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=SilentCodex(),
            zulip=poster,
        )

        await bot._handle_message(_private_message(1))

        assert poster.posts == [{"topic": "private", "content": PRIVATE_REPLY_FALLBACK}]
        assert bot.storage.load_metadata(_private_message(1).session_key).last_processed_message_id == 1

    asyncio.run(scenario())


def test_private_messages_from_different_senders_use_different_sessions(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = ThreadingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )
        first = _private_message(1, sender_id=1, sender_email="alice@example.com")
        second = _private_message(2, sender_id=2, sender_email="bob@example.com")

        await bot._handle_message(first)
        await bot._handle_message(second)

        assert first.session_key.value == "zulip:realm:private:user:1"
        assert second.session_key.value == "zulip:realm:private:user:2"
        assert first.session_key.storage_id != second.session_key.storage_id
        assert bot.storage.load_metadata(first.session_key).codex_thread_id == "thread-1"
        assert bot.storage.load_metadata(second.session_key).codex_thread_id == "thread-2"
        assert [post["content"] for post in poster.posts] == ["Reply 1", "Reply 2"]

    asyncio.run(scenario())
