from __future__ import annotations

import asyncio
import json
from pathlib import Path

from token_zulip.codex_adapter import CodexRunResult
from token_zulip.config import BotConfig
from token_zulip.instructions import InstructionLoader
from token_zulip.loop import AgentLoop
from token_zulip.memory import MemoryStore
from token_zulip.models import NormalizedMessage
from token_zulip.storage import WorkspaceStorage
from token_zulip.workspace import initialize_workspace


def _config(workspace: Path, *, post_replies: bool = True) -> BotConfig:
    return BotConfig(
        workspace_dir=workspace,
        zulip_config_file=None,
        realm_id="realm",
        bot_email="bot@example.com",
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
    )


def _message(message_id: int, content: str = "hello") -> NormalizedMessage:
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
            "memory_updates": [],
            "scratchpad_updates": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"thread-{self.calls}")


class MemoryCheckingCodex:
    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        payload = {
            "should_reply": True,
            "reply_kind": "chat",
            "message_to_post": "Recorded.",
            "memory_updates": [
                {"file": "durable.md", "mode": "append", "content": "- Launch date is Friday"}
            ],
            "scratchpad_updates": [],
            "confidence": 0.8,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class FakePoster:
    def __init__(self, memory_file: Path | None = None) -> None:
        self.posts: list[dict[str, str]] = []
        self.memory_file = memory_file

    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, str]:
        if self.memory_file is not None:
            assert "Launch date is Friday" in self.memory_file.read_text(encoding="utf-8")
        self.posts.append({"topic": message.topic, "content": content})
        return {"result": "success"}


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
        pending_path = bot.storage.session_path(_message(1).session_key, "pending.jsonl")
        assert pending_path.exists()
        codex.release.set()
        await first

        metadata = bot.storage.load_metadata(_message(1).session_key)
        assert codex.calls == 2
        assert metadata.last_processed_message_id == 2
        assert metadata.codex_thread_id == "thread-2"

    asyncio.run(scenario())


def test_memory_updates_are_applied_before_posting(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        memory_file = tmp_path / "memory" / "durable.md"
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


def test_bot_authored_events_are_ignored_but_raw_event_is_stored(tmp_path):
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
        assert list((tmp_path / "state" / "raw").glob("*.jsonl"))

    asyncio.run(scenario())
