from __future__ import annotations

import asyncio
import json
from pathlib import Path

from token_zulip.codex_adapter import CodexRunResult, CodexTurnWithForksResult, CodexWorkerSpec
from token_zulip.config import BotConfig
from token_zulip.instructions import InstructionLoader
from token_zulip.loop import CODEX_INSTRUCTION_MODE, PRIVATE_REPLY_FALLBACK, AgentLoop
from token_zulip.memory import MemoryStore
from token_zulip.models import MemoryOperation, NormalizedMessage, normalized_topic_hash
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
        codex_model="gpt-5.4",
        codex_reasoning_effort=None,
        codex_cwd=workspace,
        codex_sandbox="read-only",
        codex_approval_policy="never",
        max_recent_messages=20,
        queue_limit=8,
        worker_count=2,
        instruction_max_bytes=96_000,
        upload_max_bytes=25_000_000,
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


def _reaction_event(
    message_id: int,
    *,
    op: str = "add",
    emoji_name: str = "100",
    user_id: int = 2,
    user_email: str | None = None,
    user_full_name: str = "Bob",
) -> dict[str, object]:
    return {
        "type": "reaction",
        "op": op,
        "message_id": message_id,
        "emoji_name": emoji_name,
        "emoji_code": "1f4af",
        "reaction_type": "unicode_emoji",
        "user_id": user_id,
        "user_email": user_email or f"user{user_id}@example.com",
        "user_full_name": user_full_name,
    }


def _update_message_event() -> dict[str, object]:
    return {
        "type": "update_message",
        "message_ids": [1],
        "stream_id": 10,
        "stream_name": "Engineering",
        "orig_subject": "Launch",
        "subject": "Release",
        "propagate_mode": "change_all",
    }


def _worker_payload(payload: dict[str, object], kind: str) -> dict[str, object]:
    if kind == "memory":
        return {"memory_ops": payload.get("memory_ops", [])}
    if kind == "skill":
        return {"skill_ops": payload.get("skill_ops", [])}
    if kind == "schedule":
        return {"schedule_ops": payload.get("schedule_ops", [])}
    return {}


class ForkingCodexMixin:
    worker_payloads: dict[str, dict[str, object]] = {}
    worker_errors: dict[str, str] = {}

    async def ensure_thread(
        self,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        if not hasattr(self, "ensure_thread_ids"):
            self.ensure_thread_ids = []  # type: ignore[attr-defined]
        if not hasattr(self, "ensure_developer_instructions"):
            self.ensure_developer_instructions = []  # type: ignore[attr-defined]
        self.ensure_thread_ids.append(thread_id)  # type: ignore[attr-defined]
        self.ensure_developer_instructions.append(developer_instructions)  # type: ignore[attr-defined]
        resolved_thread_id = thread_id or f"thread-{getattr(self, 'calls', 0) + 1}"
        return CodexRunResult(raw_text="", thread_id=resolved_thread_id)

    async def run_turn_with_forks(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None,
        main_output_schema_path: Path,
        worker_specs: list[CodexWorkerSpec],
    ) -> CodexTurnWithForksResult:
        self.worker_prompts = {spec.kind: spec.prompt for spec in worker_specs}  # type: ignore[attr-defined]
        self.worker_developer_instructions = {  # type: ignore[attr-defined]
            spec.kind: spec.developer_instructions for spec in worker_specs
        }
        main = await self.run_decision(prompt, thread_id, developer_instructions=developer_instructions)
        main_payload = json.loads(main.raw_text)
        self._last_main_payload = main_payload  # type: ignore[attr-defined]
        workers = {
            spec.kind: CodexRunResult(
                raw_text=json.dumps(self.worker_payloads.get(spec.kind) or _worker_payload(main_payload, spec.kind)),
                thread_id=f"{main.thread_id}-{spec.kind}" if main.thread_id else f"fork-{spec.kind}",
            )
            for spec in worker_specs
            if spec.kind not in self.worker_errors
        }
        worker_kinds = {spec.kind for spec in worker_specs}
        worker_errors = {kind: error for kind, error in self.worker_errors.items() if kind in worker_kinds}
        return CodexTurnWithForksResult(main=main, workers=workers, worker_errors=worker_errors)

    async def run_worker_fork(
        self,
        parent_thread_id: str,
        worker_spec: CodexWorkerSpec,
    ) -> CodexRunResult:
        if not hasattr(self, "worker_prompts"):
            self.worker_prompts = {}  # type: ignore[attr-defined]
        if not hasattr(self, "worker_developer_instructions"):
            self.worker_developer_instructions = {}  # type: ignore[attr-defined]
        self.worker_prompts[worker_spec.kind] = worker_spec.prompt  # type: ignore[attr-defined]
        self.worker_developer_instructions[worker_spec.kind] = worker_spec.developer_instructions  # type: ignore[attr-defined]
        if worker_spec.kind in self.worker_errors:
            raise RuntimeError(self.worker_errors[worker_spec.kind])
        payload = self.worker_payloads.get(worker_spec.kind) or _worker_payload(
            getattr(self, "_last_main_payload", {}),
            worker_spec.kind,
        )
        return CodexRunResult(
            raw_text=json.dumps(payload),
            thread_id=f"{parent_thread_id}-{worker_spec.kind}",
        )


class BlockingCodex(ForkingCodexMixin):
    def __init__(self) -> None:
        self.calls = 0
        self.started = asyncio.Event()
        self.release = asyncio.Event()

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        self.calls += 1
        if self.calls == 1:
            self.started.set()
            await self.release.wait()
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"thread-{self.calls}")


class FailingCodex(ForkingCodexMixin):
    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        raise RuntimeError("codex failed")


class MemoryCheckingCodex(ForkingCodexMixin):
    worker_payloads = {
        "memory": {
            "memory_ops": [
                {"op": "add", "scope": "conversation", "content": "Launch date is Friday", "old_text": ""}
            ]
        }
    }

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        payload = {
            "should_reply": True,
            "reply_kind": "chat",
            "message_to_post": "Recorded.",
            "confidence": 0.8,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class SilentMemoryCodex(ForkingCodexMixin):
    worker_payloads = {
        "memory": {
            "memory_ops": [
                {"op": "add", "scope": "conversation", "content": "Silent memory fact", "old_text": ""}
            ]
        }
    }

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class SilentCodex(ForkingCodexMixin):
    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class PromptCapturingCodex(ForkingCodexMixin):
    def __init__(self) -> None:
        self.prompt = ""
        self.prompts: list[str] = []
        self.thread_ids: list[str | None] = []
        self.developer_instructions: list[str | None] = []

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        self.prompt = prompt
        self.prompts.append(prompt)
        self.thread_ids.append(thread_id)
        self.developer_instructions.append(developer_instructions)
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id="thread-1")


class ThreadingCodex(ForkingCodexMixin):
    def __init__(self) -> None:
        self.calls = 0
        self.prompts: list[str] = []
        self.thread_ids: list[str | None] = []
        self.developer_instructions: list[str | None] = []

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        self.calls += 1
        self.prompts.append(prompt)
        self.thread_ids.append(thread_id)
        self.developer_instructions.append(developer_instructions)
        payload = {
            "should_reply": True,
            "reply_kind": "chat",
            "message_to_post": f"Reply {self.calls}",
            "memory_ops": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"thread-{self.calls}")


class MissingThreadCodex(ForkingCodexMixin):
    def __init__(self) -> None:
        self.calls = 0
        self.thread_ids: list[str | None] = []
        self.developer_instructions: list[str | None] = []

    async def run_turn_with_forks(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None,
        main_output_schema_path: Path,
        worker_specs: list[CodexWorkerSpec],
    ) -> CodexTurnWithForksResult:
        self.thread_ids.append(thread_id)
        self.developer_instructions.append(developer_instructions)
        if thread_id == "missing-thread":
            raise RuntimeError("JSON-RPC error -32600: no rollout found for thread id missing-thread")
        return await super().run_turn_with_forks(
            prompt,
            thread_id,
            developer_instructions=developer_instructions,
            main_output_schema_path=main_output_schema_path,
            worker_specs=worker_specs,
        )

    async def ensure_thread(
        self,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        self.thread_ids.append(thread_id)
        self.developer_instructions.append(developer_instructions)
        if thread_id == "missing-thread":
            raise RuntimeError("JSON-RPC error -32600: no rollout found for thread id missing-thread")
        return CodexRunResult(raw_text="", thread_id=thread_id or f"recovered-thread-{self.calls + 1}")

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        self.calls += 1
        payload = {
            "should_reply": False,
            "reply_kind": "silent",
            "message_to_post": "",
            "memory_ops": [],
            "confidence": 0.9,
        }
        return CodexRunResult(raw_text=json.dumps(payload), thread_id=f"recovered-thread-{self.calls}")


class FakePoster:
    def __init__(self, memory_file: Path | None = None) -> None:
        self.posts: list[dict[str, str]] = []
        self.memory_file = memory_file

    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, str]:
        if self.memory_file is not None:
            assert "Launch date is Friday" in self.memory_file.read_text(encoding="utf-8")
        self.posts.append({"topic": message.topic, "content": content})
        return {"result": "success"}


class FakeUploadPoster(FakePoster):
    def __init__(self, typing: FakeTypingNotifier, codex_cwd: Path) -> None:
        super().__init__()
        self.typing = typing
        self.codex_cwd = codex_cwd

    async def download_upload(self, upload_path: str, destination: Path, max_bytes: int) -> dict[str, object]:
        assert self.typing.events == [("start", 1)]
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"image")
        assert destination.is_relative_to(self.codex_cwd)
        return {"status": "downloaded", "content_type": "image/png", "byte_size": 5}


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
        assert metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE

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
        assert bot.codex.thread_ids == ["thread-1", "thread-1"]
        assert bot.codex.developer_instructions == [None, None]
        assert bot.codex.ensure_developer_instructions[0] is not None
        assert bot.codex.ensure_developer_instructions[1] is None

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
        memory_file = tmp_path / "memory" / "stream-engineering-10" / "topic-launch-topic123" / "MEMORY.md"
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

        assert poster.posts == [
            {
                "topic": "Launch",
                "content": (
                    "Recorded.\n\n"
                    "Memory updated: added conversation memory: Launch date is Friday"
                ),
            }
        ]
        assert "Launch date is Friday" in memory_file.read_text(encoding="utf-8")

    asyncio.run(scenario())


def test_silent_decision_with_memory_change_posts_acknowledgement(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=SilentMemoryCodex(),
            zulip=poster,
        )

        await bot._handle_message(_message(1, "remember silently"))

        assert poster.posts == [
            {
                "topic": "Launch",
                "content": "Memory updated: added conversation memory: Silent memory fact",
            }
        ]

    asyncio.run(scenario())


def test_dry_run_records_memory_acknowledgement_without_posting(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path, post_replies=False),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=SilentMemoryCodex(),
            zulip=FakePoster(),
        )
        message = _message(1, "remember silently")

        await bot._handle_message(message)

        turns = storage.session_path(message.session_key, "turns.jsonl").read_text(encoding="utf-8").splitlines()
        record = json.loads(turns[-1])
        assert record["memory_acknowledgement"] == "Memory updated: added conversation memory: Silent memory fact"
        assert record["post"]["dry_run"] is True
        assert record["post"]["message_to_post"] == record["memory_acknowledgement"]
        pending = storage.read_pending_posted_bot_updates(message.session_key)
        assert pending[-1]["source"] == "conversation_turn"
        assert pending[-1]["content"] == record["memory_acknowledgement"]

    asyncio.run(scenario())


def test_memory_acknowledgement_formats_applied_add_replace_and_remove(tmp_path):
    initialize_workspace(tmp_path)
    bot = AgentLoop(
        config=_config(tmp_path),
        storage=WorkspaceStorage(tmp_path),
        instructions=InstructionLoader(tmp_path),
        memory=MemoryStore(tmp_path / "memory"),
        codex=SilentCodex(),
        zulip=FakePoster(),
    )

    assert bot._memory_acknowledgement(
        [
            {"op": "add", "scope": "conversation", "status": "applied", "content": "New fact"},
            {
                "op": "replace",
                "scope": "channel",
                "status": "applied",
                "old_text": "Old fact",
                "content": "New channel fact",
            },
            {"op": "remove", "scope": "global", "status": "applied", "content": "Forgotten fact"},
            {"op": "add", "scope": "conversation", "status": "skipped", "content": "Duplicate"},
        ]
    ) == (
        "Memory updated:\n"
        "- added conversation memory: New fact\n"
        '- replaced channel memory: "Old fact" -> "New channel fact"\n'
        "- forgot global memory: Forgotten fact"
    )


def test_current_message_is_not_duplicated_and_recent_context_is_not_rendered(tmp_path):
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
        assert "previous context" not in codex.prompt
        assert "Instruction Layers" not in codex.prompt
        assert "Scoped Memory" not in codex.prompt
        assert codex.thread_ids == ["thread-1"]
        assert codex.developer_instructions[0] is None
        assert codex.ensure_developer_instructions[0] is not None
        assert "Codex Thread Contract" in codex.ensure_developer_instructions[0]
        assert "references/schedule/system.md" not in codex.ensure_developer_instructions[0]

    asyncio.run(scenario())


def test_resumed_thread_gets_no_recent_context_or_developer_instructions(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(_message(1, "first context"))
        await bot._handle_message(_message(2, "second request"))

        assert codex.thread_ids == ["thread-1", "thread-1"]
        assert codex.developer_instructions == [None, None]
        assert codex.ensure_developer_instructions[0] is not None
        assert codex.ensure_developer_instructions[1] is None
        assert "first context" not in codex.prompts[1]
        assert "second request" in codex.prompts[1]

    asyncio.run(scenario())


def test_legacy_thread_without_instruction_marker_starts_fresh_without_recent_context(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        storage = WorkspaceStorage(tmp_path)
        previous = _message(1, "legacy context")
        current = _message(2, "current after migration")
        storage.append_message(previous)
        storage.set_codex_thread_id(previous.session_key, "legacy-thread")
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(current)

        metadata = storage.load_metadata(current.session_key)
        assert codex.thread_ids == ["thread-1"]
        assert codex.developer_instructions[0] is None
        assert codex.ensure_developer_instructions[0] is not None
        assert "legacy context" not in codex.prompt
        assert metadata.codex_thread_id == "thread-1"
        assert metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE

    asyncio.run(scenario())


def test_stale_reply_instruction_mode_starts_fresh_thread(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        storage = WorkspaceStorage(tmp_path)
        message = _message(1, "current after instruction bump")
        storage.append_message(message)
        storage.set_codex_thread_state(
            message.session_key,
            thread_id="old-v2-thread",
            instruction_mode="reply-session-v2",
        )
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(message)

        metadata = storage.load_metadata(message.session_key)
        assert codex.ensure_thread_ids == [None]
        assert codex.ensure_developer_instructions[0] is not None
        assert metadata.codex_thread_id == "thread-1"
        assert metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE

    asyncio.run(scenario())


def test_missing_codex_rollout_restarts_marked_reply_thread(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = MissingThreadCodex()
        storage = WorkspaceStorage(tmp_path)
        message = _message(1, "sili remind me tomorrow", directly_addressed=True)
        storage.append_message(message)
        storage.set_codex_thread_state(
            message.session_key,
            thread_id="missing-thread",
            instruction_mode=CODEX_INSTRUCTION_MODE,
        )
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(message)

        metadata = storage.load_metadata(message.session_key)
        errors = (tmp_path / "records" / "errors").glob("*.jsonl")
        error_records = [
            json.loads(line)
            for path in errors
            for line in path.read_text(encoding="utf-8").splitlines()
        ]
        assert codex.thread_ids == ["missing-thread", None]
        assert codex.developer_instructions[0] is None
        assert codex.developer_instructions[1] is not None
        assert metadata.codex_thread_id == "recovered-thread-1"
        assert metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE
        assert metadata.last_processed_message_id == 1
        assert any(record.get("kind") == "codex_thread_restarted" for record in error_records)

    asyncio.run(scenario())


def test_memory_entries_are_conditionally_injected_into_codex_prompt(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        memory = MemoryStore(tmp_path / "memory")
        message = _message(1, "hello")
        memory.apply_ops(
            message.session_key,
            [
                MemoryOperation(
                    op="add",
                    scope="conversation",
                    content="Launch date is Friday",
                )
            ],
        )
        storage = WorkspaceStorage(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=memory,
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(message)
        await bot._handle_message(_message(2, "next turn"))

        assert "Scoped Memory" in codex.prompts[0]
        assert "Launch date is Friday" in codex.prompts[0]
        assert "remembered background" not in codex.prompts[0].casefold()
        assert "memory worker may correct stale memory" not in codex.prompts[0]
        assert "Treat scoped memory and posted bot updates as background context" in (
            codex.ensure_developer_instructions[0] or ""
        )
        assert "Launch date is Friday" not in (codex.ensure_developer_instructions[0] or "")
        assert "Scoped Memory" not in codex.prompts[1]
        assert storage.load_metadata(message.session_key).last_injected_memory_hash is not None

    asyncio.run(scenario())


def test_posted_bot_update_is_injected_once_on_next_turn(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        codex = ThreadingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )
        first = _message(1, "first request")
        second = _message(2, "second request")

        await bot._handle_message(first)
        pending = storage.read_pending_posted_bot_updates(first.session_key)
        assert len(pending) == 1
        assert pending[0]["content"] == "Reply 1"

        await bot._handle_message(second)

        assert "Posted Bot Updates" in codex.prompts[1]
        assert "Reply 1" in codex.prompts[1]
        assert "Reply 1" in codex.worker_prompts["memory"]
        assert "Reply 1" in codex.worker_prompts["schedule"]
        remaining = storage.read_pending_posted_bot_updates(first.session_key)
        assert len(remaining) == 1
        assert remaining[0]["content"] == "Reply 2"

    asyncio.run(scenario())


def test_clear_resets_codex_thread_and_starts_fresh_on_next_message(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        codex = ThreadingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )
        first = _message(1, "first request", directly_addressed=True)
        clear = _message(2, "SILI CLEAR", directly_addressed=True)
        after = _message(3, "after clear", directly_addressed=True)

        await bot._handle_message(first)
        assert storage.read_pending_posted_bot_updates(first.session_key)[0]["content"] == "Reply 1"

        await bot._handle_message(clear)

        metadata = storage.load_metadata(first.session_key)
        assert codex.calls == 1
        assert metadata.codex_thread_id is None
        assert metadata.codex_instruction_mode is None
        assert metadata.last_injected_memory_hash is None
        assert metadata.cleared_at_message_id == 2
        assert metadata.previous_codex_thread_id == "thread-1"
        assert storage.read_pending_posted_bot_updates(first.session_key) == []
        assert poster.posts[-1]["content"] == "Cleared. The next normal message starts a fresh Codex thread."

        await bot._handle_message(after)

        assert codex.calls == 2
        assert codex.thread_ids == ["thread-1", "thread-2"]
        assert codex.developer_instructions == [None, None]
        assert codex.ensure_developer_instructions[1] is not None
        assert "Posted Bot Updates" not in codex.prompts[1]
        assert "Reply 1" not in codex.prompts[1]
        assert storage.load_metadata(first.session_key).codex_thread_id == "thread-2"

    asyncio.run(scenario())


def test_clear_in_pending_messages_splits_normal_turn_batches(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = BlockingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )

        first = asyncio.create_task(bot._handle_message(_message(1, "first", directly_addressed=True)))
        await codex.started.wait()
        await bot._handle_message(_message(2, "before clear", directly_addressed=True))
        await bot._handle_message(_message(3, "sili clear", directly_addressed=True))
        await bot._handle_message(_message(4, "after clear", directly_addressed=True))
        codex.release.set()
        await first

        metadata = bot.storage.load_metadata(_message(1).session_key)
        assert codex.calls == 3
        assert metadata.last_processed_message_id == 4
        assert metadata.codex_thread_id == "thread-3"
        assert metadata.cleared_at_message_id == 3
        assert poster.posts == [
            {"topic": "Launch", "content": "Cleared. The next normal message starts a fresh Codex thread."}
        ]

    asyncio.run(scenario())


def test_status_reports_silent_decision_without_codex_call(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )

        await bot._handle_message(_message(1, "hello"))
        await bot._handle_message(_message(2, "sili STATUS"))

        assert len(codex.prompts) == 1
        status = poster.posts[-1]["content"]
        assert "- Decision: silent (0.90)" in status
        assert "- Why: chose not to reply; no runtime error" in status
        assert '- Message: Alice: "hello"' in status
        assert "- Errors: none" in status
        assert "Thread:" not in status

    asyncio.run(scenario())


def test_status_reports_posted_reply(tmp_path):
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

        await bot._handle_message(_message(1, "hello", directly_addressed=True))
        await bot._handle_message(_message(2, "Sili status", directly_addressed=True))

        assert codex.calls == 1
        status = poster.posts[-1]["content"]
        assert "- Decision: chat (0.90)" in status
        assert '- Posted: "Reply 1"' in status
        assert "- Errors: none" in status

    asyncio.run(scenario())


def test_status_reports_worker_error_surface(tmp_path):
    class SkillErrorCodex(ThreadingCodex):
        worker_errors = {"skill": "concurrent turn consumer"}

    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = SkillErrorCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
        )

        await bot._handle_message(_message(1, "hello", directly_addressed=True))
        await bot._handle_message(_message(2, "sili status", directly_addressed=True))

        status = poster.posts[-1]["content"]
        assert "- Decision: chat (0.90)" in status
        assert "- Errors: skill: concurrent turn consumer" in status

    asyncio.run(scenario())


def test_status_reports_reply_failure_without_calling_codex_again(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=FailingCodex(),
            zulip=poster,
        )

        await bot._handle_message(_message(1, "hello", directly_addressed=True))
        await bot._handle_message(_message(2, "sili status", directly_addressed=True))

        status = poster.posts[-1]["content"]
        assert "- Decision: failed before reply" in status
        assert "- Why: reply failed before a decision was logged" in status
        assert '- Message: Alice: "hello"' in status
        assert "- Errors: reply: RuntimeError('codex failed')" in status

    asyncio.run(scenario())


def test_status_after_clear_reports_fresh_next_message(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=ThreadingCodex(),
            zulip=poster,
        )

        await bot._handle_message(_message(1, "hello", directly_addressed=True))
        await bot._handle_message(_message(2, "sili clear", directly_addressed=True))
        await bot._handle_message(_message(3, "sili status", directly_addressed=True))

        status = poster.posts[-1]["content"]
        assert "- Decision: cleared" in status
        assert "- Why: next normal message starts fresh" in status
        assert '- Message: Alice: "sili clear"' in status
        assert "- Errors: none" in status

    asyncio.run(scenario())


def test_unaddressed_stream_clear_is_not_control_command(tmp_path):
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

        await bot._handle_message(_message(1, "clear"))

        assert codex.calls == 1
        assert poster.posts == [{"topic": "Launch", "content": "Reply 1"}]
        assert bot.storage.load_metadata(_message(1).session_key).codex_thread_id == "thread-1"

    asyncio.run(scenario())


def test_memory_injection_hash_is_not_saved_when_codex_fails(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        memory = MemoryStore(tmp_path / "memory")
        message = _message(1, "hello")
        memory.apply_ops(
            message.session_key,
            [MemoryOperation(op="add", scope="conversation", content="Launch date is Friday")],
        )
        storage = WorkspaceStorage(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=memory,
            codex=FailingCodex(),
            zulip=FakePoster(),
        )

        await bot._handle_message(message)

        assert storage.load_metadata(message.session_key).last_injected_memory_hash is None

    asyncio.run(scenario())


def test_removed_memory_after_prior_injection_sends_stale_update(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        codex = PromptCapturingCodex()
        memory = MemoryStore(tmp_path / "memory")
        first = _message(1, "hello")
        memory.apply_ops(
            first.session_key,
            [MemoryOperation(op="add", scope="conversation", content="Launch date is Friday")],
        )
        storage = WorkspaceStorage(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=memory,
            codex=codex,
            zulip=FakePoster(),
        )

        await bot._handle_message(first)
        memory.apply_ops(
            first.session_key,
            [MemoryOperation(op="remove", scope="conversation", old_text="Launch date")],
        )
        await bot._handle_message(_message(2, "next turn"))

        assert "Launch date is Friday" in codex.prompts[0]
        assert "# Scoped Memory\n\n- Empty" in codex.prompts[1]
        assert "If a `Scoped Memory` runtime section is empty" in (codex.ensure_developer_instructions[0] or "")
        assert storage.load_metadata(first.session_key).last_injected_memory_hash is None

    asyncio.run(scenario())


def test_uploads_are_downloaded_after_typing_starts_and_rewritten_in_prompt(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        typing = FakeTypingNotifier()
        codex = PromptCapturingCodex()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakeUploadPoster(typing, tmp_path),
            typing=_typing(typing),
        )

        await bot._handle_message(_message(1, "inspect ![diagram](/user_uploads/7/Ab/diagram.png)"))

        assert typing.events == [("start", 1), ("stop", 1)]
        assert "![diagram](records/stream-engineering-10/topic-launch-topic123/uploads/1/01-diagram.png)" in codex.prompt
        message_record = bot.storage.read_recent_messages(_message(1).session_key, 1)[0]
        assert message_record["uploads"][0]["status"] == "downloaded"

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
        assert not (tmp_path / "state").exists()
        assert "ignored bot-authored message" in next((tmp_path / "records" / "errors").glob("*.jsonl")).read_text(
            encoding="utf-8"
        )

    asyncio.run(scenario())


def test_reaction_event_updates_message_without_codex_turn(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        storage.append_message(_message(1, "looks good"))
        typing = FakeTypingNotifier()
        codex = PromptCapturingCodex()
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=poster,
            typing=_typing(typing),
        )

        result = await bot.enqueue_event(_reaction_event(1))
        await bot.drain_once()

        assert result.accepted is True
        assert result.reason == "recorded reaction"
        assert bot.queue.empty()
        assert codex.prompts == []
        assert typing.events == []
        assert poster.posts == []
        record = storage.read_recent_messages(_message(1).session_key, 1)[0]
        assert record["reactions"][0]["emoji_name"] == "100"
        assert record["reaction_events"][0]["op"] == "add"

    asyncio.run(scenario())


def test_reaction_remove_deletes_active_reaction_without_queueing(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        storage.append_message(_message(1, "looks good"))
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=PromptCapturingCodex(),
            zulip=FakePoster(),
        )

        await bot.enqueue_event(_reaction_event(1, op="add"))
        result = await bot.enqueue_event(_reaction_event(1, op="remove"))

        assert result.accepted is True
        assert bot.queue.empty()
        record = storage.read_recent_messages(_message(1).session_key, 1)[0]
        assert "reactions" not in record
        assert [event["op"] for event in record["reaction_events"]] == ["add", "remove"]

    asyncio.run(scenario())


def test_unknown_reaction_event_is_ignored_without_queueing(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=PromptCapturingCodex(),
            zulip=FakePoster(),
        )

        result = await bot.enqueue_event(_reaction_event(404))

        assert result.accepted is False
        assert result.reason == "ignored reaction for unknown message"
        assert result.message_id == 404
        assert bot.queue.empty()
        error_text = next((tmp_path / "records" / "errors").glob("*.jsonl")).read_text(encoding="utf-8")
        assert "ignored reaction for unknown message" in error_text

    asyncio.run(scenario())


def test_bot_authored_reaction_event_is_ignored(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        storage.append_message(_message(1, "looks good"))
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=PromptCapturingCodex(),
            zulip=FakePoster(),
        )

        result = await bot.enqueue_event(
            _reaction_event(1, user_id=99, user_email="bot@example.com", user_full_name="Bot")
        )

        assert result.accepted is False
        assert result.reason == "ignored bot-authored reaction"
        assert bot.queue.empty()
        assert "reactions" not in storage.read_recent_messages(_message(1).session_key, 1)[0]

    asyncio.run(scenario())


def test_active_reaction_on_prior_message_is_not_replayed_as_recent_context(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        previous = _message(1, "previous context")
        current = _message(2, "current request")
        storage.append_message(previous)
        codex = PromptCapturingCodex()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        await bot.enqueue_event(_reaction_event(1, user_full_name="Bob"))
        await bot._handle_message(current)

        assert "previous context Reactions: Bob 100" not in codex.prompt
        assert "current request" in codex.prompt

    asyncio.run(scenario())


def test_update_message_move_event_relocates_records_without_codex_turn(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        source = _message(1, "moved context")
        source = NormalizedMessage(**{**source.__dict__, "topic_hash": normalized_topic_hash("Launch")})
        storage.append_message(source)
        codex = PromptCapturingCodex()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=storage,
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=codex,
            zulip=FakePoster(),
        )

        result = await bot.enqueue_event(_update_message_event())

        assert result.accepted is True
        assert bot.queue.empty()
        assert codex.prompts == []
        release = _message(2)
        release = NormalizedMessage(
            **{
                **release.__dict__,
                "topic": "Release",
                "topic_hash": normalized_topic_hash("Release"),
                "content": "new message",
            }
        )
        await bot._handle_message(release)
        assert "moved context" not in codex.prompt
        assert "new message" in codex.prompt

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


def test_private_memory_only_decision_posts_acknowledgement_without_fallback(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        poster = FakePoster()
        bot = AgentLoop(
            config=_config(tmp_path),
            storage=WorkspaceStorage(tmp_path),
            instructions=InstructionLoader(tmp_path),
            memory=MemoryStore(tmp_path / "memory"),
            codex=SilentMemoryCodex(),
            zulip=poster,
        )

        await bot._handle_message(_private_message(1))

        assert poster.posts == [
            {
                "topic": "private",
                "content": "Memory updated: added conversation memory: Silent memory fact",
            }
        ]

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
