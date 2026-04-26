from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any, Protocol

from .codex_adapter import CodexAdapter
from .config import BotConfig
from .instructions import InstructionLoader
from .memory import MemoryStore
from .models import AgentDecision, NormalizedMessage, SessionKey
from .prompt import PromptBuilder, PromptParts
from .storage import WorkspaceStorage
from .zulip_io import normalize_zulip_event

LOGGER = logging.getLogger(__name__)


class ZulipPoster(Protocol):
    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class EnqueueResult:
    accepted: bool
    reason: str
    session_key: str | None = None
    message_id: int | None = None


class AgentLoop:
    def __init__(
        self,
        *,
        config: BotConfig,
        storage: WorkspaceStorage,
        instructions: InstructionLoader,
        memory: MemoryStore,
        codex: CodexAdapter,
        zulip: ZulipPoster,
        prompt_builder: PromptBuilder | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.instructions = instructions
        self.memory = memory
        self.codex = codex
        self.zulip = zulip
        self.prompt_builder = prompt_builder or PromptBuilder()
        self.queue: asyncio.Queue[NormalizedMessage] = asyncio.Queue(maxsize=config.queue_limit)
        self._active_sessions: set[str] = set()
        self._active_guard = asyncio.Lock()

    async def enqueue_event(self, event: dict[str, Any]) -> EnqueueResult:
        self.storage.append_raw_event(event)
        message = normalize_zulip_event(event, self.config.realm_id)
        if message is None:
            return EnqueueResult(False, "ignored non-channel message")

        if self.config.bot_email and message.sender_email.casefold() == self.config.bot_email.casefold():
            return EnqueueResult(False, "ignored bot-authored message", message.session_key.value, message.message_id)

        self.storage.append_transcript(message)
        await self.queue.put(message)
        return EnqueueResult(True, "accepted", message.session_key.value, message.message_id)

    async def run_workers(self) -> None:
        workers = [
            asyncio.create_task(self._worker_loop(worker_id), name=f"token-zulip-worker-{worker_id}")
            for worker_id in range(max(1, self.config.worker_count))
        ]
        await asyncio.gather(*workers)

    async def drain_once(self) -> None:
        while not self.queue.empty():
            message = await self.queue.get()
            try:
                await self._handle_message(message)
            finally:
                self.queue.task_done()

    async def _worker_loop(self, worker_id: int) -> None:
        while True:
            message = await self.queue.get()
            try:
                await self._handle_message(message)
            except Exception:
                LOGGER.exception("Worker %s failed while handling message %s", worker_id, message.message_id)
                self.storage.log_error(
                    message.session_key,
                    {
                        "kind": "worker_exception",
                        "message_id": message.message_id,
                    },
                )
            finally:
                self.queue.task_done()

    async def _handle_message(self, message: NormalizedMessage) -> None:
        key = message.session_key
        metadata = self.storage.load_metadata(key)
        if (
            metadata.last_processed_message_id is not None
            and message.message_id <= metadata.last_processed_message_id
        ):
            return

        async with self._active_guard:
            if key.value in self._active_sessions:
                self.storage.append_pending_messages(key, [message])
                return
            self._active_sessions.add(key.value)

        try:
            await self._run_turn(key, [message])
            while True:
                pending = self.storage.pop_pending_messages(key)
                pending = self._filter_unprocessed(key, pending)
                if not pending:
                    break
                await self._run_turn(key, pending)
        except Exception as exc:
            LOGGER.exception("Agent turn failed for %s", key.value)
            self.storage.log_error(
                key,
                {
                    "kind": "turn_exception",
                    "error": repr(exc),
                    "message_ids": [message.message_id],
                },
            )
        finally:
            async with self._active_guard:
                self._active_sessions.discard(key.value)

    def _filter_unprocessed(self, key: SessionKey, messages: list[NormalizedMessage]) -> list[NormalizedMessage]:
        metadata = self.storage.load_metadata(key)
        if metadata.last_processed_message_id is None:
            return messages
        return [message for message in messages if message.message_id > metadata.last_processed_message_id]

    async def _run_turn(self, key: SessionKey, messages: list[NormalizedMessage]) -> None:
        if not messages:
            return

        first = messages[0]
        instructions = self.instructions.compose(
            stream=first.stream,
            topic_hash=first.topic_hash,
            role=self.config.role,
        )
        memory_text = self.memory.render_selected(key)
        recent_context = self.storage.read_recent_transcript(key, self.config.max_recent_messages)
        prompt = self.prompt_builder.build(
            PromptParts(
                instructions=instructions,
                memory=memory_text,
                recent_context=recent_context,
                current_messages=messages,
            )
        )

        metadata = self.storage.load_metadata(key)
        codex_result = await self.codex.run_decision(prompt, metadata.codex_thread_id)
        if codex_result.thread_id:
            self.storage.set_codex_thread_id(key, codex_result.thread_id)

        decision = AgentDecision.from_json_text(codex_result.raw_text)
        memory_applied = self.memory.apply_updates(key, decision.memory_updates)
        self.storage.apply_scratchpad_updates(key, decision.scratchpad_updates)

        post_result: dict[str, Any] | None = None
        if decision.should_reply and decision.message_to_post.strip():
            if self.config.post_replies:
                post_result = await self.zulip.post_reply(first, decision.message_to_post.strip())
            else:
                post_result = {"dry_run": True, "content": decision.message_to_post.strip()}

        self.storage.log_outbound(
            key=key,
            messages=messages,
            decision=decision,
            codex_raw_text=codex_result.raw_text,
            post_result=post_result,
            memory_applied=memory_applied,
        )
        self.storage.mark_processed(key, [message.message_id for message in messages])

