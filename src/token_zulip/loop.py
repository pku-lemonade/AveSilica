from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from dataclasses import dataclass
from typing import Any, Protocol

from .codex_adapter import CodexAdapter
from .config import BotConfig
from .instructions import InstructionLoader
from .memory import MemoryStore
from .models import AgentDecision, NormalizedMessage, NormalizedReaction, SessionKey
from .prompt import PromptBuilder, PromptParts
from .storage import WorkspaceStorage
from .typing_status import TypingStatusManager
from .uploads import MessageUploadProcessor
from .zulip_io import normalize_zulip_event, normalize_zulip_reaction_event

LOGGER = logging.getLogger(__name__)
PRIVATE_REPLY_FALLBACK = "I saw this, but couldn't produce a useful reply. Please try again."
CODEX_INSTRUCTION_MODE = "developer-v1"


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
        typing: TypingStatusManager | None = None,
        prompt_builder: PromptBuilder | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.instructions = instructions
        self.memory = memory
        self.codex = codex
        self.zulip = zulip
        self.typing = typing or TypingStatusManager(enabled=False)
        self.prompt_builder = prompt_builder or PromptBuilder(config.workspace_dir)
        self.uploads = MessageUploadProcessor(
            storage=storage,
            zulip=zulip,
            codex_cwd=config.codex_cwd,
            max_bytes=config.upload_max_bytes,
        )
        self.queue: asyncio.Queue[NormalizedMessage] = asyncio.Queue(maxsize=config.queue_limit)
        self._active_sessions: set[str] = set()
        self._active_guard = asyncio.Lock()

    async def enqueue_event(self, event: dict[str, Any]) -> EnqueueResult:
        reaction = normalize_zulip_reaction_event(event, self.config.realm_id)
        if reaction is not None:
            if self._reaction_from_bot(reaction):
                self.storage.log_ignored_event(event, "ignored bot-authored reaction")
                return EnqueueResult(False, "ignored bot-authored reaction", message_id=reaction.message_id)

            key = self.storage.apply_reaction(reaction)
            if key is None:
                self.storage.log_ignored_event(event, "ignored reaction for unknown message")
                return EnqueueResult(False, "ignored reaction for unknown message", message_id=reaction.message_id)
            return EnqueueResult(True, "recorded reaction", key.value, reaction.message_id)

        message = normalize_zulip_event(
            event,
            self.config.realm_id,
            bot_user_id=self.config.bot_user_id,
            bot_aliases=self.config.bot_aliases,
        )
        if message is None:
            reason = (
                "ignored unsupported reaction"
                if event.get("type") == "reaction"
                else "ignored unsupported message"
            )
            self.storage.log_ignored_event(event, reason)
            return EnqueueResult(False, reason)

        if self.config.bot_email and message.sender_email.casefold() == self.config.bot_email.casefold():
            self.storage.log_ignored_event(event, "ignored bot-authored message", message.session_key)
            return EnqueueResult(False, "ignored bot-authored message", message.session_key.value, message.message_id)

        self.storage.append_message(message)
        await self.queue.put(message)
        return EnqueueResult(True, "accepted", message.session_key.value, message.message_id)

    def _reaction_from_bot(self, reaction: NormalizedReaction) -> bool:
        if self.config.bot_user_id is not None and reaction.user_id == self.config.bot_user_id:
            return True
        if self.config.bot_email and reaction.user_email.casefold() == self.config.bot_email.casefold():
            return True
        return False

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
        self.storage.append_message(message)
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
        metadata = self.storage.load_metadata(key)
        async with AsyncExitStack() as stack:
            typing_started = False
            if self.typing.should_show_typing(first, post_replies=self.config.post_replies):
                await stack.enter_async_context(self.typing.active(first))
                typing_started = True

            messages = await self.uploads.process_messages(messages)
            for processed_message in messages:
                if processed_message.uploads:
                    self.storage.update_message(processed_message)

            first = messages[0]
            active_thread_id = (
                metadata.codex_thread_id
                if metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE and metadata.codex_thread_id
                else None
            )
            starting_new_thread = active_thread_id is None
            developer_instructions = None
            if starting_new_thread:
                developer_instructions = self.instructions.compose(
                    stream=first.stream,
                    topic_hash=first.topic_hash,
                    topic=first.topic,
                    stream_id=first.stream_id,
                    conversation_type=first.conversation_type,
                    private_user_key=first.private_user_key,
                )
                recent_context = self.storage.read_recent_messages(
                    key,
                    self.config.max_recent_messages,
                    exclude_message_ids={message.message_id for message in messages},
                )
            else:
                recent_context = []
            prompt = self.prompt_builder.build(
                PromptParts(
                    recent_context=recent_context,
                    current_messages=messages,
                )
            )

            codex_result = await self.codex.run_decision(
                prompt,
                active_thread_id,
                developer_instructions=developer_instructions,
            )
            if codex_result.thread_id:
                self.storage.set_codex_thread_state(
                    key,
                    thread_id=codex_result.thread_id,
                    instruction_mode=CODEX_INSTRUCTION_MODE,
                )

            decision = AgentDecision.from_json_text(codex_result.raw_text)
            message_to_post = self._message_to_post(first, decision)
            if typing_started and first.conversation_type == "stream" and not message_to_post:
                await stack.aclose()
                typing_started = False

            memory_applied = self.memory.apply_ops(
                key,
                decision.memory_ops,
                [message.message_id for message in messages],
            )

            post: dict[str, Any] | None = None
            if message_to_post:
                if self.config.post_replies:
                    post = self._post_summary(await self.zulip.post_reply(first, message_to_post), dry_run=False)
                else:
                    post = {"status": "dry_run", "dry_run": True}

            self.storage.log_turn(
                key=key,
                messages=messages,
                decision=decision,
                post=post,
                memory_applied=memory_applied,
            )
        self.storage.mark_processed(key, [message.message_id for message in messages])

    def _message_to_post(self, first: NormalizedMessage, decision: AgentDecision) -> str:
        content = decision.message_to_post.strip()
        if decision.should_reply and content:
            return content
        if first.reply_required:
            return content or PRIVATE_REPLY_FALLBACK
        return ""

    def _post_summary(self, post_result: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
        response = post_result.get("response") if isinstance(post_result.get("response"), dict) else post_result
        return {
            "status": str(response.get("result") or "unknown"),
            "dry_run": dry_run,
            "zulip_message_id": response.get("id"),
            "message": response.get("msg"),
        }
