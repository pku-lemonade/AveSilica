from __future__ import annotations

import asyncio
import hashlib
import logging
import re
from contextlib import AsyncExitStack
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol

from .codex_adapter import CodexAdapter, CodexRunResult, CodexWorkerSpec
from .config import BotConfig
from .control import ControlCommand, parse_control_command
from .instructions import InstructionLoader
from .memory import MemoryStore
from .models import (
    AgentDecision,
    MemoryDecision,
    NormalizedMessage,
    NormalizedReaction,
    ReplyDecision,
    ScheduleDecision,
    SessionKey,
    SkillDecision,
)
from .prompt import PromptBuilder, PromptParts
from .schedules import ScheduleStore, utc_now, zoneinfo_for
from .skills import SkillStore
from .storage import SessionMetadata, WorkspaceStorage
from .typing_status import TypingStatusManager
from .uploads import MessageUploadProcessor
from .workspace import (
    MEMORY_DECISION_SCHEMA_FILE,
    MEMORY_WORKER_USER_PROMPT_FILE,
    REPLY_DECISION_SCHEMA_FILE,
    REPLY_TURN_USER_PROMPT_FILE,
    SCHEDULED_JOB_DECISION_SCHEMA_FILE,
    SCHEDULED_JOB_USER_PROMPT_FILE,
    SCHEDULE_DECISION_SCHEMA_FILE,
    SCHEDULE_WORKER_USER_PROMPT_FILE,
    SKILL_DECISION_SCHEMA_FILE,
    SKILL_WORKER_USER_PROMPT_FILE,
)
from .zulip_io import normalize_zulip_event, normalize_zulip_reaction_event, normalize_zulip_update_message_event

LOGGER = logging.getLogger(__name__)
PRIVATE_REPLY_FALLBACK = "I saw this, but couldn't produce a useful reply. Please try again."
CODEX_INSTRUCTION_MODE = "reply-session-v3"


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
        skills: SkillStore | None = None,
        schedules: ScheduleStore | None = None,
    ) -> None:
        self.config = config
        self.storage = storage
        self.instructions = instructions
        self.memory = memory
        self.codex = codex
        self.zulip = zulip
        self.skills = skills or SkillStore(
            config.workspace_dir / "skills",
            max_bytes=config.schedule_skill_max_bytes,
            max_count=config.schedule_skill_max_count,
        )
        self.schedules = schedules or ScheduleStore(
            config.workspace_dir,
            timezone_name=config.schedule_timezone,
        )
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

        move = normalize_zulip_update_message_event(event, self.config.realm_id)
        if move is not None:
            result = self.storage.apply_message_move(move)
            self.schedules.apply_message_move(move)
            accepted = result.get("status") == "applied"
            return EnqueueResult(
                accepted,
                str(result.get("reason") or "processed update_message"),
                str(result["session_key"]) if result.get("session_key") else None,
                move.message_id,
            )

        message = normalize_zulip_event(
            event,
            self.config.realm_id,
            bot_user_id=self.config.bot_user_id,
            bot_email=self.config.bot_email,
            bot_aliases=self.config.bot_aliases,
        )
        if message is None:
            reason = (
                "ignored unsupported reaction"
                if event.get("type") == "reaction"
                else "ignored non-move update_message"
                if event.get("type") == "update_message"
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

    async def run_scheduler(self) -> None:
        while True:
            try:
                await self.run_schedules_once()
            except Exception:
                LOGGER.exception("Scheduled task tick failed")
            await asyncio.sleep(self.config.schedule_tick_seconds)

    async def run_schedules_once(self) -> int:
        due_jobs = self.schedules.get_due_jobs()
        completed = 0
        for job in due_jobs:
            key = self.schedules.session_key_for_job(job)
            async with self._active_guard:
                if key.value in self._active_sessions:
                    continue
                self._active_sessions.add(key.value)
            try:
                self.schedules.advance_next_run(str(job.get("id") or ""))
                await self._run_scheduled_job(job)
                completed += 1
            finally:
                async with self._active_guard:
                    self._active_sessions.discard(key.value)
        return completed

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
            await self._run_messages(key, [message])
            while True:
                pending = self.storage.pop_pending_messages(key)
                pending = self._filter_unprocessed(key, pending)
                if not pending:
                    break
                await self._run_messages(key, pending)
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

    async def _run_messages(self, key: SessionKey, messages: list[NormalizedMessage]) -> None:
        batch: list[NormalizedMessage] = []
        for message in messages:
            command = parse_control_command(message, self.config.bot_aliases)
            if command is None:
                batch.append(message)
                continue
            if batch:
                await self._run_turn(key, batch)
                batch = []
            await self._run_control_command(key, message, command)
        if batch:
            await self._run_turn(key, batch)

    async def _run_control_command(
        self,
        key: SessionKey,
        message: NormalizedMessage,
        command: ControlCommand,
    ) -> None:
        if command.name == "clear":
            await self._run_clear_command(key, message)
            return
        await self._run_status_command(key, message)

    async def _run_clear_command(self, key: SessionKey, message: NormalizedMessage) -> None:
        self.storage.clear_session_context(key, message)
        content = "Cleared. The next normal message starts a fresh Codex thread."
        post = await self._post_control_response(message, content)
        self.storage.log_control_turn(
            key,
            message,
            command="clear",
            post=post,
            summary={"decision": "cleared", "next_thread": "fresh on next normal message"},
        )
        self.storage.mark_processed(key, [message.message_id])

    async def _run_status_command(self, key: SessionKey, message: NormalizedMessage) -> None:
        content, summary = self._status_response(key, message)
        post = await self._post_control_response(message, content)
        self.storage.log_control_turn(
            key,
            message,
            command="status",
            post=post,
            summary=summary,
        )
        self.storage.mark_processed(key, [message.message_id])

    async def _post_control_response(self, message: NormalizedMessage, content: str) -> dict[str, Any]:
        if self.config.post_replies:
            return self._post_summary(await self.zulip.post_reply(message, content), dry_run=False)
        return {"status": "dry_run", "dry_run": True, "message_to_post": content}

    def _status_response(
        self,
        key: SessionKey,
        status_message: NormalizedMessage,
    ) -> tuple[str, dict[str, Any]]:
        metadata = self.storage.load_metadata(key)
        turns = self.storage.read_turns(key)
        messages_by_id = self._message_records_by_id(key)
        errors = self._status_errors(key, metadata)
        latest_turn = self._latest_model_turn(turns, metadata)
        latest_error = errors[-1] if errors else None

        lines = ["**Sili status**", ""]
        summary: dict[str, Any] = {}
        if latest_turn is not None:
            decision = latest_turn.get("decision") if isinstance(latest_turn.get("decision"), dict) else {}
            reply_kind = str(decision.get("reply_kind") or "silent")
            confidence = self._format_confidence(decision.get("confidence"))
            lines.append(f"- Decision: {reply_kind}{confidence}")
            summary["decision"] = reply_kind
            summary["confidence"] = decision.get("confidence")

            posted = self._posted_text(latest_turn)
            if posted:
                lines.append(f"- Posted: {self._quote_excerpt(posted)}")
                summary["posted"] = True
            else:
                why = "chose not to reply" if reply_kind == "silent" else "no visible reply"
                why += "; no runtime error" if latest_error is None else f"; latest error on {self._error_surface(latest_error)}"
                lines.append(f"- Why: {why}")
        elif latest_error is not None:
            lines.append("- Decision: failed before reply")
            lines.append("- Why: reply failed before a decision was logged")
            summary["decision"] = "failed before reply"
        elif metadata.cleared_at_message_id is not None:
            lines.append("- Decision: cleared")
            lines.append("- Why: next normal message starts fresh")
            summary["decision"] = "cleared"
        else:
            lines.append("- Decision: none")
            lines.append("- Why: no normal turn yet")
            summary["decision"] = "none"

        message_line = self._status_message_line(latest_turn, latest_error, metadata, status_message, messages_by_id)
        lines.append(f"- Message: {message_line}")
        error_line = self._status_error_line(errors)
        lines.append(f"- Errors: {error_line}")
        summary["errors"] = error_line
        return "\n".join(lines), summary

    def _message_records_by_id(self, key: SessionKey) -> dict[int, dict[str, Any]]:
        records: dict[int, dict[str, Any]] = {}
        for record in self.storage.read_messages(key):
            message_id = self._record_message_id(record)
            if message_id is not None:
                records[message_id] = record
        return records

    def _latest_model_turn(
        self,
        turns: list[dict[str, Any]],
        metadata: SessionMetadata,
    ) -> dict[str, Any] | None:
        for turn in reversed(turns):
            if turn.get("kind") == "control":
                continue
            if not isinstance(turn.get("decision"), dict):
                continue
            if self._record_after_clear(turn, metadata):
                return turn
        return None

    def _status_errors(self, key: SessionKey, metadata: SessionMetadata) -> list[dict[str, Any]]:
        return [
            error
            for error in self.storage.read_errors_for_session(key)
            if error.get("kind") != "ignored_event" and self._record_after_clear(error, metadata)
        ]

    def _record_after_clear(self, record: dict[str, Any], metadata: SessionMetadata) -> bool:
        clear_message_id = metadata.cleared_at_message_id
        if clear_message_id is None:
            return True
        record_ids = self._record_message_ids(record)
        if record_ids:
            return max(record_ids) >= clear_message_id
        if metadata.cleared_at and record.get("created_at"):
            return str(record.get("created_at")) >= metadata.cleared_at
        return False

    def _status_message_line(
        self,
        latest_turn: dict[str, Any] | None,
        latest_error: dict[str, Any] | None,
        metadata: SessionMetadata,
        status_message: NormalizedMessage,
        messages_by_id: dict[int, dict[str, Any]],
    ) -> str:
        source: dict[str, Any] | NormalizedMessage | None = None
        if latest_turn is not None:
            source = self._message_record_for_ids(self._record_message_ids(latest_turn), messages_by_id)
        elif latest_error is not None:
            source = self._message_record_for_ids(self._record_message_ids(latest_error), messages_by_id)
        elif metadata.cleared_at_message_id is not None:
            source = messages_by_id.get(metadata.cleared_at_message_id)
        if source is None and metadata.cleared_at_message_id is None:
            source = status_message
        if source is None:
            return "none"
        return self._format_status_message(source)

    def _message_record_for_ids(
        self,
        message_ids: list[int],
        messages_by_id: dict[int, dict[str, Any]],
    ) -> dict[str, Any] | None:
        for message_id in message_ids:
            record = messages_by_id.get(message_id)
            if record is not None:
                return record
        return None

    def _format_status_message(self, source: dict[str, Any] | NormalizedMessage) -> str:
        if isinstance(source, NormalizedMessage):
            sender = source.sender_full_name or source.sender_email or "unknown"
            content = source.content
        else:
            sender = str(source.get("sender_full_name") or source.get("sender_email") or "unknown")
            content = str(source.get("content") or "")
        return f"{sender}: {self._quote_excerpt(content)}"

    def _status_error_line(self, errors: list[dict[str, Any]]) -> str:
        if not errors:
            return "none"
        recent = errors[-2:]
        parts = [f"{self._error_surface(error)}: {self._error_text(error)}" for error in recent]
        remaining = len(errors) - len(recent)
        if remaining > 0:
            parts.append(f"+{remaining} more")
        return "; ".join(parts)

    def _error_surface(self, error: dict[str, Any]) -> str:
        worker = str(error.get("worker") or "").strip().casefold()
        if worker in {"memory", "skill", "schedule"}:
            return worker
        kind = str(error.get("kind") or "").strip().casefold()
        event = str(error.get("event") or "").strip().casefold()
        if "scheduled" in kind or "scheduled" in event:
            return "scheduled_job"
        if kind in {"turn_exception", "codex_thread_restarted"}:
            return "reply"
        if kind == "worker_exception":
            return "runtime"
        return "runtime"

    def _error_text(self, error: dict[str, Any]) -> str:
        value = str(error.get("error") or error.get("reason") or error.get("kind") or error.get("event") or "error")
        return self._compact_text(value, limit=120)

    def _posted_text(self, turn: dict[str, Any]) -> str:
        post = turn.get("post") if isinstance(turn.get("post"), dict) else {}
        message_to_post = str(post.get("message_to_post") or "").strip()
        if message_to_post:
            return message_to_post
        decision = turn.get("decision") if isinstance(turn.get("decision"), dict) else {}
        return str(decision.get("message_to_post") or "").strip()

    def _quote_excerpt(self, value: str) -> str:
        text = self._compact_text(value, limit=140)
        return f'"{text}"'

    def _compact_text(self, value: str, *, limit: int) -> str:
        text = re.sub(r"\s+", " ", value).strip()
        if len(text) <= limit:
            return text
        return text[: max(0, limit - 1)].rstrip() + "..."

    def _format_confidence(self, value: Any) -> str:
        try:
            confidence = float(value)
        except (TypeError, ValueError):
            return ""
        return f" ({confidence:.2f})"

    def _record_message_ids(self, record: dict[str, Any]) -> list[int]:
        raw_ids = record.get("message_ids")
        values = raw_ids if isinstance(raw_ids, list) else [record.get("message_id")]
        ids: list[int] = []
        for value in values:
            try:
                ids.append(int(value))
            except (TypeError, ValueError):
                continue
        return ids

    def _record_message_id(self, record: dict[str, Any]) -> int | None:
        try:
            return int(record["message_id"])
        except (KeyError, TypeError, ValueError):
            return None

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
            instruction_kwargs = {
                "stream": first.stream,
                "topic_hash": first.topic_hash,
                "topic": first.topic,
                "stream_id": first.stream_id,
                "conversation_type": first.conversation_type,
                "private_recipient_key": first.private_recipient_key,
            }
            reply_developer_instructions = None
            if starting_new_thread:
                reply_developer_instructions = self.instructions.compose(role="reply", **instruction_kwargs)

            pending_posted_bot_updates = self.storage.read_pending_posted_bot_updates(key)
            posted_bot_update_context = self._posted_bot_update_context(pending_posted_bot_updates)
            memory_context, memory_hash, memory_hash_changed = self._memory_context_for_prompt(key, metadata)
            shared_context = self._join_acknowledgements([memory_context, posted_bot_update_context])
            memory_prompt = self.prompt_builder.build(
                PromptParts(
                    current_messages=messages,
                    injected_context=shared_context,
                ),
                template_file=MEMORY_WORKER_USER_PROMPT_FILE,
            )
            skill_prompt = self.prompt_builder.build(
                PromptParts(
                    current_messages=messages,
                    injected_context=shared_context,
                ),
                template_file=SKILL_WORKER_USER_PROMPT_FILE,
            )

            worker_specs = [
                CodexWorkerSpec(
                    kind="memory",
                    prompt=memory_prompt,
                    developer_instructions=self.instructions.compose(role="memory_worker", **instruction_kwargs),
                    output_schema_path=self.config.workspace_dir / MEMORY_DECISION_SCHEMA_FILE,
                ),
                CodexWorkerSpec(
                    kind="skill",
                    prompt=skill_prompt,
                    developer_instructions=self.instructions.compose(role="skill_worker", **instruction_kwargs),
                    output_schema_path=self.config.workspace_dir / SKILL_DECISION_SCHEMA_FILE,
                ),
            ]
            try:
                parent_result = await self.codex.ensure_thread(
                    active_thread_id,
                    developer_instructions=reply_developer_instructions,
                )
            except Exception as exc:
                if active_thread_id is None or not self._is_missing_codex_rollout_error(exc):
                    raise
                self.storage.log_error(
                    key,
                    {
                        "kind": "codex_thread_restarted",
                        "thread_id": active_thread_id,
                        "error": repr(exc),
                        "message_ids": [message.message_id for message in messages],
                    },
                )
                self.storage.set_codex_thread_state(key, thread_id=None, instruction_mode=None)
                parent_result = await self.codex.ensure_thread(
                    None,
                    developer_instructions=self.instructions.compose(role="reply", **instruction_kwargs),
                )
            parent_thread_id = parent_result.thread_id
            if parent_thread_id:
                self.storage.set_codex_thread_state(
                    key,
                    thread_id=parent_thread_id,
                    instruction_mode=CODEX_INSTRUCTION_MODE,
                )

            worker_results: dict[str, CodexRunResult | None] = {
                spec.kind: await self._run_op_worker(key, messages, parent_thread_id, spec)
                for spec in worker_specs
            }

            memory_decision, memory_applied = self._apply_memory_worker_result(
                key,
                messages,
                worker_results.get("memory"),
            )
            skill_decision, skill_applied = self._apply_skill_worker_result(
                key,
                messages,
                worker_results.get("skill"),
            )
            schedule_context = self._join_acknowledgements(
                [
                    self._schedule_context_for_prompt(),
                    self._current_schedules_context(first),
                    self._mentionable_participants_context(key),
                    self._skill_availability_context(skill_applied),
                    shared_context,
                ]
            )
            schedule_prompt = self.prompt_builder.build(
                PromptParts(
                    current_messages=messages,
                    injected_context=schedule_context,
                ),
                template_file=SCHEDULE_WORKER_USER_PROMPT_FILE,
            )
            schedule_result = await self._run_op_worker(
                key,
                messages,
                parent_thread_id,
                CodexWorkerSpec(
                    kind="schedule",
                    prompt=schedule_prompt,
                    developer_instructions=self.instructions.compose(role="schedule_worker", **instruction_kwargs),
                    output_schema_path=self.config.workspace_dir / SCHEDULE_DECISION_SCHEMA_FILE,
                ),
            )
            schedule_decision, schedule_applied = self._apply_schedule_worker_result(
                key,
                first,
                messages,
                schedule_result,
            )
            memory_acknowledgement = self._memory_acknowledgement(memory_applied)
            skill_acknowledgement = self._skill_acknowledgement(skill_applied)
            schedule_acknowledgement = self._schedule_acknowledgement(schedule_applied)
            acknowledgement = self._join_acknowledgements(
                [skill_acknowledgement, schedule_acknowledgement, memory_acknowledgement]
            )
            reply_context = self._join_acknowledgements(
                [
                    shared_context,
                    self._applied_changes_context(acknowledgement),
                ]
            )
            reply_prompt = self.prompt_builder.build(
                PromptParts(
                    current_messages=messages,
                    injected_context=reply_context,
                ),
                template_file=REPLY_TURN_USER_PROMPT_FILE,
            )
            try:
                reply_result = await self.codex.run_decision(
                    reply_prompt,
                    parent_thread_id,
                    developer_instructions=None,
                    output_schema_path=self.config.workspace_dir / REPLY_DECISION_SCHEMA_FILE,
                )
            except Exception as exc:
                if parent_thread_id is None or not self._is_missing_codex_rollout_error(exc):
                    raise
                self.storage.log_error(
                    key,
                    {
                        "kind": "codex_thread_restarted",
                        "thread_id": parent_thread_id,
                        "error": repr(exc),
                        "message_ids": [message.message_id for message in messages],
                    },
                )
                self.storage.set_codex_thread_state(key, thread_id=None, instruction_mode=None)
                reply_result = await self.codex.run_decision(
                    reply_prompt,
                    None,
                    developer_instructions=self.instructions.compose(role="reply", **instruction_kwargs),
                    output_schema_path=self.config.workspace_dir / REPLY_DECISION_SCHEMA_FILE,
                )
            if reply_result.thread_id:
                self.storage.set_codex_thread_state(
                    key,
                    thread_id=reply_result.thread_id,
                    instruction_mode=CODEX_INSTRUCTION_MODE,
                )

            reply_decision = ReplyDecision.from_json_text(reply_result.raw_text)
            if memory_hash_changed:
                self.storage.set_last_injected_memory_hash(key, memory_hash)

            decision = AgentDecision.from_parts(
                reply_decision,
                memory_ops=memory_decision.memory_ops,
                skill_ops=skill_decision.skill_ops,
                schedule_ops=schedule_decision.schedule_ops,
            )
            message_to_post = self._message_to_post(
                first,
                decision,
                acknowledgement=acknowledgement,
            )
            if self._reply_conflicts_with_schedule_acknowledgement(message_to_post, schedule_acknowledgement):
                message_to_post = ""
            outbound_message = self._with_acknowledgement(message_to_post, acknowledgement)
            if typing_started and first.conversation_type == "stream" and not outbound_message:
                await stack.aclose()
                typing_started = False

            post: dict[str, Any] | None = None
            if outbound_message:
                if self.config.post_replies:
                    post = self._post_summary(await self.zulip.post_reply(first, outbound_message), dry_run=False)
                else:
                    post = {"status": "dry_run", "dry_run": True, "message_to_post": outbound_message}
                self._enqueue_posted_bot_update(
                    key,
                    source="conversation_turn",
                    content=outbound_message,
                    post=post,
                    acknowledgement=acknowledgement,
                    message_ids=[message.message_id for message in messages],
                )

            self.storage.log_turn(
                key=key,
                messages=messages,
                decision=decision,
                post=post,
                memory_applied=memory_applied,
                skill_applied=skill_applied,
                schedule_applied=schedule_applied,
                memory_acknowledgement=memory_acknowledgement,
                skill_acknowledgement=skill_acknowledgement,
                schedule_acknowledgement=schedule_acknowledgement,
            )
            self.storage.consume_posted_bot_updates(key, pending_posted_bot_updates)
        self.storage.mark_processed(key, [message.message_id for message in messages])

    async def _run_scheduled_job(self, job: dict[str, Any]) -> None:
        job_id = str(job.get("id") or "")
        origin_message = self.schedules.message_for_job(job)
        key = origin_message.session_key
        developer_instructions = self.instructions.compose(
            role="scheduled_job",
            stream=origin_message.stream,
            topic_hash=origin_message.topic_hash,
            topic=origin_message.topic,
            stream_id=origin_message.stream_id,
            conversation_type=origin_message.conversation_type,
            private_recipient_key=origin_message.private_recipient_key,
        )

        post: dict[str, Any] | None = None
        memory_applied: list[dict[str, Any]] = []
        memory_acknowledgement = ""
        schedule_ignored: list[dict[str, Any]] = []
        skill_ignored: list[dict[str, Any]] = []
        try:
            prompt = self._scheduled_prompt(job, origin_message)
            codex_result = await asyncio.wait_for(
                self.codex.run_decision(
                    prompt,
                    None,
                    developer_instructions=developer_instructions,
                    output_schema_path=self.config.workspace_dir / SCHEDULED_JOB_DECISION_SCHEMA_FILE,
                ),
                timeout=self.config.schedule_run_timeout_seconds,
            )

            decision = AgentDecision.from_json_text(codex_result.raw_text)
            if decision.schedule_ops:
                schedule_ignored = [op.to_record() for op in decision.schedule_ops]
            if decision.skill_ops:
                skill_ignored = [op.to_record() for op in decision.skill_ops]

            memory_applied = self.memory.apply_ops(key, decision.memory_ops)
            memory_acknowledgement = self._memory_acknowledgement(memory_applied)
            outbound_message = decision.message_to_post.strip() if decision.should_reply else ""
            if outbound_message and outbound_message.strip().upper() != "[SILENT]":
                outbound_message = self._with_scheduled_mentions(job, outbound_message)
            outbound_message = self._with_acknowledgement(outbound_message, memory_acknowledgement)
            should_deliver = bool(outbound_message) and outbound_message.strip().upper() != "[SILENT]"

            if should_deliver:
                if self.config.post_replies:
                    post = self._post_summary(await self.zulip.post_reply(origin_message, outbound_message), dry_run=False)
                else:
                    post = {"status": "dry_run", "dry_run": True, "message_to_post": outbound_message}
                self._enqueue_posted_bot_update(
                    key,
                    source="scheduled_job",
                    content=outbound_message,
                    post=post,
                    acknowledgement=memory_acknowledgement,
                    job_id=job_id,
                )

            self.schedules.log_run(
                job_id,
                {
                    "status": "ok" if outbound_message or not should_deliver else "empty",
                    "decision": decision.to_record(),
                    "post": post,
                    "memory_applied": memory_applied,
                    "memory_acknowledgement": memory_acknowledgement,
                    "ignored_schedule_ops": schedule_ignored,
                    "ignored_skill_ops": skill_ignored,
                },
            )
            self.schedules.mark_job_run(
                job_id,
                success=bool(outbound_message) or not should_deliver,
                error=None if outbound_message or not should_deliver else "scheduled task produced no output",
            )
        except Exception as exc:
            LOGGER.exception("Scheduled job %s failed", job_id)
            error = repr(exc)
            self.schedules.log_run(
                job_id,
                {
                    "status": "error",
                    "error": error,
                    "post": post,
                    "memory_applied": memory_applied,
                    "memory_acknowledgement": memory_acknowledgement,
                },
            )
            self.schedules.mark_job_run(job_id, success=False, error=error)

    def _scheduled_prompt(self, job: dict[str, Any], origin_message: NormalizedMessage) -> str:
        timezone_name = self.config.schedule_timezone
        local_now = utc_now().astimezone(zoneinfo_for(timezone_name))
        skill_context, skill_errors = self.skills.render_for_prompt(job.get("skills") or [])
        memory_context = self.memory.render_selected(origin_message.session_key).strip()
        return self.prompt_builder.render_template(
            SCHEDULED_JOB_USER_PROMPT_FILE,
            {
                "job_id": job.get("id") or "",
                "job_name": job.get("name") or "",
                "current_time_utc": utc_now().isoformat(),
                "schedule_timezone": timezone_name,
                "current_time_local": local_now.isoformat(),
                "delivery": f"original Zulip {origin_message.conversation_type}",
                "task": str(job.get("prompt") or "").strip(),
                "mention_targets": self._scheduled_mention_context(job),
                "skill_context": skill_context.strip(),
                "skill_errors": "\n".join(f"- {error}" for error in skill_errors),
                "memory_context": memory_context,
            },
        )

    def _memory_context_for_prompt(
        self,
        key: SessionKey,
        metadata: SessionMetadata,
    ) -> tuple[str, str | None, bool]:
        rendered = self.memory.render_selected(key).strip()
        current_hash = self._memory_hash(rendered)
        previous_hash = metadata.last_injected_memory_hash or None
        if rendered and current_hash != previous_hash:
            return (
                "\n".join(["# Scoped Memory", "", rendered]),
                current_hash,
                True,
            )
        if not rendered and previous_hash:
            return (
                "\n".join(["# Scoped Memory", "", "- Empty"]),
                None,
                True,
            )
        return "", current_hash, False

    def _schedule_context_for_prompt(self) -> str:
        timezone_name = self.config.schedule_timezone
        try:
            local_now = utc_now().astimezone(zoneinfo_for(timezone_name))
        except ValueError:
            timezone_name = "UTC"
            local_now = utc_now()
        return "\n".join(
            [
                "# Scheduling Context",
                "",
                f"- Current time (UTC): {utc_now().isoformat()}",
                f"- Scheduling timezone: {timezone_name}",
                f"- Current time ({timezone_name}): {local_now.isoformat()}",
            ]
        )

    def _current_schedules_context(self, origin: NormalizedMessage) -> str:
        result = self.schedules.list_context_jobs(origin)
        raw_jobs = result.get("jobs") if isinstance(result, dict) else []
        jobs = [job for job in raw_jobs if isinstance(job, dict)] if isinstance(raw_jobs, list) else []
        sections = ["# Current Scheduled Tasks Here"]
        if not jobs:
            sections.extend(["", "- None"])
            return "\n".join(sections)

        sections.append("")
        for job in jobs:
            job_id = str(job.get("id") or "").strip()
            name = str(job.get("name") or "").strip() or "unnamed schedule"
            state = str(job.get("state") or ("active" if job.get("enabled", True) else "inactive"))
            trigger = self._schedule_trigger_label(job)
            next_run = self._format_schedule_time(job.get("next_run_at"))
            skills = self._schedule_inventory_skills(job)
            mentions = self._schedule_inventory_mentions(job)
            sections.append(
                f"- id={job_id}; name={name}; state={state}; trigger={trigger}; "
                f"next={next_run}; skills=[{skills}]; mentions=[{mentions}]"
            )
            prompt = str(job.get("prompt") or "").strip()
            if prompt:
                sections.append(f"  prompt: {prompt}")
        return "\n".join(sections).rstrip()

    def _schedule_inventory_skills(self, job: dict[str, Any]) -> str:
        raw_skills = job.get("skills")
        if not isinstance(raw_skills, list):
            return "none"
        skills = [str(skill).strip() for skill in raw_skills if str(skill).strip()]
        return ", ".join(skills) if skills else "none"

    def _schedule_inventory_mentions(self, job: dict[str, Any]) -> str:
        mentions: list[str] = []
        for target in self._job_mention_targets(job):
            kind = str(target.get("kind") or "").strip().lower()
            if kind == "person":
                full_name = str(target.get("full_name") or "").strip()
                user_id = target.get("user_id")
                if full_name and user_id is not None:
                    mentions.append(f"person:{full_name}#{user_id}")
                elif full_name:
                    mentions.append(f"person:{full_name}")
            elif kind in {"topic", "channel", "all"}:
                mentions.append(kind)
        return ", ".join(mentions) if mentions else "none"

    def _mentionable_participants_context(self, key: SessionKey) -> str:
        participants = self._mentionable_users(key)
        sections = [
            "# Mentionable Zulip Participants",
        ]
        if not participants:
            sections.extend(["", "- None"])
            return "\n".join(sections)
        sections.append("")
        for user_id, full_name in sorted(participants.items(), key=lambda item: item[1].casefold()):
            sections.append(f"- full_name={full_name}; user_id={user_id}")
        return "\n".join(sections)

    def _mentionable_users(self, key: SessionKey) -> dict[int, str]:
        users: dict[int, str] = {}
        for participant in self.storage.read_conversation_participants(key):
            user_id = participant.get("user_id")
            if not isinstance(user_id, int):
                continue
            email = str(participant.get("sender_email") or "")
            if self.config.bot_user_id is not None and user_id == self.config.bot_user_id:
                continue
            if self.config.bot_email and email.casefold() == self.config.bot_email.casefold():
                continue
            full_name = str(participant.get("full_name") or "").strip()
            if full_name:
                users[user_id] = full_name
        return users

    def _scheduled_mention_context(self, job: dict[str, Any]) -> str:
        targets = self._job_mention_targets(job)
        if not targets:
            return "- None"
        lines: list[str] = []
        for target in targets:
            mention = self._mention_text(target)
            if not mention:
                continue
            kind = str(target.get("kind") or "").strip()
            user_id = target.get("user_id")
            full_name = str(target.get("full_name") or "").strip()
            parts = [f"kind={kind}", f"mention={mention}"]
            if user_id is not None:
                parts.append(f"user_id={user_id}")
            if full_name:
                parts.append(f"full_name={full_name}")
            lines.append("- " + "; ".join(parts))
        return "\n".join(lines) if lines else "- None"

    def _with_scheduled_mentions(self, job: dict[str, Any], message_to_post: str) -> str:
        mentions = [
            mention
            for target, mention in self._scheduled_mentions(job)
            if not self._normal_mention_already_present(target, message_to_post)
        ]
        if not mentions:
            return message_to_post
        return f"{' '.join(mentions)} {message_to_post}".strip()

    def _scheduled_mentions(self, job: dict[str, Any]) -> list[tuple[dict[str, Any], str]]:
        mentions: list[tuple[dict[str, Any], str]] = []
        for target in self._job_mention_targets(job):
            mention = self._mention_text(target)
            if mention and all(existing != mention for _, existing in mentions):
                mentions.append((target, mention))
        return mentions

    def _normal_mention_already_present(self, target: dict[str, Any], message_to_post: str) -> bool:
        kind = str(target.get("kind") or "").strip().lower()
        if kind == "person":
            full_name = str(target.get("full_name") or "").strip()
            if not full_name:
                return False
            pattern = rf"@\*\*{re.escape(full_name)}(?:\|\d+)?\*\*"
            return re.search(pattern, message_to_post) is not None
        mention = self._mention_text(target)
        return bool(mention and mention in message_to_post)

    def _job_mention_targets(self, job: dict[str, Any]) -> list[dict[str, Any]]:
        raw_targets = job.get("mention_targets")
        if not isinstance(raw_targets, list):
            return []
        return [target for target in raw_targets if isinstance(target, dict)]

    def _mention_text(self, target: dict[str, Any]) -> str:
        kind = str(target.get("kind") or "").strip().lower()
        if kind == "person":
            full_name = str(target.get("full_name") or "").strip()
            if not full_name:
                return ""
            return f"@**{full_name}**"
        if kind in {"topic", "channel", "all"}:
            return f"@**{kind}**"
        return ""

    def _confirmation_mentions(self, job: dict[str, Any]) -> list[str]:
        mentions: list[str] = []
        for target in self._job_mention_targets(job):
            mention = self._confirmation_mention(target)
            if mention and mention not in mentions:
                mentions.append(mention)
        return mentions

    def _confirmation_mention(self, target: dict[str, Any]) -> str:
        kind = str(target.get("kind") or "").strip().lower()
        if kind == "person":
            full_name = str(target.get("full_name") or "").strip()
            if not full_name:
                return ""
            return f"@_**{full_name}**"
        if kind == "topic":
            return "topic participants"
        if kind == "channel":
            return "channel"
        if kind == "all":
            return "all channel members"
        return ""

    def _skill_availability_context(self, skill_applied: list[dict[str, Any]]) -> str:
        sections = [
            "# Skill Availability",
        ]
        summaries = self.skills.list_summaries()
        if summaries:
            sections.extend(["", "## Available Skills"])
            for summary in summaries:
                name = summary.get("name", "").strip()
                description = summary.get("description", "").strip()
                if not name:
                    continue
                suffix = f": {description}" if description else ""
                sections.append(f"- `{name}`{suffix}")
        else:
            sections.extend(["", "## Available Skills", "- None"])

        sections.extend(["", "## Skill Changes This Turn"])
        if not skill_applied:
            sections.append("- None")
        for result in skill_applied:
            status = str(result.get("status") or "unknown")
            action = str(result.get("action") or "unknown")
            name = str(result.get("name") or "").strip()
            reason = str(result.get("reason") or "").strip()
            target = f" `{name}`" if name else ""
            detail = f": {reason}" if reason else ""
            sections.append(f"- {status} {action}{target}{detail}")
        return "\n".join(sections).rstrip()

    async def _run_op_worker(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        parent_thread_id: str | None,
        worker_spec: CodexWorkerSpec,
    ) -> CodexRunResult | None:
        if not parent_thread_id:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_failed",
                    "worker": worker_spec.kind,
                    "error": "missing parent thread id",
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return None
        try:
            return await self.codex.run_worker_fork(parent_thread_id, worker_spec)
        except Exception as exc:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_failed",
                    "worker": worker_spec.kind,
                    "error": str(exc),
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return None

    def _applied_changes_context(self, acknowledgement: str) -> str:
        if not acknowledgement.strip():
            return ""
        return "\n".join(
            [
                "# Applied Changes This Turn",
                "",
                "These changes have already been validated and persisted by TokenZulip before this reply decision.",
                "",
                "```text",
                acknowledgement.strip(),
                "```",
            ]
        )

    def _reply_conflicts_with_schedule_acknowledgement(
        self,
        message_to_post: str,
        schedule_acknowledgement: str,
    ) -> bool:
        if not message_to_post.strip() or not schedule_acknowledgement.strip():
            return False
        acknowledgement = schedule_acknowledgement.casefold()
        if "schedule removed" not in acknowledgement and "scheduled tasks here" not in acknowledgement:
            return False
        text = (
            message_to_post.casefold()
            .replace("\u2018", "'")
            .replace("\u2019", "'")
        )
        blocked_phrases = [
            "can't remove",
            "cannot remove",
            "can't list",
            "cannot list",
            "doesn't have a live reminder",
            "does not have a live reminder",
            "no deletion has been performed",
            "no scheduler",
            "no reminder-listing tool",
            "no live reminder-listing tool",
            "reply-only thread",
        ]
        return any(phrase in text for phrase in blocked_phrases)

    def _posted_bot_update_context(self, updates: list[dict[str, Any]]) -> str:
        if not updates:
            return ""
        sections = ["# Posted Bot Updates"]
        for update in updates:
            source = str(update.get("source") or "unknown")
            created_at = str(update.get("created_at") or "unknown time")
            sections.extend(["", f"## {source} at {created_at}"])
            job_id = str(update.get("job_id") or "").strip()
            if job_id:
                sections.append(f"- Scheduled job: {job_id}")
            message_ids = update.get("message_ids")
            if isinstance(message_ids, list) and message_ids:
                sections.append("- Source message IDs: " + ", ".join(str(value) for value in message_ids))
            content = str(update.get("content") or "").strip()
            if content:
                sections.extend(["", "```text", content, "```"])
        return "\n".join(sections).rstrip()

    def _enqueue_posted_bot_update(
        self,
        key: SessionKey,
        *,
        source: str,
        content: str,
        post: dict[str, Any] | None,
        acknowledgement: str = "",
        message_ids: list[int] | None = None,
        job_id: str | None = None,
    ) -> dict[str, Any] | None:
        if not self._post_was_visible(post):
            return None
        return self.storage.append_posted_bot_update(
            key,
            source=source,
            content=content,
            post=post or {},
            acknowledgement=acknowledgement,
            message_ids=message_ids,
            job_id=job_id,
        )

    @staticmethod
    def _is_missing_codex_rollout_error(exc: Exception) -> bool:
        return "no rollout found for thread id" in str(exc).casefold()

    def _post_was_visible(self, post: dict[str, Any] | None) -> bool:
        if not post:
            return False
        if post.get("dry_run") is True:
            return True
        return str(post.get("status") or "").lower() == "success"

    def _memory_hash(self, rendered: str) -> str | None:
        if not rendered:
            return None
        return hashlib.sha256(rendered.encode("utf-8")).hexdigest()

    def _message_to_post(
        self,
        first: NormalizedMessage,
        decision: AgentDecision,
        *,
        acknowledgement: str = "",
    ) -> str:
        content = decision.message_to_post.strip()
        if decision.should_reply and content:
            return content
        if first.reply_required and acknowledgement and not content:
            return ""
        if first.reply_required:
            return content or PRIVATE_REPLY_FALLBACK
        return ""

    def _apply_memory_worker_result(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        result: CodexRunResult | None,
    ) -> tuple[MemoryDecision, list[dict[str, Any]]]:
        if result is None:
            return MemoryDecision(), []
        try:
            decision = MemoryDecision.from_json_text(result.raw_text)
            applied = self.memory.apply_ops(
                key,
                decision.memory_ops,
                [message.message_id for message in messages],
            )
            return decision, applied
        except Exception as exc:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_apply_failed",
                    "worker": "memory",
                    "error": str(exc),
                    "thread_id": result.thread_id,
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return MemoryDecision(), []

    def _apply_skill_worker_result(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        result: CodexRunResult | None,
    ) -> tuple[SkillDecision, list[dict[str, Any]]]:
        if result is None:
            return SkillDecision(), []
        try:
            decision = SkillDecision.from_json_text(result.raw_text)
            return decision, self.skills.apply_ops(decision.skill_ops)
        except Exception as exc:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_apply_failed",
                    "worker": "skill",
                    "error": str(exc),
                    "thread_id": result.thread_id,
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return SkillDecision(), []

    def _apply_schedule_worker_result(
        self,
        key: SessionKey,
        first: NormalizedMessage,
        messages: list[NormalizedMessage],
        result: CodexRunResult | None,
    ) -> tuple[ScheduleDecision, list[dict[str, Any]]]:
        if result is None:
            return ScheduleDecision(), []
        try:
            decision = ScheduleDecision.from_json_text(result.raw_text)
            applied = self.schedules.apply_ops(
                first,
                decision.schedule_ops,
                skills=self.skills,
                mentionable_users=self._mentionable_users(key),
            )
            return decision, applied
        except Exception as exc:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_apply_failed",
                    "worker": "schedule",
                    "error": str(exc),
                    "thread_id": result.thread_id,
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return ScheduleDecision(), []

    def _with_acknowledgement(self, message_to_post: str, acknowledgement: str) -> str:
        if not acknowledgement:
            return message_to_post
        if not message_to_post:
            return acknowledgement
        return f"{message_to_post}\n\n{acknowledgement}"

    def _join_acknowledgements(self, acknowledgements: list[str]) -> str:
        return "\n".join(ack.strip() for ack in acknowledgements if ack.strip())

    def _skill_acknowledgement(self, results: list[dict[str, Any]]) -> str:
        changes: list[str] = []
        for result in results:
            action = str(result.get("action") or "")
            name = str(result.get("name") or "").strip()
            status = str(result.get("status") or "")
            reason = str(result.get("reason") or "").strip()
            if status == "applied" and name:
                verb = "saved" if action in {"create", "update"} else "removed"
                changes.append(f"Skill {verb}: {name}")
            elif status == "rejected" and (name or reason):
                target = f" {name}" if name else ""
                changes.append(f"Skill{target} not changed: {reason or 'rejected'}")
        return "\n".join(changes)

    def _schedule_acknowledgement(self, results: list[dict[str, Any]]) -> str:
        changes: list[str] = []
        for result in results:
            action = str(result.get("action") or "")
            status = str(result.get("status") or "")
            name = str(result.get("name") or "").strip()
            job = result.get("job") if isinstance(result.get("job"), dict) else {}
            display_name = name or str(job.get("name") or "").strip()
            reason = str(result.get("reason") or "").strip()
            if action == "list" and status == "applied":
                jobs = result.get("jobs") if isinstance(result.get("jobs"), list) else []
                if not jobs:
                    changes.append("**Scheduled tasks here**\n- None")
                else:
                    lines = ["**Scheduled tasks here**"]
                    for item in jobs:
                        if not isinstance(item, dict):
                            continue
                        state = str(item.get("state") or ("active" if item.get("enabled", True) else "inactive"))
                        item_name = str(item.get("name") or item.get("id") or "unnamed schedule")
                        item_id = str(item.get("id") or "")
                        id_suffix = f" (`{item_id}`)" if item_id else ""
                        lines.append(
                            f"- **{item_name}**{id_suffix}: {self._schedule_trigger_label(item)}; "
                            f"{state}; next {self._format_schedule_time(item.get('next_run_at'))}"
                        )
                    changes.append("\n".join(lines))
                continue
            if status == "applied":
                changes.append(self._schedule_applied_acknowledgement(action, result, job, display_name))
            elif status == "rejected":
                target = f" {display_name}" if display_name else ""
                lines = ["**Schedule not changed**"]
                if target:
                    lines.append(f"- Target:{target}")
                lines.append(f"- Reason: {reason or 'rejected'}")
                changes.append("\n".join(lines))
        return "\n\n".join(changes)

    def _schedule_applied_acknowledgement(
        self,
        action: str,
        result: dict[str, Any],
        job: dict[str, Any],
        display_name: str,
    ) -> str:
        title = {
            "create": "Schedule created",
            "update": "Schedule updated",
            "remove": "Schedule removed",
            "pause": "Schedule paused",
            "resume": "Schedule resumed",
            "run_now": "Schedule queued",
        }.get(action, "Schedule updated")
        job_id = str(result.get("job_id") or job.get("id") or "")
        name = display_name or job_id or "unnamed schedule"
        lines = [f"**{title}**", f"- Name: {name}"]
        if action == "pause":
            lines.append("- State: paused")
        elif action in {"create", "update", "resume", "run_now"}:
            lines.append(f"- Trigger: {self._schedule_trigger_label(job or result)}")
            next_run_at = result.get("next_run_at") or job.get("next_run_at")
            lines.append(f"- Next run: {self._format_schedule_time(next_run_at)}")
        confirmation_mentions = self._confirmation_mentions(job)
        if confirmation_mentions:
            lines.append(f"- Mentions on run: {', '.join(confirmation_mentions)}")
        if job_id:
            lines.append(f"- Job ID: `{job_id}`")
        return "\n".join(lines)

    def _schedule_trigger_label(self, job: dict[str, Any]) -> str:
        detail = job.get("schedule_detail")
        if not isinstance(detail, dict):
            detail = job.get("schedule") if isinstance(job.get("schedule"), dict) else {}
        if detail:
            described = self._describe_schedule_detail(detail)
            if described:
                return described
        schedule = str(job.get("schedule") or "").strip()
        return schedule or "unscheduled"

    def _describe_schedule_detail(self, schedule: dict[str, Any]) -> str:
        timezone_name = str(schedule.get("timezone") or self.config.schedule_timezone)
        kind = str(schedule.get("kind") or "")
        if kind == "once":
            return f"once at {self._format_schedule_time(schedule.get('run_at'), timezone_name=timezone_name)}"
        if kind == "interval":
            return f"every {self._duration_label(int(schedule.get('minutes') or 0))}"
        if kind == "cron":
            expr = str(schedule.get("expr") or "").strip()
            return self._cron_label(expr, timezone_name)
        return ""

    def _duration_label(self, minutes: int) -> str:
        if minutes > 0 and minutes % 1440 == 0:
            days = minutes // 1440
            return f"{days}d"
        if minutes > 0 and minutes % 60 == 0:
            hours = minutes // 60
            return f"{hours}h"
        return f"{minutes}m"

    def _cron_label(self, expr: str, timezone_name: str) -> str:
        parts = expr.split()
        if len(parts) >= 5:
            minute, hour, day, month, weekday = parts[:5]
            if day == "*" and month == "*" and weekday == "*" and minute.isdigit() and hour.isdigit():
                return f"every day at {int(hour):02d}:{int(minute):02d} {timezone_name}"
        return f"{expr} ({timezone_name})" if expr else f"cron ({timezone_name})"

    def _format_schedule_time(self, value: object, *, timezone_name: str | None = None) -> str:
        if not value:
            return "none"
        tz_name = timezone_name or self.config.schedule_timezone
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=zoneinfo_for(tz_name))
            local_dt = dt.astimezone(zoneinfo_for(tz_name))
            return f"{local_dt.strftime('%Y-%m-%d %H:%M')} {tz_name}"
        except (TypeError, ValueError):
            return str(value)

    def _memory_acknowledgement(self, memory_applied: list[dict[str, Any]]) -> str:
        changes = [
            self._memory_change_text(result)
            for result in memory_applied
            if result.get("status") == "applied"
        ]
        changes = [change for change in changes if change]
        if not changes:
            return ""
        if len(changes) == 1:
            return f"Memory updated: {changes[0]}"
        return "Memory updated:\n" + "\n".join(f"- {change}" for change in changes)

    def _memory_change_text(self, result: dict[str, Any]) -> str:
        scope = str(result.get("scope") or "conversation")
        op = str(result.get("op") or "")
        content = str(result.get("content") or "").strip()
        old_text = str(result.get("old_text") or "").strip()
        if op == "add" and content:
            return f"added {scope} memory: {content}"
        if op == "remove":
            removed = content or old_text
            if removed:
                return f"forgot {scope} memory: {removed}"
        if op == "replace" and content:
            if old_text:
                return f'replaced {scope} memory: "{old_text}" -> "{content}"'
            return f"replaced {scope} memory: {content}"
        return ""

    def _post_summary(self, post_result: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
        response = post_result.get("response") if isinstance(post_result.get("response"), dict) else post_result
        return {
            "status": str(response.get("result") or "unknown"),
            "dry_run": dry_run,
            "zulip_message_id": response.get("id"),
            "message": response.get("msg"),
        }
