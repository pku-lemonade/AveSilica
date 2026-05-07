from __future__ import annotations

import asyncio
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
from .models import (
    AgentDecision,
    NormalizedMessage,
    NormalizedReaction,
    ReflectionDecision,
    PostDecision,
    ScheduleDecision,
    SessionKey,
    SkillDecision,
)
from .prompt import PromptBuilder
from .reflections import ReflectionStore
from .schedules import ScheduleStore, utc_now, zoneinfo_for
from .skills import SkillStore
from .storage import SessionMetadata, WorkspaceStorage
from .telemetry import TurnTelemetry
from .turn_context import RenderContext, TurnContext, WorkflowDeltas
from .typing_status import TypingStatusManager
from .uploads import MessageUploadProcessor
from .workspace import (
    POST_CONVERSATION_RECORDS_TEMPLATE_FILE,
    REFLECTIONS_DECISION_SCHEMA_FILE,
    REFLECTIONS_WORKER_USER_PROMPT_FILE,
    POST_DECISION_SCHEMA_FILE,
    POST_TURN_USER_PROMPT_FILE,
    SCHEDULED_JOB_DECISION_SCHEMA_FILE,
    SCHEDULED_JOB_USER_PROMPT_FILE,
    SCHEDULE_DECISION_SCHEMA_FILE,
    SCHEDULE_WORKER_USER_PROMPT_FILE,
    SKILL_DECISION_SCHEMA_FILE,
    SKILL_WORKER_USER_PROMPT_FILE,
)
from .zulip_io import normalize_zulip_event, normalize_zulip_reaction_event, normalize_zulip_update_message_event

LOGGER = logging.getLogger(__name__)
PRIVATE_POST_FALLBACK = "I saw this, but couldn't produce a useful message. Please try again."
CODEX_INSTRUCTION_MODE = "post-session-v6"


class ZulipPoster(Protocol):
    async def post_message(self, message: NormalizedMessage, content: str) -> dict[str, Any]:
        ...


@dataclass(frozen=True)
class EnqueueResult:
    accepted: bool
    reason: str
    session_key: str | None = None
    message_id: int | None = None


@dataclass(frozen=True)
class TurnPromptState:
    active_thread_id: str | None
    instruction_kwargs: dict[str, Any]
    post_developer_instructions: str | None
    pending_posted_bot_updates: list[dict[str, Any]]
    posted_bot_update_context: str
    worker_specs: list[CodexWorkerSpec]


@dataclass(frozen=True)
class SessionThreadState:
    thread_id: str | None
    developer_instructions: str | None


@dataclass(frozen=True)
class ReflectionsSkillResult:
    reflection_decision: ReflectionDecision
    reflection_applied: list[dict[str, Any]]
    skill_decision: SkillDecision
    skill_applied: list[dict[str, Any]]


class AgentLoop:
    def __init__(
        self,
        *,
        config: BotConfig,
        storage: WorkspaceStorage,
        instructions: InstructionLoader,
        reflections: ReflectionStore,
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
        self.reflections = reflections
        self.codex = codex
        self.zulip = zulip
        self.skills = skills or SkillStore(
            config.codex_cwd / ".codex" / "skills",
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
        if self.config.posting_enabled:
            post = self._post_summary(await self.zulip.post_message(message, content), dry_run=False)
            post["messages_to_post"] = [content]
            return post
        return {"status": "dry_run", "dry_run": True, "messages_to_post": [content]}

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
            post_kind = str(decision.get("post_kind") or "silent")
            confidence = self._format_confidence(decision.get("confidence"))
            lines.append(f"- Decision: {post_kind}{confidence}")
            summary["decision"] = post_kind
            summary["confidence"] = decision.get("confidence")

            posted = self._posted_text(latest_turn)
            if posted:
                lines.append(f"- Posted: {self._quote_excerpt(posted)}")
                summary["posted"] = True
            else:
                why = "chose not to post" if post_kind == "silent" else "no visible post"
                why += "; no runtime error" if latest_error is None else f"; latest error on {self._error_surface(latest_error)}"
                lines.append(f"- Why: {why}")
        elif latest_error is not None:
            lines.append("- Decision: failed before post")
            lines.append("- Why: post failed before a decision was logged")
            summary["decision"] = "failed before post"
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
        if worker in {"reflections", "skill", "schedule"}:
            return worker
        kind = str(error.get("kind") or "").strip().casefold()
        event = str(error.get("event") or "").strip().casefold()
        if "scheduled" in kind or "scheduled" in event:
            return "scheduled_job"
        if kind in {"turn_exception", "codex_thread_restarted"}:
            return "post"
        if kind == "worker_exception":
            return "runtime"
        return "runtime"

    def _error_text(self, error: dict[str, Any]) -> str:
        value = str(error.get("error") or error.get("reason") or error.get("kind") or error.get("event") or "error")
        return self._compact_text(value, limit=120)

    def _posted_text(self, turn: dict[str, Any]) -> str:
        post = turn.get("post") if isinstance(turn.get("post"), dict) else {}
        messages_to_post = post.get("messages_to_post")
        if isinstance(messages_to_post, list):
            messages = [str(message).strip() for message in messages_to_post if str(message).strip()]
            if messages:
                return self._join_post_messages(messages)
        # Older turn records used message_to_post; keep status output readable for them.
        message_to_post = str(post.get("message_to_post") or "").strip()
        if message_to_post:
            return message_to_post
        decision = turn.get("decision") if isinstance(turn.get("decision"), dict) else {}
        decision_messages = decision.get("messages_to_post")
        if isinstance(decision_messages, list):
            messages = [str(message).strip() for message in decision_messages if str(message).strip()]
            if messages:
                return self._join_post_messages(messages)
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

        telemetry = TurnTelemetry(source="conversation_turn")
        trace_id = telemetry.telemetry_id
        trace_roles: list[dict[str, Any]] = []
        first = messages[0]
        metadata = self.storage.load_metadata(key)
        async with AsyncExitStack() as stack:
            typing_started = False
            with telemetry.phase("prepare_messages"):
                if self.typing.should_show_typing(first, posting_enabled=self.config.posting_enabled):
                    await stack.enter_async_context(self.typing.active(first))
                    typing_started = True

                messages = await self.uploads.process_messages(messages)
                for processed_message in messages:
                    if processed_message.uploads:
                        self.storage.update_message(processed_message)

                first = messages[0]

            with telemetry.phase("build_worker_prompts"):
                prompt_state = self._build_turn_prompt_state(key, messages, first, metadata)

            session_thread = await self._ensure_session_thread(key, messages, prompt_state, telemetry)
            parent_thread_id = session_thread.thread_id
            worker_results = await self._run_worker_specs(
                key,
                messages,
                parent_thread_id,
                prompt_state.worker_specs,
                telemetry,
            )

            with telemetry.phase("apply_reflections_skill"):
                op_result = self._apply_reflections_and_skill_workers(
                    key,
                    messages,
                    worker_results,
                )
                trace_roles.extend(
                    self._worker_trace_roles(
                        parent_thread_id,
                        prompt_state.worker_specs,
                        worker_results,
                        {
                            "reflections": op_result.reflection_decision,
                            "skill": op_result.skill_decision,
                        },
                    )
                )

            with telemetry.phase("build_schedule_prompt"):
                schedule_spec = self._build_schedule_worker_spec(
                    key,
                    messages,
                    first,
                    op_result.skill_applied,
                    prompt_state.instruction_kwargs,
                )
            schedule_results = await self._run_worker_specs(
                key,
                messages,
                parent_thread_id,
                [schedule_spec],
                telemetry,
            )
            schedule_result = schedule_results.get("schedule")
            with telemetry.phase("apply_schedule"):
                schedule_decision, schedule_applied = self._apply_schedule_worker_result(
                    key,
                    first,
                    messages,
                    schedule_result,
                )
                trace_roles.append(
                    self._trace_role(
                        "schedule",
                        prompt=schedule_spec.prompt,
                        developer_instructions=schedule_spec.developer_instructions,
                        output_schema_path=schedule_spec.output_schema_path,
                        result=schedule_result,
                        decision=schedule_decision,
                        parent_thread_id=parent_thread_id,
                        worker_mode="fork",
                        status="ok" if schedule_result is not None else "error",
                        error=None if schedule_result is not None else "worker did not return a result",
                    )
                )

            with telemetry.phase("build_post_prompt"):
                skill_acknowledgement = self._skill_acknowledgement(op_result.skill_applied)
                schedule_acknowledgement = self._schedule_acknowledgement(schedule_applied)
                acknowledgement = self._join_acknowledgements(
                    [skill_acknowledgement, schedule_acknowledgement]
                )
                post_prompt = self._build_post_prompt(
                    messages,
                    prompt_state.posted_bot_update_context,
                    acknowledgement,
                )
            post_result, post_trace_developer_instructions = await self._run_post_decision(
                key,
                messages,
                parent_thread_id,
                session_thread.developer_instructions,
                prompt_state.instruction_kwargs,
                post_prompt,
                telemetry,
            )

            with telemetry.phase("apply_post_decision"):
                post_decision = PostDecision.from_json_text(post_result.raw_text)
                trace_roles.append(
                    self._trace_role(
                        "post",
                        prompt=post_prompt,
                        developer_instructions=post_trace_developer_instructions,
                        output_schema_path=self.config.workspace_dir / POST_DECISION_SCHEMA_FILE,
                        result=post_result,
                        decision=post_decision,
                        parent_thread_id=parent_thread_id,
                        worker_mode="persistent",
                    )
                )
                decision = AgentDecision.from_parts(
                    post_decision,
                    skill_ops=op_result.skill_decision.skill_ops,
                    schedule_ops=schedule_decision.schedule_ops,
                )
                outbound_messages = self._messages_to_post(
                    first,
                    decision,
                    acknowledgement=acknowledgement,
                )
                if self._post_conflicts_with_schedule_acknowledgement(
                    self._join_post_messages(outbound_messages),
                    schedule_acknowledgement,
                ):
                    outbound_messages = []
                outbound_messages = self._with_acknowledgement_messages(outbound_messages, acknowledgement)
                if typing_started and first.conversation_type == "stream" and not outbound_messages:
                    await stack.aclose()
                    typing_started = False

            post: dict[str, Any] | None = None
            if outbound_messages:
                with telemetry.phase("post_delivery"):
                    post = await self._post_messages(first, outbound_messages)
                    self._enqueue_posted_bot_update(
                        key,
                        source="conversation_turn",
                        content=self._join_post_messages(outbound_messages),
                        post=post,
                        acknowledgement=acknowledgement,
                        message_ids=[message.message_id for message in messages],
                    )

            timing = telemetry.finish()
            self.storage.log_turn(
                key=key,
                messages=messages,
                decision=decision,
                post=post,
                reflection_applied=op_result.reflection_applied,
                skill_applied=op_result.skill_applied,
                schedule_applied=schedule_applied,
                skill_acknowledgement=skill_acknowledgement,
                schedule_acknowledgement=schedule_acknowledgement,
                trace_id=trace_id,
                timing=timing,
            )
            self._log_trace(
                key,
                trace_id,
                source="conversation_turn",
                roles=trace_roles,
                message_ids=[message.message_id for message in messages],
                parent_thread_id=parent_thread_id,
                timing=timing,
            )
            self.storage.consume_posted_bot_updates(key, prompt_state.pending_posted_bot_updates)
        self.storage.mark_processed(key, [message.message_id for message in messages])

    def _build_turn_prompt_state(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        first: NormalizedMessage,
        metadata: SessionMetadata,
    ) -> TurnPromptState:
        if metadata.codex_thread_id and metadata.codex_instruction_mode != CODEX_INSTRUCTION_MODE:
            metadata = self.storage.clear_session_context(key, first)
        active_thread_id = (
            metadata.codex_thread_id
            if metadata.codex_instruction_mode == CODEX_INSTRUCTION_MODE and metadata.codex_thread_id
            else None
        )
        instruction_kwargs = self._conversation_instruction_kwargs(first)
        post_developer_instructions = (
            None if active_thread_id else self._post_developer_instructions(key, first, instruction_kwargs)
        )
        pending_posted_bot_updates = self.storage.read_pending_posted_bot_updates(key)
        return TurnPromptState(
            active_thread_id=active_thread_id,
            instruction_kwargs=instruction_kwargs,
            post_developer_instructions=post_developer_instructions,
            pending_posted_bot_updates=pending_posted_bot_updates,
            posted_bot_update_context=self._posted_bot_update_context(pending_posted_bot_updates),
            worker_specs=self._build_reflections_skill_worker_specs(messages, first, instruction_kwargs),
        )

    def _conversation_instruction_kwargs(self, message: NormalizedMessage) -> dict[str, Any]:
        return {
            "stream": message.stream,
            "topic_hash": message.topic_hash,
            "topic": message.topic,
            "stream_id": message.stream_id,
            "conversation_type": message.conversation_type,
            "private_recipient_key": message.private_recipient_key,
        }

    def _post_developer_instructions(
        self,
        key: SessionKey,
        message: NormalizedMessage,
        instruction_kwargs: dict[str, Any],
    ) -> str:
        return self.prompt_builder.render_sections(
            [
                self.instructions.compose(role="post", **instruction_kwargs),
                self.prompt_builder.render_template(
                    POST_CONVERSATION_RECORDS_TEMPLATE_FILE,
                    self._conversation_records_template_values(key, message),
                ),
            ]
        )

    def _conversation_records_template_values(
        self,
        key: SessionKey,
        message: NormalizedMessage,
    ) -> dict[str, object]:
        records_dir = self.storage.layout.relative(self.storage.session_dir(key))
        if key.conversation_type == "private":
            recipient_key = key.private_recipient_key or message.private_recipient_key or key.topic_hash
            return {
                "conversation_type": "private",
                "conversation_identity": f"private recipient key {recipient_key}",
                "conversation_display": "private message",
                "records_dir": records_dir,
            }
        return {
            "conversation_type": "stream/topic",
            "conversation_identity": f"stream id {message.stream_id}, topic hash {message.topic_hash}",
            "conversation_display": f"{message.stream} / {message.topic}",
            "records_dir": records_dir,
        }

    def _build_reflections_skill_worker_specs(
        self,
        messages: list[NormalizedMessage],
        first: NormalizedMessage,
        instruction_kwargs: dict[str, Any],
    ) -> list[CodexWorkerSpec]:
        reflections_prompt = self.prompt_builder.build(
            TurnContext.from_messages(
                messages,
                deltas=WorkflowDeltas(reflection_context=self._reflection_context_for_prompt(first)),
            ),
            role="reflections",
            template_file=REFLECTIONS_WORKER_USER_PROMPT_FILE,
        )
        skill_prompt = self.prompt_builder.build(
            TurnContext.from_messages(messages),
            role="skill",
            template_file=SKILL_WORKER_USER_PROMPT_FILE,
        )
        return [
            CodexWorkerSpec(
                kind="reflections",
                prompt=reflections_prompt,
                developer_instructions=self.instructions.compose(role="reflections_worker", **instruction_kwargs),
                output_schema_path=self.config.workspace_dir / REFLECTIONS_DECISION_SCHEMA_FILE,
            ),
            CodexWorkerSpec(
                kind="skill",
                prompt=skill_prompt,
                developer_instructions=self.instructions.compose(role="skill_worker", **instruction_kwargs),
                output_schema_path=self.config.workspace_dir / SKILL_DECISION_SCHEMA_FILE,
            ),
        ]

    async def _ensure_session_thread(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        prompt_state: TurnPromptState,
        telemetry: TurnTelemetry,
    ) -> SessionThreadState:
        phase = None
        post_developer_instructions = prompt_state.post_developer_instructions
        try:
            with telemetry.phase("ensure_session_thread") as phase:
                result = await self.codex.ensure_thread(
                    prompt_state.active_thread_id,
                    developer_instructions=post_developer_instructions,
                )
        except Exception as exc:
            if prompt_state.active_thread_id is None or not self._is_missing_codex_rollout_error(exc):
                raise
            self.storage.log_error(
                key,
                {
                    "kind": "codex_thread_restarted",
                    "thread_id": prompt_state.active_thread_id,
                    "error": repr(exc),
                    "message_ids": [message.message_id for message in messages],
                },
            )
            self.storage.set_codex_thread_state(key, thread_id=None, instruction_mode=None)
            post_developer_instructions = self._post_developer_instructions(
                key,
                messages[0],
                prompt_state.instruction_kwargs,
            )
            with telemetry.phase("ensure_session_thread_restarted") as phase:
                result = await self.codex.ensure_thread(
                    None,
                    developer_instructions=post_developer_instructions,
                )
        telemetry.add_codex_result(result, role="session_thread", phase=phase)
        if result.thread_id:
            self.storage.set_codex_thread_state(
                key,
                thread_id=result.thread_id,
                instruction_mode=CODEX_INSTRUCTION_MODE,
            )
        return SessionThreadState(result.thread_id, post_developer_instructions)

    async def _run_worker_specs(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        parent_thread_id: str | None,
        worker_specs: list[CodexWorkerSpec],
        telemetry: TurnTelemetry,
    ) -> dict[str, CodexRunResult | None]:
        worker_results: dict[str, CodexRunResult | None] = {}
        for spec in worker_specs:
            with telemetry.phase(f"{spec.kind}_worker") as phase:
                result = await self._run_op_worker(key, messages, parent_thread_id, spec)
            telemetry.add_codex_result(result, role=spec.kind, phase=phase)
            worker_results[spec.kind] = result
        return worker_results

    def _apply_reflections_and_skill_workers(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        worker_results: dict[str, CodexRunResult | None],
    ) -> ReflectionsSkillResult:
        reflection_decision, reflection_applied = self._apply_reflections_worker_result(
            key,
            messages,
            worker_results.get("reflections"),
        )
        skill_decision, skill_applied = self._apply_skill_worker_result(
            key,
            messages,
            worker_results.get("skill"),
        )
        return ReflectionsSkillResult(
            reflection_decision=reflection_decision,
            reflection_applied=reflection_applied,
            skill_decision=skill_decision,
            skill_applied=skill_applied,
        )

    def _worker_trace_roles(
        self,
        parent_thread_id: str | None,
        worker_specs: list[CodexWorkerSpec],
        worker_results: dict[str, CodexRunResult | None],
        decisions: dict[str, object],
    ) -> list[dict[str, Any]]:
        trace_roles: list[dict[str, Any]] = []
        for spec in worker_specs:
            result = worker_results.get(spec.kind)
            trace_roles.append(
                self._trace_role(
                    spec.kind,
                    prompt=spec.prompt,
                    developer_instructions=spec.developer_instructions,
                    output_schema_path=spec.output_schema_path,
                    result=result,
                    decision=decisions.get(spec.kind) or {},
                    parent_thread_id=parent_thread_id,
                    worker_mode="fork",
                    status="ok" if result is not None else "error",
                    error=None if result is not None else "worker did not return a result",
                )
            )
        return trace_roles

    def _build_schedule_worker_spec(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        first: NormalizedMessage,
        skill_applied: list[dict[str, Any]],
        instruction_kwargs: dict[str, Any],
    ) -> CodexWorkerSpec:
        schedule_prompt = self.prompt_builder.build(
            TurnContext.from_messages(
                messages,
                deltas=WorkflowDeltas(
                    scheduling_context=self._schedule_context_for_prompt(),
                    current_schedules=self._current_schedules_context(first),
                    mentionable_participants=self._mentionable_participants_context(key),
                    same_turn_skill_changes=self._skill_changes_context(skill_applied),
                ),
            ),
            role="schedule",
            template_file=SCHEDULE_WORKER_USER_PROMPT_FILE,
        )
        return CodexWorkerSpec(
            kind="schedule",
            prompt=schedule_prompt,
            developer_instructions=self.instructions.compose(
                role="schedule_worker",
                template_values={
                    "schedule_timezone": self.config.schedule_timezone,
                    "schedule_default_time": self.config.schedule_default_time,
                },
                **instruction_kwargs,
            ),
            output_schema_path=self.config.workspace_dir / SCHEDULE_DECISION_SCHEMA_FILE,
        )

    def _build_post_prompt(
        self,
        messages: list[NormalizedMessage],
        posted_bot_update_context: str,
        acknowledgement: str,
    ) -> str:
        return self.prompt_builder.build(
            TurnContext.from_messages(
                messages,
                deltas=WorkflowDeltas(
                    posted_bot_updates=posted_bot_update_context,
                    applied_changes=self._applied_changes_context(acknowledgement),
                ),
                render=RenderContext(message_timezone=self.config.schedule_timezone),
            ),
            role="post",
            template_file=POST_TURN_USER_PROMPT_FILE,
        )

    async def _run_post_decision(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        parent_thread_id: str | None,
        initial_developer_instructions: str | None,
        instruction_kwargs: dict[str, Any],
        post_prompt: str,
        telemetry: TurnTelemetry,
    ) -> tuple[CodexRunResult, str]:
        phase = None
        trace_developer_instructions = initial_developer_instructions or ""
        try:
            with telemetry.phase("post_decision") as phase:
                result = await self.codex.run_decision(
                    post_prompt,
                    parent_thread_id,
                    developer_instructions=None,
                    output_schema_path=self.config.workspace_dir / POST_DECISION_SCHEMA_FILE,
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
            trace_developer_instructions = self._post_developer_instructions(
                key,
                messages[0],
                instruction_kwargs,
            )
            with telemetry.phase("post_decision_restarted") as phase:
                result = await self.codex.run_decision(
                    post_prompt,
                    None,
                    developer_instructions=trace_developer_instructions,
                    output_schema_path=self.config.workspace_dir / POST_DECISION_SCHEMA_FILE,
                )
        telemetry.add_codex_result(result, role="post", phase=phase)
        if result.thread_id:
            self.storage.set_codex_thread_state(
                key,
                thread_id=result.thread_id,
                instruction_mode=CODEX_INSTRUCTION_MODE,
            )
        return result, trace_developer_instructions

    async def _run_scheduled_job(self, job: dict[str, Any]) -> None:
        job_id = str(job.get("id") or "")
        origin_message = self.schedules.message_for_job(job)
        key = origin_message.session_key
        telemetry = TurnTelemetry(source="scheduled_job")
        trace_id = telemetry.telemetry_id
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
        schedule_ignored: list[dict[str, Any]] = []
        skill_ignored: list[dict[str, Any]] = []
        trace_roles: list[dict[str, Any]] = []
        prompt = ""
        codex_result: CodexRunResult | None = None
        try:
            with telemetry.phase("build_prompt"):
                prompt = self._scheduled_prompt(job, origin_message)
            with telemetry.phase("scheduled_job_decision") as codex_phase:
                codex_result = await asyncio.wait_for(
                    self.codex.run_decision(
                        prompt,
                        None,
                        developer_instructions=developer_instructions,
                        output_schema_path=self.config.workspace_dir / SCHEDULED_JOB_DECISION_SCHEMA_FILE,
                    ),
                    timeout=self.config.schedule_run_timeout_seconds,
                )
            telemetry.add_codex_result(codex_result, role="scheduled_job", phase=codex_phase)
            trace_roles.append(
                self._trace_role(
                    "scheduled_job",
                    prompt=prompt,
                    developer_instructions=developer_instructions,
                    output_schema_path=self.config.workspace_dir / SCHEDULED_JOB_DECISION_SCHEMA_FILE,
                    result=codex_result,
                    decision={},
                    worker_mode="fresh",
                    status="captured",
                )
            )

            with telemetry.phase("apply_result"):
                decision = AgentDecision.from_json_text(codex_result.raw_text)
                trace_roles[-1] = self._trace_role(
                    "scheduled_job",
                    prompt=prompt,
                    developer_instructions=developer_instructions,
                    output_schema_path=self.config.workspace_dir / SCHEDULED_JOB_DECISION_SCHEMA_FILE,
                    result=codex_result,
                    decision=decision,
                    worker_mode="fresh",
                )
                if decision.schedule_ops:
                    schedule_ignored = [op.to_record() for op in decision.schedule_ops]
                if decision.skill_ops:
                    skill_ignored = [op.to_record() for op in decision.skill_ops]

                outbound_messages = self._scheduled_messages_to_post(job, decision)
                should_deliver = bool(outbound_messages)

            if should_deliver:
                with telemetry.phase("post_delivery"):
                    post = await self._post_messages(origin_message, outbound_messages)
                    self._enqueue_posted_bot_update(
                        key,
                        source="scheduled_job",
                        content=self._join_post_messages(outbound_messages),
                        post=post,
                        acknowledgement="",
                        job_id=job_id,
                    )

            timing = telemetry.finish()
            self._log_trace(
                key,
                trace_id,
                source="scheduled_job",
                roles=trace_roles,
                job_id=job_id,
                timing=timing,
            )
            self.schedules.log_run(
                job_id,
                {
                    "status": "ok" if outbound_messages or not should_deliver else "empty",
                    "trace_id": trace_id,
                    "decision": decision.to_record(),
                    "post": post,
                    "ignored_schedule_ops": schedule_ignored,
                    "ignored_skill_ops": skill_ignored,
                    "timing": timing,
                },
            )
            self.storage.append_telemetry_stats(
                key,
                timing,
                source="scheduled_job",
                job_id=job_id,
            )
            self.schedules.mark_job_run(
                job_id,
                success=bool(outbound_messages) or not should_deliver,
                error=None if outbound_messages or not should_deliver else "scheduled task produced no output",
            )
        except Exception as exc:
            LOGGER.exception("Scheduled job %s failed", job_id)
            error = repr(exc)
            timing = telemetry.finish(status="error")
            if trace_roles:
                trace_roles[-1]["status"] = "error"
                trace_roles[-1]["error"] = error
            elif prompt:
                trace_roles.append(
                    self._trace_role(
                        "scheduled_job",
                        prompt=prompt,
                        developer_instructions=developer_instructions,
                        output_schema_path=self.config.workspace_dir / SCHEDULED_JOB_DECISION_SCHEMA_FILE,
                        result=codex_result,
                        decision={},
                        worker_mode="fresh",
                        status="error",
                        error=error,
                    )
                )
            self._log_trace(
                key,
                trace_id,
                source="scheduled_job",
                roles=trace_roles,
                job_id=job_id,
                timing=timing,
            )
            self.schedules.log_run(
                job_id,
                {
                    "status": "error",
                    "trace_id": trace_id,
                    "error": error,
                    "post": post,
                    "timing": timing,
                },
            )
            self.storage.append_telemetry_stats(
                key,
                timing,
                source="scheduled_job",
                job_id=job_id,
            )
            self.schedules.mark_job_run(job_id, success=False, error=error)

    def _scheduled_prompt(self, job: dict[str, Any], origin_message: NormalizedMessage) -> str:
        timezone_name = self.config.schedule_timezone
        local_now = utc_now().astimezone(zoneinfo_for(timezone_name))
        skill_context, skill_errors = self.skills.render_for_prompt(job.get("skills") or [])
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
                "loaded_skills_section": self.prompt_builder.render_section("Loaded Skills", skill_context),
                "skill_errors_section": self.prompt_builder.render_section(
                    "Skill Loading Problems",
                    "\n".join(f"- {error}" for error in skill_errors),
                ),
            },
        )

    def _reflection_context_for_prompt(self, message: NormalizedMessage) -> str:
        if message.conversation_type == "private":
            source = f"private-recipient-{message.private_recipient_key or message.topic_hash}"
            source_scope = "source resolves to the current DM/group chat"
        else:
            source = f"stream-{message.stream_slug}-{message.stream_id}"
            source_scope = "source resolves to the current public channel; never the topic"
        return "\n".join(
            [
                "# Reflection Scope",
                "",
                f"- Current source: {source}",
                f"- Source behavior: {source_scope}",
                "- Allowed scopes: global, source",
                "- Reflections are not injected into future prompts; they are review candidates.",
            ]
        )

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

    def _with_scheduled_mention_messages(self, job: dict[str, Any], messages: list[str]) -> list[str]:
        if not messages:
            return []
        joined = self._join_post_messages(messages)
        mentions = [
            mention
            for target, mention in self._scheduled_mentions(job)
            if not self._normal_mention_already_present(target, joined)
        ]
        if not mentions:
            return messages
        mention_text = " ".join(mentions)
        if self._is_slash_widget_message(messages[0]):
            return [mention_text, *messages]
        return [f"{mention_text} {messages[0]}".strip(), *messages[1:]]

    def _scheduled_mentions(self, job: dict[str, Any]) -> list[tuple[dict[str, Any], str]]:
        mentions: list[tuple[dict[str, Any], str]] = []
        for target in self._job_mention_targets(job):
            mention = self._mention_text(target)
            if mention and all(existing != mention for _, existing in mentions):
                mentions.append((target, mention))
        return mentions

    def _normal_mention_already_present(self, target: dict[str, Any], content: str) -> bool:
        kind = str(target.get("kind") or "").strip().lower()
        if kind == "person":
            full_name = str(target.get("full_name") or "").strip()
            if not full_name:
                return False
            pattern = rf"@\*\*{re.escape(full_name)}(?:\|\d+)?\*\*"
            return re.search(pattern, content) is not None
        mention = self._mention_text(target)
        return bool(mention and mention in content)

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

    def _skill_changes_context(self, skill_applied: list[dict[str, Any]]) -> str:
        if not skill_applied:
            return ""
        sections = ["# Skill Changes This Turn", ""]
        for result in skill_applied:
            status = str(result.get("status") or "unknown")
            action = str(result.get("action") or "unknown")
            name = str(result.get("name") or "").strip()
            reason = str(result.get("reason") or "").strip()
            target = f" `{name}`" if name else ""
            detail = f": {reason}" if reason else ""
            sections.append(f"- {status} {action}{target}{detail}")
        return "\n".join(sections).rstrip()

    def _trace_role(
        self,
        role: str,
        *,
        prompt: str,
        developer_instructions: str,
        output_schema_path: object,
        result: CodexRunResult | None,
        decision: object,
        parent_thread_id: str | None = None,
        worker_mode: str = "",
        status: str = "ok",
        error: str | None = None,
    ) -> dict[str, Any]:
        return {
            "role": role,
            "prompt": prompt,
            "developer_instructions": developer_instructions,
            "developer_instructions_sent": bool(developer_instructions.strip()),
            "output_schema_path": output_schema_path,
            "raw_output": result.raw_text if result is not None else "",
            "decision": self._trace_decision(decision),
            "thread_id": result.thread_id if result is not None else None,
            "parent_thread_id": parent_thread_id,
            "worker_mode": worker_mode,
            "status": status,
            "error": error,
            "stats": result.stats if result is not None else None,
        }

    def _trace_decision(self, decision: object) -> dict[str, Any]:
        raw = getattr(decision, "raw", None)
        if isinstance(raw, dict) and raw:
            return raw
        if isinstance(decision, AgentDecision):
            return decision.to_record()
        if isinstance(decision, PostDecision):
            return decision.to_record()
        if isinstance(decision, ReflectionDecision):
            return {"reflection_ops": [item.to_record() for item in decision.reflection_ops]}
        if isinstance(decision, SkillDecision):
            return {"skill_ops": [item.to_record() for item in decision.skill_ops]}
        if isinstance(decision, ScheduleDecision):
            return {"schedule_ops": [item.to_record() for item in decision.schedule_ops]}
        return {}

    def _log_trace(
        self,
        key: SessionKey,
        trace_id: str,
        *,
        source: str,
        roles: list[dict[str, Any]],
        message_ids: list[int] | None = None,
        job_id: str | None = None,
        parent_thread_id: str | None = None,
        timing: dict[str, Any] | None = None,
    ) -> None:
        try:
            self.storage.log_trace(
                key,
                trace_id,
                source=source,
                roles=roles,
                message_ids=message_ids,
                job_id=job_id,
                model=self.config.codex_model,
                parent_thread_id=parent_thread_id,
                timing=timing,
            )
        except Exception:
            LOGGER.exception("Unable to write prompt trace %s", trace_id)

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
                "These changes have already been validated and persisted by TokenZulip before this post decision.",
                "",
                "```text",
                acknowledgement.strip(),
                "```",
            ]
        )

    def _post_conflicts_with_schedule_acknowledgement(
        self,
        content: str,
        schedule_acknowledgement: str,
    ) -> bool:
        if not content.strip() or not schedule_acknowledgement.strip():
            return False
        acknowledgement = schedule_acknowledgement.casefold()
        if "schedule removed" not in acknowledgement and "scheduled tasks here" not in acknowledgement:
            return False
        text = (
            content.casefold()
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
            "post-only context",
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

    async def _post_messages(
        self,
        origin: NormalizedMessage,
        messages: list[str],
    ) -> dict[str, Any]:
        delivered: list[dict[str, Any]] = []
        for content in messages:
            if self.config.posting_enabled:
                summary = self._post_summary(await self.zulip.post_message(origin, content), dry_run=False)
            else:
                summary = {"status": "dry_run", "dry_run": True}
            summary["messages_to_post"] = [content]
            delivered.append(summary)
        return self._post_record(delivered, messages)

    def _post_record(self, delivered: list[dict[str, Any]], messages: list[str]) -> dict[str, Any]:
        if len(delivered) == 1:
            return delivered[0]
        statuses = {str(item.get("status") or "").lower() for item in delivered}
        dry_run = all(item.get("dry_run") is True for item in delivered)
        status = "success" if statuses <= {"success"} else "dry_run" if dry_run else "partial"
        return {
            "status": status,
            "dry_run": dry_run,
            "messages_to_post": list(messages),
            "posts": delivered,
        }

    def _scheduled_messages_to_post(self, job: dict[str, Any], decision: AgentDecision) -> list[str]:
        messages = self._decision_messages_to_post(decision)
        messages = [message for message in messages if message.strip().upper() != "[SILENT]"]
        return self._with_scheduled_mention_messages(job, messages)

    @staticmethod
    def _is_missing_codex_rollout_error(exc: Exception) -> bool:
        return "no rollout found for thread id" in str(exc).casefold()

    def _post_was_visible(self, post: dict[str, Any] | None) -> bool:
        if not post:
            return False
        if post.get("dry_run") is True:
            return True
        return str(post.get("status") or "").lower() == "success"

    def _messages_to_post(
        self,
        first: NormalizedMessage,
        decision: AgentDecision,
        *,
        acknowledgement: str = "",
    ) -> list[str]:
        messages = self._decision_messages_to_post(decision)
        if decision.should_post and messages:
            return messages
        if first.post_required and acknowledgement and not messages:
            return []
        if first.post_required:
            return messages or [PRIVATE_POST_FALLBACK]
        return []

    def _decision_messages_to_post(
        self,
        decision: AgentDecision,
        *,
        require_should_post: bool = True,
    ) -> list[str]:
        if require_should_post and not decision.should_post:
            return []
        return [message.strip() for message in decision.messages_to_post if message.strip()]

    def _apply_reflections_worker_result(
        self,
        key: SessionKey,
        messages: list[NormalizedMessage],
        result: CodexRunResult | None,
    ) -> tuple[ReflectionDecision, list[dict[str, Any]]]:
        if result is None:
            return ReflectionDecision(), []
        try:
            decision = ReflectionDecision.from_json_text(result.raw_text)
            applied = self.reflections.apply_ops(
                key,
                decision.reflection_ops,
                [message.message_id for message in messages],
            )
            return decision, applied
        except Exception as exc:
            self.storage.log_error(
                key,
                {
                    "event": "op_worker_apply_failed",
                    "worker": "reflections",
                    "error": str(exc),
                    "thread_id": result.thread_id,
                    "message_ids": [message.message_id for message in messages],
                },
            )
            return ReflectionDecision(), []

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

    def _with_acknowledgement(self, message: str, acknowledgement: str) -> str:
        if not acknowledgement:
            return message
        if not message:
            return acknowledgement
        return f"{message}\n\n{acknowledgement}"

    def _with_acknowledgement_messages(self, messages: list[str], acknowledgement: str) -> list[str]:
        acknowledgement = acknowledgement.strip()
        if not acknowledgement:
            return messages
        if not messages:
            return [acknowledgement]
        if self._is_slash_widget_message(messages[-1]):
            return [*messages, acknowledgement]
        return [*messages[:-1], self._with_acknowledgement(messages[-1], acknowledgement)]

    @staticmethod
    def _is_slash_widget_message(message: str) -> bool:
        for line in message.splitlines():
            stripped = line.strip()
            if stripped:
                return re.match(r"^/(?:poll|todo)(?:\s|$)", stripped, flags=re.IGNORECASE) is not None
        return False

    @staticmethod
    def _join_post_messages(messages: list[str]) -> str:
        return "\n\n".join(message.strip() for message in messages if message.strip())

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
                            f"- **{item_name}**{id_suffix}: {self._schedule_trigger_label(item, visible=True)}; "
                            f"{state}; next {self._format_visible_schedule_time(item.get('next_run_at'))}"
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
            lines.append(f"- Trigger: {self._schedule_trigger_label(job or result, visible=True)}")
            next_run_at = result.get("next_run_at") or job.get("next_run_at")
            lines.append(f"- Next run: {self._format_visible_schedule_time(next_run_at)}")
        confirmation_mentions = self._confirmation_mentions(job)
        if confirmation_mentions:
            lines.append(f"- Mentions on run: {', '.join(confirmation_mentions)}")
        if job_id:
            lines.append(f"- Job ID: `{job_id}`")
        return "\n".join(lines)

    def _schedule_trigger_label(self, job: dict[str, Any], *, visible: bool = False) -> str:
        detail = job.get("schedule_detail")
        if not isinstance(detail, dict):
            detail = job.get("schedule") if isinstance(job.get("schedule"), dict) else {}
        if detail:
            described = self._describe_schedule_detail(detail, visible=visible)
            if described:
                return described
        schedule = str(job.get("schedule") or "").strip()
        return schedule or "unscheduled"

    def _describe_schedule_detail(self, schedule: dict[str, Any], *, visible: bool = False) -> str:
        timezone_name = str(schedule.get("timezone") or self.config.schedule_timezone)
        kind = str(schedule.get("kind") or "")
        if kind == "once":
            formatter = self._format_visible_schedule_time if visible else self._format_schedule_time
            return f"once at {formatter(schedule.get('run_at'), timezone_name=timezone_name)}"
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

    def _format_visible_schedule_time(self, value: object, *, timezone_name: str | None = None) -> str:
        if not value:
            return "none"
        tz_name = timezone_name or self.config.schedule_timezone
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=zoneinfo_for(tz_name))
            local_dt = dt.astimezone(zoneinfo_for(tz_name))
            return f"<time:{local_dt.isoformat(timespec='seconds')}>"
        except (TypeError, ValueError):
            return str(value)

    def _post_summary(self, post_result: dict[str, Any], *, dry_run: bool) -> dict[str, Any]:
        response = post_result.get("response") if isinstance(post_result.get("response"), dict) else post_result
        return {
            "status": str(response.get("result") or "unknown"),
            "dry_run": dry_run,
            "zulip_message_id": response.get("id"),
            "message": response.get("msg"),
        }
