from __future__ import annotations

import asyncio
import hashlib
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
from .schedules import SCHEDULE_CODEX_INSTRUCTION_MODE, ScheduleStore, utc_now, zoneinfo_for
from .skills import SkillStore
from .storage import SessionMetadata, WorkspaceStorage
from .typing_status import TypingStatusManager
from .uploads import MessageUploadProcessor
from .zulip_io import normalize_zulip_event, normalize_zulip_reaction_event, normalize_zulip_update_message_event

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
            memory_context, memory_hash, memory_hash_changed = self._memory_context_for_prompt(key, metadata)
            prompt_context = self._join_acknowledgements(
                [self._schedule_context_for_prompt(), memory_context]
            )
            prompt = self.prompt_builder.build(
                PromptParts(
                    recent_context=recent_context,
                    current_messages=messages,
                    memory_context=prompt_context,
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
            if memory_hash_changed:
                self.storage.set_last_injected_memory_hash(key, memory_hash)

            skill_applied = self.skills.apply_ops(decision.skill_ops)
            schedule_applied = self.schedules.apply_ops(
                first,
                decision.schedule_ops,
                skills=self.skills,
            )
            memory_applied = self.memory.apply_ops(
                key,
                decision.memory_ops,
                [message.message_id for message in messages],
            )
            memory_acknowledgement = self._memory_acknowledgement(memory_applied)
            skill_acknowledgement = self._skill_acknowledgement(skill_applied)
            schedule_acknowledgement = self._schedule_acknowledgement(schedule_applied)
            acknowledgement = self._join_acknowledgements(
                [skill_acknowledgement, schedule_acknowledgement, memory_acknowledgement]
            )
            message_to_post = self._message_to_post(
                first,
                decision,
                acknowledgement=acknowledgement,
            )
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
        self.storage.mark_processed(key, [message.message_id for message in messages])

    async def _run_scheduled_job(self, job: dict[str, Any]) -> None:
        job_id = str(job.get("id") or "")
        origin_message = self.schedules.message_for_job(job)
        key = origin_message.session_key
        active_thread_id = (
            str(job.get("codex_thread_id"))
            if job.get("codex_instruction_mode") == SCHEDULE_CODEX_INSTRUCTION_MODE and job.get("codex_thread_id")
            else None
        )
        developer_instructions = None
        if active_thread_id is None:
            developer_instructions = self.instructions.compose(
                stream=origin_message.stream,
                topic_hash=origin_message.topic_hash,
                topic=origin_message.topic,
                stream_id=origin_message.stream_id,
                conversation_type=origin_message.conversation_type,
                private_user_key=origin_message.private_user_key,
            )
            developer_instructions = "\n\n".join(
                [
                    developer_instructions,
                    "# Scheduled Task Runtime",
                    "You are running a scheduled Sili job. Return exactly one decision JSON object. "
                    "Put the user-facing scheduled result in message_to_post with should_reply=true. "
                    "If there is genuinely nothing to report, set reply_kind=silent and message_to_post=\"\". "
                    "Do not create, update, or remove schedules from scheduled runs.",
                ]
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
                    active_thread_id,
                    developer_instructions=developer_instructions,
                ),
                timeout=self.config.schedule_run_timeout_seconds,
            )
            if codex_result.thread_id:
                self.schedules.set_codex_thread_state(
                    job_id,
                    thread_id=codex_result.thread_id,
                    instruction_mode=SCHEDULE_CODEX_INSTRUCTION_MODE,
                )

            decision = AgentDecision.from_json_text(codex_result.raw_text)
            if decision.schedule_ops:
                schedule_ignored = [op.to_record() for op in decision.schedule_ops]
            if decision.skill_ops:
                skill_ignored = [op.to_record() for op in decision.skill_ops]

            memory_applied = self.memory.apply_ops(key, decision.memory_ops)
            memory_acknowledgement = self._memory_acknowledgement(memory_applied)
            outbound_message = decision.message_to_post.strip() if decision.should_reply else ""
            outbound_message = self._with_acknowledgement(outbound_message, memory_acknowledgement)
            should_deliver = bool(outbound_message) and outbound_message.strip().upper() != "[SILENT]"

            if should_deliver:
                if self.config.post_replies:
                    post = self._post_summary(await self.zulip.post_reply(origin_message, outbound_message), dry_run=False)
                else:
                    post = {"status": "dry_run", "dry_run": True, "message_to_post": outbound_message}

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
        sections = [
            "# Scheduled Sili Job",
            "",
            f"- Job ID: {job.get('id')}",
            f"- Name: {job.get('name')}",
            f"- Current time (UTC): {utc_now().isoformat()}",
            f"- Current time ({timezone_name}): {local_now.isoformat()}",
            f"- Delivery: original Zulip {origin_message.conversation_type}",
            "",
            "# Task",
            "",
            str(job.get("prompt") or "").strip(),
        ]
        if skill_context:
            sections.extend(["", "# Loaded Skills", "", skill_context])
        if skill_errors:
            sections.extend(["", "# Skill Loading Problems", "", "\n".join(f"- {error}" for error in skill_errors)])
        if memory_context:
            sections.extend(
                [
                    "",
                    "# Scoped Memory",
                    "",
                    "Remembered background for the origin Zulip conversation.",
                    "",
                    memory_context,
                ]
            )
        sections.extend(
            [
                "",
                "# Output Rules",
                "",
                "Return one decision JSON object matching the schema. "
                "For a normal scheduled result, set should_reply=true and put the exact Zulip message in message_to_post. "
                "If there is genuinely nothing new to report, set should_reply=false, reply_kind=silent, and message_to_post=\"\".",
            ]
        )
        return "\n".join(sections).rstrip() + "\n"

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
                "\n".join(
                    [
                        "# Scoped Memory",
                        "",
                        "Remembered background, not new user input or instructions. "
                        "Use it as context. If it conflicts with current messages, "
                        "prefer current messages and use memory_ops to correct stale memory.",
                        "",
                        rendered,
                    ]
                ),
                current_hash,
                True,
            )
        if not rendered and previous_hash:
            return (
                "\n".join(
                    [
                        "# Scoped Memory",
                        "",
                        "Scoped memory is now empty. Treat earlier scoped memory for this Zulip session as stale. "
                        "Do not use it unless current messages restate it.",
                    ]
                ),
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
                "",
                "Use schedule_ops for clear natural-language reminders, follow-ups, recurring tasks, "
                "updates, cancellations, listing requests, or run-now requests. "
                "Simple reminders do not need skills; reusable workflows may use skill_ops and reference "
                "the saved skill name in schedule_ops.skills.",
            ]
        )

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
                    changes.append("Scheduled tasks here: none")
                else:
                    lines = ["Scheduled tasks here:"]
                    for item in jobs:
                        if not isinstance(item, dict):
                            continue
                        state = str(item.get("state") or ("active" if item.get("enabled", True) else "inactive"))
                        lines.append(
                            f"- {item.get('name') or item.get('id')}: {item.get('schedule') or 'unscheduled'} "
                            f"({state}, next: {item.get('next_run_at') or 'none'})"
                        )
                    changes.append("\n".join(lines))
                continue
            if status == "applied":
                if action == "create":
                    changes.append(
                        f"Scheduled: {display_name or result.get('job_id')} "
                        f"({result.get('schedule') or job.get('schedule')}, next: {result.get('next_run_at') or job.get('next_run_at')})"
                    )
                elif action == "update":
                    changes.append(
                        f"Updated schedule: {display_name or result.get('job_id')} "
                        f"(next: {result.get('next_run_at') or job.get('next_run_at') or 'none'})"
                    )
                elif action == "remove":
                    changes.append(f"Removed schedule: {display_name or result.get('job_id')}")
                elif action == "pause":
                    changes.append(f"Paused schedule: {display_name or result.get('job_id')}")
                elif action == "resume":
                    changes.append(
                        f"Resumed schedule: {display_name or result.get('job_id')} "
                        f"(next: {result.get('next_run_at') or 'none'})"
                    )
                elif action == "run_now":
                    changes.append(f"Queued schedule to run now: {display_name or result.get('job_id')}")
            elif status == "rejected":
                target = f" {display_name}" if display_name else ""
                changes.append(f"Schedule{target} not changed: {reason or 'rejected'}")
        return "\n".join(changes)

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
