from __future__ import annotations

import copy
import json
import os
import re
import tempfile
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Protocol
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

try:
    from croniter import croniter  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - exercised by fallback cron tests
    croniter = None

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows fallback
    fcntl = None

from .models import (
    NormalizedMessage,
    NormalizedMessageMove,
    ScheduleMentionTarget,
    ScheduleOperation,
    ScheduleSpec,
    SessionKey,
    safe_slug,
    utc_now_iso,
)
from .layout import WorkspaceLayout


SCHEDULES_FILENAME = "jobs.json"
SCHEDULE_OUTPUT_FILENAME = "runs.jsonl"
ONESHOT_GRACE_SECONDS = 120

_jobs_file_lock = threading.Lock()


class SkillLookup(Protocol):
    def skill_exists(self, name: str) -> bool:
        ...

    def validate_name(self, name: str) -> str:
        ...


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def zoneinfo_for(name: str) -> ZoneInfo:
    try:
        return ZoneInfo(name)
    except ZoneInfoNotFoundError as exc:
        raise ValueError(f"unknown timezone: {name}") from exc


def utc_iso(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def parse_duration(value: str) -> int:
    text = value.strip().lower()
    match = re.fullmatch(r"(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)", text)
    if not match:
        raise ValueError(f"invalid duration: {value!r}")
    amount = int(match.group(1))
    unit = match.group(2)[0]
    return amount * {"m": 1, "h": 60, "d": 1440}[unit]


def parse_schedule_spec(spec: ScheduleSpec, timezone_name: str) -> dict[str, Any]:
    kind = spec.kind
    if kind == "unchanged":
        raise ValueError("schedule_spec is unchanged")
    if kind == "once_at":
        return _parse_once_at(spec.run_at, timezone_name)
    if kind == "once_in":
        minutes = parse_duration(spec.duration)
        run_at = utc_now() + timedelta(minutes=minutes)
        return {
            "kind": "once",
            "run_at": utc_iso(run_at),
            "display": f"once in {spec.duration.strip()}",
            "timezone": timezone_name,
        }
    if kind == "interval":
        minutes = parse_duration(spec.duration)
        return {
            "kind": "interval",
            "minutes": minutes,
            "display": f"every {minutes}m",
            "timezone": timezone_name,
        }
    if kind == "cron":
        cron_expr = spec.cron.strip()
        if not cron_expr:
            raise ValueError("cron is required for cron schedule_spec")
        tz = zoneinfo_for(timezone_name)
        _validate_cron_expr(cron_expr, tz)
        return {
            "kind": "cron",
            "expr": cron_expr,
            "display": cron_expr,
            "timezone": timezone_name,
        }
    raise ValueError(f"invalid schedule_spec kind: {kind!r}")


def _parse_once_at(value: str, timezone_name: str) -> dict[str, Any]:
    run_at = value.strip()
    if not run_at:
        raise ValueError("run_at is required for once_at schedule_spec")
    tz = zoneinfo_for(timezone_name)
    text = run_at.replace("Z", "+00:00")
    if " " in text and "T" not in text:
        text = text.replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"invalid ISO timestamp: {run_at!r}") from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=tz)
    return {
        "kind": "once",
        "run_at": utc_iso(dt),
        "display": f"once at {_format_local_dt(dt, timezone_name)}",
        "timezone": timezone_name,
    }


def _format_local_dt(dt: datetime, timezone_name: str) -> str:
    tz = zoneinfo_for(timezone_name)
    return f"{dt.astimezone(tz).strftime('%Y-%m-%d %H:%M')} {timezone_name}"


def _operation_has_schedule(op: ScheduleOperation) -> bool:
    return op.schedule_spec.has_schedule()


def _parse_operation_schedule(op: ScheduleOperation, timezone_name: str) -> dict[str, Any]:
    return parse_schedule_spec(op.schedule_spec, timezone_name)


def compute_next_run(schedule: dict[str, Any], timezone_name: str, last_run_at: str | None = None) -> str | None:
    now = utc_now()
    kind = schedule.get("kind")
    if kind == "once":
        if last_run_at:
            return None
        run_at = schedule.get("run_at")
        if not run_at:
            return None
        run_at_dt = _parse_aware(str(run_at))
        if run_at_dt >= now - timedelta(seconds=ONESHOT_GRACE_SECONDS):
            return utc_iso(run_at_dt)
        return None

    if kind == "interval":
        minutes = int(schedule.get("minutes") or 0)
        if minutes <= 0:
            return None
        base = _parse_aware(last_run_at) if last_run_at else now
        return utc_iso(base + timedelta(minutes=minutes))

    if kind == "cron":
        expr = str(schedule.get("expr") or "")
        tz = zoneinfo_for(str(schedule.get("timezone") or timezone_name))
        return utc_iso(_cron_next(expr, now.astimezone(tz)))

    return None


class ScheduleStore:
    def __init__(self, workspace_dir: Path, *, timezone_name: str = "UTC") -> None:
        self.workspace_dir = workspace_dir.expanduser().resolve()
        self.layout = WorkspaceLayout(self.workspace_dir)
        self.timezone_name = timezone_name
        self.schedules_dir = self.workspace_dir / "schedules"
        self.scheduled_runs_dir = self.layout.scheduled_runs_dir
        self.jobs_file = self.schedules_dir / SCHEDULES_FILENAME
        self.lock_file = self.schedules_dir / ".jobs.lock"
        self.ensure_dirs()

    def ensure_dirs(self) -> None:
        self.schedules_dir.mkdir(parents=True, exist_ok=True)
        self.scheduled_runs_dir.mkdir(parents=True, exist_ok=True)

    def apply_ops(
        self,
        origin: NormalizedMessage,
        ops: list[ScheduleOperation],
        *,
        skills: SkillLookup | None = None,
        mentionable_users: dict[int, str] | None = None,
    ) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for op in ops:
            try:
                if op.action == "create":
                    results.append(
                        self.create_job(
                            origin,
                            op,
                            skills=skills,
                            mentionable_users=mentionable_users,
                        )
                    )
                elif op.action == "list":
                    results.append(self.list_context_jobs(origin))
                elif op.action == "update":
                    results.append(
                        self.update_job(
                            origin,
                            op,
                            skills=skills,
                            mentionable_users=mentionable_users,
                        )
                    )
                elif op.action == "remove":
                    results.append(self.remove_job(origin, op))
                elif op.action == "pause":
                    results.append(self.pause_job(origin, op))
                elif op.action == "resume":
                    results.append(self.resume_job(origin, op))
                elif op.action == "run_now":
                    results.append(self.trigger_job(origin, op))
            except Exception as exc:
                results.append(
                    {
                        "action": op.action,
                        "status": "rejected",
                        "reason": str(exc),
                        "job_id": op.job_id or None,
                        "name": op.name or None,
                    }
                )
        return results

    def create_job(
        self,
        origin: NormalizedMessage,
        op: ScheduleOperation,
        *,
        skills: SkillLookup | None = None,
        mentionable_users: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        if not op.prompt.strip():
            return self._rejected("create", "prompt is required", name=op.name)
        if not _operation_has_schedule(op):
            return self._rejected("create", "schedule is required", name=op.name)
        skill_names = self._validated_skills(op.skills, skills)
        mention_targets = self._validated_mention_targets(
            op.mention_targets,
            mentionable_users,
            prompt=op.prompt,
        )
        schedule = _parse_operation_schedule(op, self.timezone_name)
        next_run_at = compute_next_run(schedule, self.timezone_name)
        if next_run_at is None:
            return self._rejected("create", "schedule has no future run", name=op.name)

        now = utc_now_iso()
        job_id = uuid.uuid4().hex[:12]
        repeat_times = op.repeat
        if repeat_times is None and schedule.get("kind") == "once":
            repeat_times = 1
        job = {
            "id": job_id,
            "name": op.name.strip() or op.prompt.strip()[:60],
            "prompt": op.prompt.strip(),
            "skills": skill_names,
            "mention_targets": mention_targets,
            "schedule": schedule,
            "schedule_display": schedule.get("display", ""),
            "repeat": {"times": repeat_times, "completed": 0},
            "enabled": True,
            "state": "scheduled",
            "created_at": now,
            "updated_at": now,
            "next_run_at": next_run_at,
            "last_run_at": None,
            "last_status": None,
            "last_error": None,
            "last_delivery_error": None,
            "origin": self.origin_record(origin),
        }
        with self._locked_jobs() as jobs:
            jobs.append(job)
        return {
            "action": "create",
            "status": "applied",
            "job": self._public_job(job),
            "job_id": job_id,
            "name": job["name"],
            "next_run_at": next_run_at,
            "schedule": job["schedule_display"],
        }

    def update_job(
        self,
        origin: NormalizedMessage,
        op: ScheduleOperation,
        *,
        skills: SkillLookup | None = None,
        mentionable_users: dict[int, str] | None = None,
    ) -> dict[str, Any]:
        with self._locked_jobs() as jobs:
            index, job = self._resolve_job(jobs, origin, op)
            if job is None or index is None:
                return self._rejected("update", "job not found", op)
            updated = copy.deepcopy(job)
            if op.name.strip():
                updated["name"] = op.name.strip()
            if op.prompt.strip():
                updated["prompt"] = op.prompt.strip()
            if op.skills:
                updated["skills"] = self._validated_skills(op.skills, skills)
            if op.mention_targets:
                updated["mention_targets"] = self._validated_mention_targets(
                    op.mention_targets,
                    mentionable_users,
                    prompt=op.prompt or str(updated.get("prompt") or ""),
                )
            if op.repeat is not None:
                repeat_state = dict(updated.get("repeat") or {})
                repeat_state["times"] = op.repeat
                updated["repeat"] = repeat_state
            if _operation_has_schedule(op):
                schedule = _parse_operation_schedule(op, self.timezone_name)
                updated["schedule"] = schedule
                updated["schedule_display"] = schedule.get("display", "")
                if updated.get("state") != "paused":
                    updated["enabled"] = True
                    updated["state"] = "scheduled"
                    updated["next_run_at"] = compute_next_run(schedule, self.timezone_name)
            updated["updated_at"] = utc_now_iso()
            jobs[index] = updated
        return {
            "action": "update",
            "status": "applied",
            "job": self._public_job(updated),
            "job_id": updated["id"],
            "name": updated["name"],
            "next_run_at": updated.get("next_run_at"),
            "schedule": updated.get("schedule_display"),
        }

    def remove_job(self, origin: NormalizedMessage, op: ScheduleOperation) -> dict[str, Any]:
        with self._locked_jobs() as jobs:
            index, job = self._resolve_job(jobs, origin, op)
            if job is None or index is None:
                return self._rejected("remove", "job not found", op)
            jobs.pop(index)
        return {
            "action": "remove",
            "status": "applied",
            "job_id": job["id"],
            "name": job["name"],
        }

    def pause_job(self, origin: NormalizedMessage, op: ScheduleOperation) -> dict[str, Any]:
        return self._set_enabled(origin, op, enabled=False, state="paused")

    def resume_job(self, origin: NormalizedMessage, op: ScheduleOperation) -> dict[str, Any]:
        with self._locked_jobs() as jobs:
            index, job = self._resolve_job(jobs, origin, op)
            if job is None or index is None:
                return self._rejected("resume", "job not found", op)
            updated = copy.deepcopy(job)
            updated["enabled"] = True
            updated["state"] = "scheduled"
            updated["next_run_at"] = compute_next_run(updated["schedule"], self.timezone_name)
            updated["updated_at"] = utc_now_iso()
            jobs[index] = updated
        return {
            "action": "resume",
            "status": "applied",
            "job": self._public_job(updated),
            "job_id": updated["id"],
            "name": updated["name"],
            "next_run_at": updated.get("next_run_at"),
        }

    def trigger_job(self, origin: NormalizedMessage, op: ScheduleOperation) -> dict[str, Any]:
        with self._locked_jobs() as jobs:
            index, job = self._resolve_job(jobs, origin, op)
            if job is None or index is None:
                return self._rejected("run_now", "job not found", op)
            updated = copy.deepcopy(job)
            updated["enabled"] = True
            updated["state"] = "scheduled"
            updated["next_run_at"] = utc_iso(utc_now())
            updated["updated_at"] = utc_now_iso()
            jobs[index] = updated
        return {
            "action": "run_now",
            "status": "applied",
            "job": self._public_job(updated),
            "job_id": updated["id"],
            "name": updated["name"],
            "next_run_at": updated.get("next_run_at"),
        }

    def list_context_jobs(self, origin: NormalizedMessage) -> dict[str, Any]:
        jobs = [self._public_job(job) for job in self.load_jobs() if self._same_origin(job, origin)]
        return {
            "action": "list",
            "status": "applied",
            "count": len(jobs),
            "jobs": jobs,
        }

    def apply_message_move(self, move: NormalizedMessageMove) -> int:
        if move.propagate_mode != "change_all":
            return 0
        moved = 0
        with self._locked_jobs() as jobs:
            for index, job in enumerate(jobs):
                origin = job.get("origin") if isinstance(job.get("origin"), dict) else {}
                if origin.get("session_key") != move.source_key.value:
                    continue
                updated = copy.deepcopy(job)
                next_origin = dict(origin)
                destination_key = move.destination_key
                stream_slug = destination_key.stream_slug or safe_slug(move.stream_name)
                next_origin.update(
                    {
                        "session_key": destination_key.value,
                        "stream_id": destination_key.stream_id,
                        "stream": move.stream_name,
                        "stream_slug": stream_slug,
                        "topic": move.subject,
                        "topic_hash": destination_key.topic_hash,
                        "topic_slug": destination_key.topic_slug or safe_slug(move.subject),
                    }
                )
                updated["origin"] = next_origin
                updated["updated_at"] = utc_now_iso()
                jobs[index] = updated
                moved += 1
        return moved

    def load_jobs(self) -> list[dict[str, Any]]:
        self.ensure_dirs()
        if not self.jobs_file.exists():
            return []
        try:
            data = json.loads(self.jobs_file.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return []
        jobs = data.get("jobs") if isinstance(data, dict) else []
        return [job for job in jobs if isinstance(job, dict)]

    def save_jobs(self, jobs: list[dict[str, Any]]) -> None:
        self.ensure_dirs()
        fd, tmp_name = tempfile.mkstemp(dir=str(self.schedules_dir), prefix=".jobs-", suffix=".tmp")
        tmp_path = Path(tmp_name)
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                json.dump({"jobs": jobs, "updated_at": utc_now_iso()}, handle, indent=2, ensure_ascii=False)
                handle.write("\n")
                handle.flush()
                os.fsync(handle.fileno())
            tmp_path.replace(self.jobs_file)
        except BaseException:
            try:
                tmp_path.unlink()
            except OSError:
                pass
            raise

    def get_due_jobs(self) -> list[dict[str, Any]]:
        now = utc_now()
        due: list[dict[str, Any]] = []
        with self._locked_jobs() as jobs:
            for index, job in enumerate(jobs):
                if not job.get("enabled", True):
                    continue
                next_run_at = job.get("next_run_at")
                if not next_run_at:
                    continue
                next_run_dt = _parse_aware(str(next_run_at))
                if next_run_dt > now:
                    continue
                kind = (job.get("schedule") or {}).get("kind")
                if kind in {"interval", "cron"} and (now - next_run_dt).total_seconds() > self._grace_seconds(job):
                    updated = copy.deepcopy(job)
                    updated["next_run_at"] = compute_next_run(updated["schedule"], self.timezone_name, now.isoformat())
                    updated["updated_at"] = utc_now_iso()
                    jobs[index] = updated
                    continue
                due.append(copy.deepcopy(job))
        return due

    def advance_next_run(self, job_id: str) -> bool:
        with self._locked_jobs() as jobs:
            for index, job in enumerate(jobs):
                if job.get("id") != job_id:
                    continue
                kind = (job.get("schedule") or {}).get("kind")
                if kind not in {"interval", "cron"}:
                    return False
                updated = copy.deepcopy(job)
                updated["next_run_at"] = compute_next_run(updated["schedule"], self.timezone_name, utc_now().isoformat())
                updated["updated_at"] = utc_now_iso()
                jobs[index] = updated
                return True
        return False

    def mark_job_run(
        self,
        job_id: str,
        *,
        success: bool,
        error: str | None = None,
        delivery_error: str | None = None,
    ) -> None:
        with self._locked_jobs() as jobs:
            for index, job in enumerate(jobs):
                if job.get("id") != job_id:
                    continue
                updated = copy.deepcopy(job)
                now = utc_now_iso()
                updated["last_run_at"] = now
                updated["last_status"] = "ok" if success else "error"
                updated["last_error"] = None if success else error
                updated["last_delivery_error"] = delivery_error
                repeat = dict(updated.get("repeat") or {})
                repeat["completed"] = int(repeat.get("completed") or 0) + 1
                updated["repeat"] = repeat
                kind = (updated.get("schedule") or {}).get("kind")
                times = repeat.get("times")
                if times is not None and repeat["completed"] >= int(times):
                    updated["enabled"] = False
                    updated["state"] = "completed"
                    updated["next_run_at"] = None
                elif kind in {"interval", "cron"}:
                    updated["enabled"] = True
                    updated["state"] = "scheduled"
                    updated["next_run_at"] = compute_next_run(updated["schedule"], self.timezone_name, now)
                else:
                    updated["enabled"] = False
                    updated["state"] = "completed"
                    updated["next_run_at"] = None
                updated["updated_at"] = now
                jobs[index] = updated
                return

    def log_run(self, job_id: str, record: dict[str, Any]) -> None:
        directory = self.scheduled_runs_dir / safe_slug(job_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / SCHEDULE_OUTPUT_FILENAME
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"created_at": utc_now_iso(), **record}, ensure_ascii=False) + "\n")

    def origin_record(self, message: NormalizedMessage) -> dict[str, Any]:
        key = message.session_key
        return {
            "session_key": key.value,
            "realm_id": message.realm_id,
            "conversation_type": message.conversation_type,
            "stream_id": message.stream_id,
            "stream": message.stream,
            "stream_slug": message.stream_slug,
            "topic": message.topic,
            "topic_hash": message.topic_hash,
            "topic_slug": safe_slug(message.topic),
            "private_recipient_key": message.private_recipient_key,
            "private_recipients": message.private_recipients,
            "sender_email": message.sender_email,
            "sender_full_name": message.sender_full_name,
            "sender_id": message.sender_id,
        }

    def session_key_for_job(self, job: dict[str, Any]) -> SessionKey:
        origin = job.get("origin") if isinstance(job.get("origin"), dict) else {}
        return SessionKey(
            realm_id=str(origin.get("realm_id") or "unknown"),
            stream_id=_optional_int(origin.get("stream_id")),
            topic_hash=str(origin.get("topic_hash") or origin.get("private_recipient_key") or "unknown"),
            conversation_type=str(origin.get("conversation_type") or "stream"),
            private_recipient_key=(
                str(origin["private_recipient_key"]) if origin.get("private_recipient_key") is not None else None
            ),
            stream_slug=str(origin.get("stream_slug") or origin.get("stream") or "unknown"),
            topic_slug=str(origin.get("topic_slug") or origin.get("topic") or "unknown"),
        )

    def message_for_job(self, job: dict[str, Any]) -> NormalizedMessage:
        origin = job.get("origin") if isinstance(job.get("origin"), dict) else {}
        key = self.session_key_for_job(job)
        return NormalizedMessage(
            realm_id=key.realm_id,
            message_id=0,
            stream_id=key.stream_id,
            stream=str(origin.get("stream") or ("private" if key.conversation_type == "private" else "unknown")),
            stream_slug=str(origin.get("stream_slug") or key.stream_slug or "unknown"),
            topic=str(origin.get("topic") or ("private" if key.conversation_type == "private" else key.topic_hash)),
            topic_hash=key.topic_hash,
            sender_email=str(origin.get("sender_email") or ""),
            sender_full_name=str(origin.get("sender_full_name") or ""),
            sender_id=_optional_int(origin.get("sender_id")),
            content=str(job.get("prompt") or ""),
            timestamp=None,
            received_at=utc_now_iso(),
            raw={"scheduled_job_id": job.get("id")},
            conversation_type=key.conversation_type,
            private_recipient_key=key.private_recipient_key,
            private_recipients=[
                item for item in origin.get("private_recipients", []) if isinstance(item, dict)
            ] if isinstance(origin.get("private_recipients"), list) else [],
            post_required=True,
        )

    def _set_enabled(self, origin: NormalizedMessage, op: ScheduleOperation, *, enabled: bool, state: str) -> dict[str, Any]:
        with self._locked_jobs() as jobs:
            index, job = self._resolve_job(jobs, origin, op)
            if job is None or index is None:
                return self._rejected(op.action, "job not found", op)
            updated = copy.deepcopy(job)
            updated["enabled"] = enabled
            updated["state"] = state
            updated["updated_at"] = utc_now_iso()
            jobs[index] = updated
        return {
            "action": op.action,
            "status": "applied",
            "job": self._public_job(updated),
            "job_id": updated["id"],
            "name": updated["name"],
            "next_run_at": updated.get("next_run_at"),
        }

    def _resolve_job(
        self,
        jobs: list[dict[str, Any]],
        origin: NormalizedMessage,
        op: ScheduleOperation,
    ) -> tuple[int | None, dict[str, Any] | None]:
        if op.job_id.strip():
            for index, job in enumerate(jobs):
                if job.get("id") == op.job_id.strip():
                    return index, job
            return None, None

        candidates = [(index, job) for index, job in enumerate(jobs) if self._same_origin(job, origin)]
        needle = (op.match or op.name).strip().casefold()
        if needle:
            candidates = [
                (index, job)
                for index, job in candidates
                if needle in str(job.get("name") or "").casefold()
                or needle in str(job.get("prompt") or "").casefold()
            ]
        active = [(index, job) for index, job in candidates if job.get("enabled", True)]
        candidates = active or candidates
        if len(candidates) == 1:
            return candidates[0]
        return None, None

    def _same_origin(self, job: dict[str, Any], origin: NormalizedMessage) -> bool:
        job_origin = job.get("origin") if isinstance(job.get("origin"), dict) else {}
        return str(job_origin.get("session_key") or "") == origin.session_key.value

    def _validated_skills(self, skill_names: tuple[str, ...], skills: SkillLookup | None) -> list[str]:
        normalized: list[str] = []
        for name in skill_names:
            text = str(name).strip()
            if not text:
                continue
            if skills is None:
                value = text
            else:
                value = skills.validate_name(text)
                if not skills.skill_exists(value):
                    raise ValueError(f"skill not found: {value}")
            if value not in normalized:
                normalized.append(value)
        return normalized

    def _validated_mention_targets(
        self,
        requested: tuple[ScheduleMentionTarget, ...],
        mentionable_users: dict[int, str] | None,
        *,
        prompt: str,
    ) -> list[dict[str, Any]]:
        normalized: list[ScheduleMentionTarget] = []
        seen: set[tuple[str, int | None]] = set()
        prompt_text = prompt.strip()
        for target in requested:
            if target.kind == "person":
                if target.user_id is None:
                    raise ValueError("person mention target requires user_id")
                full_name = target.full_name.strip()
                if mentionable_users is not None:
                    full_name = mentionable_users.get(target.user_id, "")
                    if not full_name:
                        raise ValueError(f"mention target not found in conversation: {target.user_id}")
                if not full_name:
                    raise ValueError("person mention target requires full_name")
                normalized_target = ScheduleMentionTarget(
                    kind="person",
                    user_id=target.user_id,
                    full_name=full_name,
                    confidence=target.confidence,
                )
            else:
                literal = f"@**{target.kind}**"
                if literal not in prompt_text:
                    raise ValueError(f"broadcast mention target requires explicit {literal} in prompt")
                normalized_target = ScheduleMentionTarget(
                    kind=target.kind,
                    user_id=None,
                    full_name="",
                    confidence=target.confidence,
                )

            key = (normalized_target.kind, normalized_target.user_id)
            if key not in seen:
                seen.add(key)
                normalized.append(normalized_target)
        return [target.to_record() for target in normalized]

    def _public_job(self, job: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": job.get("id"),
            "name": job.get("name"),
            "prompt": job.get("prompt") or "",
            "schedule": job.get("schedule_display"),
            "schedule_detail": job.get("schedule") or {},
            "next_run_at": job.get("next_run_at"),
            "last_run_at": job.get("last_run_at"),
            "last_status": job.get("last_status"),
            "enabled": job.get("enabled", True),
            "state": job.get("state"),
            "skills": job.get("skills") or [],
            "mention_targets": job.get("mention_targets") or [],
        }

    def _rejected(
        self,
        action: str,
        reason: str,
        op: ScheduleOperation | None = None,
        *,
        name: str = "",
    ) -> dict[str, Any]:
        return {
            "action": action,
            "status": "rejected",
            "reason": reason,
            "job_id": op.job_id if op else None,
            "name": (op.name if op else name) or None,
        }

    def _grace_seconds(self, job: dict[str, Any]) -> int:
        schedule = job.get("schedule") or {}
        kind = schedule.get("kind")
        if kind == "interval":
            seconds = int(schedule.get("minutes") or 1) * 60
            return max(120, min(seconds // 2, 7200))
        return 7200 if kind == "cron" else ONESHOT_GRACE_SECONDS

    def _locked_jobs(self) -> "_JobsContext":
        return _JobsContext(self)


class _JobsContext:
    def __init__(self, store: ScheduleStore) -> None:
        self.store = store
        self.jobs: list[dict[str, Any]] = []
        self.lock_handle: Any = None

    def __enter__(self) -> list[dict[str, Any]]:
        self.store.ensure_dirs()
        _jobs_file_lock.acquire()
        self.lock_handle = self.store.lock_file.open("w", encoding="utf-8")
        if fcntl is not None:
            fcntl.flock(self.lock_handle, fcntl.LOCK_EX)
        self.jobs = self.store.load_jobs()
        return self.jobs

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        try:
            if exc_type is None:
                self.store.save_jobs(self.jobs)
        finally:
            if self.lock_handle is not None:
                if fcntl is not None:
                    fcntl.flock(self.lock_handle, fcntl.LOCK_UN)
                self.lock_handle.close()
            _jobs_file_lock.release()


def _parse_aware(value: str | None) -> datetime:
    if not value:
        return utc_now()
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _validate_cron_expr(expr: str, tz: ZoneInfo) -> None:
    _cron_next(expr, utc_now().astimezone(tz))


def _cron_next(expr: str, base: datetime) -> datetime:
    if croniter is not None:
        return croniter(expr, base).get_next(datetime)
    return _cron_next_fallback(expr, base)


def _cron_next_fallback(expr: str, base: datetime) -> datetime:
    fields = expr.split()
    if len(fields) not in {5, 6}:
        raise ValueError(f"invalid cron expression: {expr!r}")
    minute_set = _cron_field_values(fields[0], 0, 59)
    hour_set = _cron_field_values(fields[1], 0, 23)
    day_set = _cron_field_values(fields[2], 1, 31)
    month_set = _cron_field_values(fields[3], 1, 12)
    weekday_set = _cron_field_values(fields[4], 0, 6)
    candidate = (base + timedelta(minutes=1)).replace(second=0, microsecond=0)
    deadline = candidate + timedelta(days=366 * 5)
    while candidate <= deadline:
        cron_weekday = (candidate.weekday() + 1) % 7
        if (
            candidate.minute in minute_set
            and candidate.hour in hour_set
            and candidate.day in day_set
            and candidate.month in month_set
            and cron_weekday in weekday_set
        ):
            return candidate
        candidate += timedelta(minutes=1)
    raise ValueError(f"unable to compute next run for cron expression: {expr!r}")


def _cron_field_values(field: str, minimum: int, maximum: int) -> set[int]:
    values: set[int] = set()
    for part in field.split(","):
        if not part:
            raise ValueError(f"invalid cron field: {field!r}")
        step = 1
        if "/" in part:
            part, step_text = part.split("/", 1)
            step = int(step_text)
            if step <= 0:
                raise ValueError(f"invalid cron step: {field!r}")
        if part == "*":
            start, end = minimum, maximum
        elif "-" in part:
            start_text, end_text = part.split("-", 1)
            start, end = int(start_text), int(end_text)
        else:
            start = end = int(part)
        if start < minimum or end > maximum or start > end:
            raise ValueError(f"cron field out of range: {field!r}")
        values.update(range(start, end + 1, step))
    return values


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
