from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from slugify import slugify


REPLY_KINDS = {"chat", "draft_plan", "question", "report", "silent"}
REFLECTION_SCOPES = {"global", "source"}
SCHEDULE_OPS = {"create", "update", "remove", "pause", "resume", "list", "run_now"}
SCHEDULE_SPEC_KINDS = {"unchanged", "once_at", "once_in", "interval", "cron"}
SCHEDULE_MENTION_TARGET_KINDS = {"person", "topic", "channel", "all"}
SKILL_OPS = {"create", "update", "remove"}
CONVERSATION_TYPES = {"stream", "private"}
TOPIC_HASH_LENGTH = 6


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_topic_name(topic: str) -> str:
    return re.sub(r"\s+", " ", topic.strip()).casefold()


def normalized_topic_hash(topic: str) -> str:
    normalized = normalize_topic_name(topic)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:TOPIC_HASH_LENGTH]


def safe_slug(value: str) -> str:
    slug = slugify(value, allow_unicode=True, regex_pattern=r"[^\w.\-]+")
    return slug or "unnamed"


def stream_scope_dir_name(stream_id: int | None, stream_slug: str | None = None) -> str:
    if stream_id is None:
        raise ValueError("stream-scoped paths require stream_id")
    slug = safe_slug(stream_slug or "")
    if slug == "unnamed":
        raise ValueError("stream-scoped paths require stream_slug")
    return f"stream-{slug}-{stream_id}"


def topic_dir_name(topic_hash: str, topic_slug: str | None) -> str:
    return f"topic-{safe_slug(topic_slug or '')}-{safe_slug(topic_hash)}"


def topic_scope_dir_name(topic_hash: str, topic_slug: str | None = None) -> str:
    return topic_dir_name(topic_hash, topic_slug)


def private_scope_dir_name(recipient_key: str | None) -> str:
    return f"private-recipient-{safe_slug(recipient_key or 'unknown')}"


def topic_record_dir_name(topic_hash: str, topic_slug: str | None = None) -> str:
    return topic_dir_name(topic_hash, topic_slug)


def scoped_stream_dir(root: Path, key: "SessionKey") -> Path:
    return root / stream_scope_dir_name(key.stream_id, key.stream_slug)


def scoped_private_dir(root: Path, key: "SessionKey") -> Path:
    return root / private_scope_dir_name(key.private_recipient_key or key.topic_hash)


def scoped_conversation_dir(root: Path, key: "SessionKey", *, readable_topic: bool = False) -> Path:
    if key.conversation_type == "private":
        return scoped_private_dir(root, key)
    return scoped_stream_dir(root, key) / topic_dir_name(key.topic_hash, key.topic_slug)


@dataclass(frozen=True)
class SessionKey:
    realm_id: str
    stream_id: int | None
    topic_hash: str
    conversation_type: str = "stream"
    private_recipient_key: str | None = None
    stream_slug: str | None = None
    topic_slug: str | None = None

    @property
    def value(self) -> str:
        if self.conversation_type == "private":
            recipient_key = self.private_recipient_key or self.topic_hash or "unknown"
            return f"zulip:{self.realm_id}:private:recipient:{recipient_key}"
        return f"zulip:{self.realm_id}:stream:{self.stream_id}:topic:{self.topic_hash}"

    @property
    def storage_id(self) -> str:
        return hashlib.sha256(self.value.encode("utf-8")).hexdigest()[:20]


@dataclass(frozen=True)
class NormalizedMessage:
    realm_id: str
    message_id: int
    stream_id: int | None
    stream: str
    stream_slug: str
    topic: str
    topic_hash: str
    sender_email: str
    sender_full_name: str
    sender_id: int | None
    content: str
    timestamp: int | None
    received_at: str
    raw: dict[str, Any]
    conversation_type: str = "stream"
    private_recipient_key: str | None = None
    private_recipients: list[dict[str, Any]] = field(default_factory=list)
    reply_required: bool = False
    directly_addressed: bool = False
    uploads: list[dict[str, Any]] = field(default_factory=list)
    reactions: list[dict[str, Any]] = field(default_factory=list)
    reaction_events: list[dict[str, Any]] = field(default_factory=list)

    @property
    def session_key(self) -> SessionKey:
        return SessionKey(
            realm_id=self.realm_id,
            stream_id=self.stream_id,
            topic_hash=self.topic_hash,
            conversation_type=self.conversation_type,
            private_recipient_key=self.private_recipient_key,
            stream_slug=self.stream_slug,
            topic_slug=safe_slug(self.topic),
        )

    def to_record(self) -> dict[str, Any]:
        record: dict[str, Any] = {
            "message_id": self.message_id,
            "reply_required": self.reply_required,
            "sender_email": self.sender_email,
            "sender_full_name": self.sender_full_name,
            "sender_id": self.sender_id,
            "content": self.content,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
            "directly_addressed": self.directly_addressed,
        }
        if self.private_recipients:
            record["private_recipients"] = self.private_recipients
        if self.uploads:
            record["uploads"] = self.uploads
        if self.reactions:
            record["reactions"] = self.reactions
        if self.reaction_events:
            record["reaction_events"] = self.reaction_events
        return record


@dataclass(frozen=True)
class NormalizedReaction:
    realm_id: str
    message_id: int
    op: str
    emoji_name: str
    emoji_code: str
    reaction_type: str
    user_id: int | None
    user_email: str
    user_full_name: str
    timestamp: int | None
    received_at: str
    raw: dict[str, Any]

    @property
    def user_key(self) -> str:
        if self.user_id is not None:
            return str(self.user_id)
        email = self.user_email.strip().casefold()
        return email or "unknown"

    @property
    def active_key(self) -> tuple[str, str]:
        return (self.user_key, self.emoji_name)

    def to_active_record(self) -> dict[str, Any]:
        return {
            "user_key": self.user_key,
            "user_id": self.user_id,
            "user_email": self.user_email,
            "user_full_name": self.user_full_name,
            "emoji_name": self.emoji_name,
            "emoji_code": self.emoji_code,
            "reaction_type": self.reaction_type,
            "received_at": self.received_at,
        }

    def to_event_record(self) -> dict[str, Any]:
        return {
            "op": self.op,
            "message_id": self.message_id,
            "user_key": self.user_key,
            "user_id": self.user_id,
            "user_email": self.user_email,
            "user_full_name": self.user_full_name,
            "emoji_name": self.emoji_name,
            "emoji_code": self.emoji_code,
            "reaction_type": self.reaction_type,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
        }


@dataclass(frozen=True)
class NormalizedMessageMove:
    realm_id: str
    message_id: int
    message_ids: list[int]
    stream_id: int
    stream_name: str
    orig_subject: str
    new_stream_id: int
    subject: str
    propagate_mode: str
    raw: dict[str, Any]

    @property
    def source_topic_hash(self) -> str:
        return normalized_topic_hash(self.orig_subject)

    @property
    def destination_topic_hash(self) -> str:
        return normalized_topic_hash(self.subject)

    @property
    def source_key(self) -> SessionKey:
        return SessionKey(
            realm_id=self.realm_id,
            stream_id=self.stream_id,
            topic_hash=self.source_topic_hash,
            stream_slug=safe_slug(self.stream_name),
            topic_slug=safe_slug(self.orig_subject),
        )

    @property
    def destination_key(self) -> SessionKey:
        stream_slug = safe_slug(self.stream_name) if self.new_stream_id == self.stream_id else None
        return SessionKey(
            realm_id=self.realm_id,
            stream_id=self.new_stream_id,
            topic_hash=self.destination_topic_hash,
            stream_slug=stream_slug,
            topic_slug=safe_slug(self.subject),
        )


@dataclass(frozen=True)
class ReflectionOperation:
    scope: str = "source"
    kind: str = "observation"
    suggested_target: str = "none"
    content: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ReflectionOperation":
        scope = str(value.get("scope") or "source").strip().lower()
        kind = str(value.get("kind") or "observation").strip()
        suggested_target = str(value.get("suggested_target") or "none").strip()
        content = str(value.get("content") or "").strip()
        if scope not in REFLECTION_SCOPES:
            raise ValueError(f"invalid reflection scope: {scope!r}")
        if not content:
            raise ValueError("reflection op requires content")
        return cls(
            scope=scope,
            kind=kind or "observation",
            suggested_target=suggested_target or "none",
            content=content,
        )

    def to_record(self) -> dict[str, str]:
        return {
            "scope": self.scope,
            "kind": self.kind,
            "suggested_target": self.suggested_target,
            "content": self.content,
        }


@dataclass(frozen=True)
class SkillOperation:
    action: str
    name: str = ""
    description: str = ""
    content: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "SkillOperation":
        action = str(value.get("action") or "").strip().lower()
        if action not in SKILL_OPS:
            raise ValueError(f"invalid skill op: {action!r}")
        return cls(
            action=action,
            name=str(value.get("name") or ""),
            description=str(value.get("description") or ""),
            content=str(value.get("content") or ""),
        )

    def to_record(self) -> dict[str, str]:
        return {
            "action": self.action,
            "name": self.name,
            "description": self.description,
            "content": self.content,
        }


@dataclass(frozen=True)
class ScheduleSpec:
    kind: str = "unchanged"
    run_at: str = ""
    duration: str = ""
    cron: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> "ScheduleSpec":
        if value is None:
            return cls()
        if not isinstance(value, dict):
            raise ValueError("schedule_spec must be an object")
        kind = str(value.get("kind") or "unchanged").strip().lower()
        if kind not in SCHEDULE_SPEC_KINDS:
            raise ValueError(f"invalid schedule_spec kind: {kind!r}")
        return cls(
            kind=kind,
            run_at=str(value.get("run_at") or ""),
            duration=str(value.get("duration") or ""),
            cron=str(value.get("cron") or ""),
        )

    def has_schedule(self) -> bool:
        return self.kind != "unchanged"

    def to_record(self) -> dict[str, str]:
        return {
            "kind": self.kind,
            "run_at": self.run_at,
            "duration": self.duration,
            "cron": self.cron,
        }


@dataclass(frozen=True)
class ScheduleMentionTarget:
    kind: str
    user_id: int | None = None
    full_name: str = ""
    confidence: float = 0.0

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ScheduleMentionTarget":
        if not isinstance(value, dict):
            raise ValueError("mention target must be an object")
        kind = str(value.get("kind") or "").strip().lower()
        if kind not in SCHEDULE_MENTION_TARGET_KINDS:
            raise ValueError(f"invalid mention target kind: {kind!r}")

        raw_user_id = value.get("user_id")
        user_id: int | None = None
        if raw_user_id is not None:
            try:
                user_id = int(raw_user_id)
            except (TypeError, ValueError) as exc:
                raise ValueError("mention target user_id must be an integer or null") from exc

        full_name = str(value.get("full_name") or "").strip()
        if kind == "person":
            if user_id is None:
                raise ValueError("person mention target requires user_id")
            if not full_name:
                raise ValueError("person mention target requires full_name")

        confidence = float(value.get("confidence") or 0.0)
        confidence = max(0.0, min(1.0, confidence))
        return cls(
            kind=kind,
            user_id=user_id,
            full_name=full_name,
            confidence=confidence,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "user_id": self.user_id,
            "full_name": self.full_name,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class ScheduleOperation:
    action: str
    job_id: str = ""
    name: str = ""
    match: str = ""
    prompt: str = ""
    schedule: str = ""
    schedule_spec: ScheduleSpec = field(default_factory=ScheduleSpec)
    repeat: int | None = None
    skills: tuple[str, ...] = ()
    mention_targets: tuple[ScheduleMentionTarget, ...] = ()
    confidence: float = 0.0

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ScheduleOperation":
        action = str(value.get("action") or "").strip().lower()
        if action == "modify":
            action = "update"
        if action not in SCHEDULE_OPS:
            raise ValueError(f"invalid schedule op: {action!r}")

        raw_skills = value.get("skills") or []
        if isinstance(raw_skills, str):
            skills = (raw_skills,)
        elif isinstance(raw_skills, list):
            skills = tuple(str(item) for item in raw_skills if str(item).strip())
        else:
            skills = ()

        raw_mention_targets = value.get("mention_targets") or []
        if not isinstance(raw_mention_targets, list):
            raise ValueError("mention_targets must be an array")
        mention_targets = tuple(ScheduleMentionTarget.from_mapping(item) for item in raw_mention_targets)

        repeat = value.get("repeat")
        if repeat is not None:
            try:
                repeat = int(repeat)
            except (TypeError, ValueError) as exc:
                raise ValueError("schedule repeat must be an integer or null") from exc
            if repeat <= 0:
                repeat = None

        confidence = float(value.get("confidence") or 0.0)
        confidence = max(0.0, min(1.0, confidence))

        return cls(
            action=action,
            job_id=str(value.get("job_id") or ""),
            name=str(value.get("name") or ""),
            match=str(value.get("match") or ""),
            prompt=str(value.get("prompt") or ""),
            schedule=str(value.get("schedule") or ""),
            schedule_spec=ScheduleSpec.from_mapping(value.get("schedule_spec")),
            repeat=repeat,
            skills=skills,
            mention_targets=mention_targets,
            confidence=confidence,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "action": self.action,
            "job_id": self.job_id,
            "name": self.name,
            "match": self.match,
            "prompt": self.prompt,
            "schedule": self.schedule,
            "schedule_spec": self.schedule_spec.to_record(),
            "repeat": self.repeat,
            "skills": list(self.skills),
            "mention_targets": [target.to_record() for target in self.mention_targets],
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class ReplyDecision:
    should_reply: bool
    reply_kind: str
    message_to_post: str
    confidence: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def silent(cls, raw: dict[str, Any] | None = None) -> "ReplyDecision":
        return cls(
            should_reply=False,
            reply_kind="silent",
            message_to_post="",
            confidence=0.0,
            raw=raw or {},
        )

    @classmethod
    def from_json_text(cls, text: str) -> "ReplyDecision":
        payload = _extract_json_object(text)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("decision JSON must be an object")

        reply_kind = str(data.get("reply_kind") or "silent")
        if reply_kind not in REPLY_KINDS:
            raise ValueError(f"invalid reply_kind: {reply_kind!r}")

        confidence = float(data.get("confidence") or 0.0)
        confidence = max(0.0, min(1.0, confidence))

        should_reply = bool(data.get("should_reply"))
        message_to_post = str(data.get("message_to_post") or "")
        if reply_kind == "silent":
            should_reply = False
            message_to_post = ""

        return cls(
            should_reply=should_reply,
            reply_kind=reply_kind,
            message_to_post=message_to_post,
            confidence=confidence,
            raw=data,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "should_reply": self.should_reply,
            "reply_kind": self.reply_kind,
            "message_to_post": self.message_to_post,
            "confidence": self.confidence,
        }


@dataclass(frozen=True)
class ReflectionDecision:
    reflection_ops: list[ReflectionOperation] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json_text(cls, text: str) -> "ReflectionDecision":
        payload = _extract_json_object(text)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("reflection decision JSON must be an object")
        ops = [
            ReflectionOperation.from_mapping(item)
            for item in data.get("reflection_ops", [])
            if isinstance(item, dict)
        ]
        return cls(reflection_ops=ops, raw=data)


@dataclass(frozen=True)
class SkillDecision:
    skill_ops: list[SkillOperation] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json_text(cls, text: str) -> "SkillDecision":
        payload = _extract_json_object(text)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("skill decision JSON must be an object")
        ops = [
            SkillOperation.from_mapping(item)
            for item in data.get("skill_ops", [])
            if isinstance(item, dict)
        ]
        return cls(skill_ops=ops, raw=data)


@dataclass(frozen=True)
class ScheduleDecision:
    schedule_ops: list[ScheduleOperation] = field(default_factory=list)
    raw: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def from_json_text(cls, text: str) -> "ScheduleDecision":
        payload = _extract_json_object(text)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("schedule decision JSON must be an object")
        ops = [
            ScheduleOperation.from_mapping(item)
            for item in data.get("schedule_ops", [])
            if isinstance(item, dict)
        ]
        return cls(schedule_ops=ops, raw=data)


@dataclass(frozen=True)
class AgentDecision(ReplyDecision):
    schedule_ops: list[ScheduleOperation] = field(default_factory=list)
    skill_ops: list[SkillOperation] = field(default_factory=list)

    @classmethod
    def silent(cls, raw: dict[str, Any] | None = None) -> "AgentDecision":
        return cls(
            should_reply=False,
            reply_kind="silent",
            message_to_post="",
            confidence=0.0,
            raw=raw or {},
        )

    @classmethod
    def from_json_text(cls, text: str) -> "AgentDecision":
        payload = _extract_json_object(text)
        data = json.loads(payload)
        if not isinstance(data, dict):
            raise ValueError("decision JSON must be an object")

        reply = ReplyDecision.from_json_text(payload)
        schedule = ScheduleDecision.from_json_text(payload)
        skill = SkillDecision.from_json_text(payload)
        return cls(
            should_reply=reply.should_reply,
            reply_kind=reply.reply_kind,
            message_to_post=reply.message_to_post,
            schedule_ops=schedule.schedule_ops,
            skill_ops=skill.skill_ops,
            confidence=reply.confidence,
            raw=data,
        )

    @classmethod
    def from_parts(
        cls,
        reply: ReplyDecision,
        *,
        schedule_ops: list[ScheduleOperation] | None = None,
        skill_ops: list[SkillOperation] | None = None,
    ) -> "AgentDecision":
        return cls(
            should_reply=reply.should_reply,
            reply_kind=reply.reply_kind,
            message_to_post=reply.message_to_post,
            schedule_ops=schedule_ops or [],
            skill_ops=skill_ops or [],
            confidence=reply.confidence,
            raw=reply.raw,
        )

    def to_record(self) -> dict[str, Any]:
        record = super().to_record()
        record["schedule_ops"] = [item.to_record() for item in self.schedule_ops]
        record["skill_ops"] = [item.to_record() for item in self.skill_ops]
        return record


def _extract_json_object(text: str) -> str:
    stripped = text.strip()
    if stripped.startswith("{") and stripped.endswith("}"):
        return stripped

    fence_match = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", stripped, re.DOTALL)
    if fence_match:
        return fence_match.group(1)

    start = stripped.find("{")
    if start < 0:
        raise ValueError("no JSON object found in model response")

    depth = 0
    in_string = False
    escaped = False
    for idx in range(start, len(stripped)):
        char = stripped[idx]
        if in_string:
            if escaped:
                escaped = False
            elif char == "\\":
                escaped = True
            elif char == '"':
                in_string = False
            continue
        if char == '"':
            in_string = True
        elif char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return stripped[start : idx + 1]

    raise ValueError("unterminated JSON object in model response")
