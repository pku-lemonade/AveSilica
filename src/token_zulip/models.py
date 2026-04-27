from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


REPLY_KINDS = {"chat", "draft_plan", "question", "report", "silent"}
MEMORY_OPS = {"add", "remove", "replace"}
MEMORY_SCOPES = {"channel", "conversation", "global"}
CONVERSATION_TYPES = {"stream", "private"}


DECISION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "should_reply",
        "reply_kind",
        "message_to_post",
        "memory_ops",
        "confidence",
    ],
    "properties": {
        "should_reply": {"type": "boolean"},
        "reply_kind": {"type": "string", "enum": sorted(REPLY_KINDS)},
        "message_to_post": {"type": "string"},
        "memory_ops": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["op", "scope", "content", "old_text"],
                "properties": {
                    "op": {"type": "string", "enum": sorted(MEMORY_OPS)},
                    "scope": {"type": "string", "enum": sorted(MEMORY_SCOPES)},
                    "content": {"type": "string"},
                    "old_text": {"type": "string"},
                },
            },
        },
        "confidence": {"type": "number", "minimum": 0, "maximum": 1},
    },
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_topic_name(topic: str) -> str:
    return re.sub(r"\s+", " ", topic.strip()).casefold()


def normalized_topic_hash(topic: str) -> str:
    normalized = normalize_topic_name(topic)
    return hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:16]


def private_user_key(sender_id: int | None, sender_email: str) -> str:
    if sender_id is not None:
        return str(sender_id)
    normalized_email = sender_email.strip().casefold()
    if normalized_email:
        return "email-" + hashlib.sha256(normalized_email.encode("utf-8")).hexdigest()[:16]
    return "unknown"


def safe_slug(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9._-]+", "-", value.strip().lower())
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug or "unnamed"


def stream_memory_dir_name(stream_id: int | None, stream_slug: str | None = None) -> str:
    if stream_id is None:
        raise ValueError("stream memory paths require stream_id")
    slug = safe_slug(stream_slug or "")
    if slug == "unnamed":
        raise ValueError("stream memory paths require stream_slug")
    return f"stream-{stream_id}-{slug}"


def topic_memory_dir_name(topic_hash: str) -> str:
    return f"topic-{safe_slug(topic_hash)}"


def private_memory_dir_name(user_key: str | None) -> str:
    return f"private-{safe_slug(user_key or 'unknown')}"


@dataclass(frozen=True)
class SessionKey:
    realm_id: str
    stream_id: int | None
    topic_hash: str
    conversation_type: str = "stream"
    private_user_key: str | None = None
    stream_slug: str | None = None

    @property
    def value(self) -> str:
        if self.conversation_type == "private":
            user_key = self.private_user_key or self.topic_hash or "unknown"
            return f"zulip:{self.realm_id}:private:user:{user_key}"
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
    private_user_key: str | None = None
    reply_required: bool = False
    directly_addressed: bool = False

    @property
    def session_key(self) -> SessionKey:
        return SessionKey(
            realm_id=self.realm_id,
            stream_id=self.stream_id,
            topic_hash=self.topic_hash,
            conversation_type=self.conversation_type,
            private_user_key=self.private_user_key,
            stream_slug=self.stream_slug,
        )

    def to_record(self) -> dict[str, Any]:
        return {
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


@dataclass(frozen=True)
class MemoryOperation:
    op: str
    scope: str = "conversation"
    content: str = ""
    old_text: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "MemoryOperation":
        op = str(value.get("op") or "")
        scope = str(value.get("scope") or "conversation")
        content = str(value.get("content") or "")
        old_text = str(value.get("old_text") or "")
        if op not in MEMORY_OPS:
            raise ValueError(f"invalid memory op: {op!r}")
        if scope not in MEMORY_SCOPES:
            raise ValueError(f"invalid memory scope: {scope!r}")
        if op in {"add", "replace"} and not content.strip():
            raise ValueError(f"{op} memory op requires content")
        if op in {"replace", "remove"} and not old_text.strip():
            raise ValueError(f"{op} memory op requires old_text")
        return cls(
            op=op,
            scope=scope,
            content=content,
            old_text=old_text,
        )

    def to_record(self) -> dict[str, str]:
        return {
            "op": self.op,
            "scope": self.scope,
            "content": self.content,
            "old_text": self.old_text,
        }


@dataclass(frozen=True)
class AgentDecision:
    should_reply: bool
    reply_kind: str
    message_to_post: str
    memory_ops: list[MemoryOperation] = field(default_factory=list)
    confidence: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)

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

        reply_kind = str(data.get("reply_kind") or "silent")
        if reply_kind not in REPLY_KINDS:
            raise ValueError(f"invalid reply_kind: {reply_kind!r}")

        confidence = float(data.get("confidence") or 0.0)
        confidence = max(0.0, min(1.0, confidence))

        memory_ops = [
            MemoryOperation.from_mapping(item)
            for item in data.get("memory_ops", [])
            if isinstance(item, dict)
        ]

        should_reply = bool(data.get("should_reply"))
        message_to_post = str(data.get("message_to_post") or "")
        if reply_kind == "silent":
            should_reply = False
            message_to_post = ""

        return cls(
            should_reply=should_reply,
            reply_kind=reply_kind,
            message_to_post=message_to_post,
            memory_ops=memory_ops,
            confidence=confidence,
            raw=data,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "should_reply": self.should_reply,
            "reply_kind": self.reply_kind,
            "message_to_post": self.message_to_post,
            "memory_ops": [item.to_record() for item in self.memory_ops],
            "confidence": self.confidence,
        }


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
