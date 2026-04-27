from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


REPLY_KINDS = {"chat", "draft_plan", "question", "report", "silent"}
MEMORY_OPS = {"archive", "upsert"}
MEMORY_KINDS = {"decision", "fact", "person", "preference", "question", "task"}
MEMORY_SCOPES = {"channel", "conversation", "global"}
MEMORY_STATUSES = {"active", "answered", "archived", "done"}
SCRATCHPAD_OPS = {"clear", "none", "replace"}
CONVERSATION_TYPES = {"stream", "private"}


DECISION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "should_reply",
        "reply_kind",
        "message_to_post",
        "memory_ops",
        "scratchpad_op",
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
                "required": ["op", "id", "scope", "kind", "status", "content"],
                "properties": {
                    "op": {"type": "string", "enum": sorted(MEMORY_OPS)},
                    "id": {"type": ["string", "null"]},
                    "scope": {"type": "string", "enum": sorted(MEMORY_SCOPES)},
                    "kind": {"type": "string", "enum": sorted(MEMORY_KINDS)},
                    "status": {"type": "string", "enum": sorted(MEMORY_STATUSES)},
                    "content": {"type": "string"},
                },
            },
        },
        "scratchpad_op": {
            "type": "object",
            "additionalProperties": False,
            "required": ["op", "content"],
            "properties": {
                "op": {"type": "string", "enum": sorted(SCRATCHPAD_OPS)},
                "content": {"type": "string"},
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


def normalize_memory_content(content: str) -> str:
    return re.sub(r"\s+", " ", content.strip()).casefold()


@dataclass(frozen=True)
class MemoryOperation:
    op: str
    scope: str = "conversation"
    kind: str = "fact"
    content: str = ""
    id: str | None = None
    status: str = "active"

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "MemoryOperation":
        op = str(value.get("op") or "")
        scope = str(value.get("scope") or "conversation")
        kind = str(value.get("kind") or "fact")
        status = str(value.get("status") or "active")
        content = str(value.get("content") or "")
        item_id = value.get("id")
        if op not in MEMORY_OPS:
            raise ValueError(f"invalid memory op: {op!r}")
        if scope not in MEMORY_SCOPES:
            raise ValueError(f"invalid memory scope: {scope!r}")
        if kind not in MEMORY_KINDS:
            raise ValueError(f"invalid memory kind: {kind!r}")
        if status not in MEMORY_STATUSES:
            raise ValueError(f"invalid memory status: {status!r}")
        if op == "archive" and not str(item_id or "").strip():
            raise ValueError("archive memory op requires id")
        if op == "upsert" and not content.strip():
            raise ValueError("upsert memory op requires content")
        return cls(
            op=op,
            scope=scope,
            kind=kind,
            content=content,
            id=str(item_id).strip() if item_id is not None else None,
            status=status,
        )

    def to_record(self) -> dict[str, str]:
        record = {
            "op": self.op,
            "scope": self.scope,
            "kind": self.kind,
            "status": self.status,
            "content": self.content,
        }
        if self.id:
            record["id"] = self.id
        return record


@dataclass(frozen=True)
class ScratchpadOperation:
    op: str = "none"
    content: str = ""

    @classmethod
    def from_mapping(cls, value: dict[str, Any] | None) -> "ScratchpadOperation":
        if value is None:
            return cls()
        op = str(value.get("op") or "none")
        content = str(value.get("content") or "")
        if op not in SCRATCHPAD_OPS:
            raise ValueError(f"invalid scratchpad op: {op!r}")
        return cls(op=op, content=content)

    def to_record(self) -> dict[str, str]:
        return {"op": self.op, "content": self.content}


@dataclass(frozen=True)
class MemoryItem:
    id: str
    scope: str
    kind: str
    status: str
    content: str
    source_session_id: str
    source_message_ids: list[int]
    created_at: str
    updated_at: str

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "MemoryItem":
        source_message_ids = [
            int(value)
            for value in record.get("source_message_ids", [])
            if isinstance(value, int | str) and str(value).strip()
        ]
        return cls(
            id=str(record["id"]),
            scope=str(record["scope"]),
            kind=str(record["kind"]),
            status=str(record["status"]),
            content=str(record["content"]),
            source_session_id=str(record.get("source_session_id") or ""),
            source_message_ids=source_message_ids,
            created_at=str(record.get("created_at") or utc_now_iso()),
            updated_at=str(record.get("updated_at") or utc_now_iso()),
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "scope": self.scope,
            "kind": self.kind,
            "status": self.status,
            "content": self.content,
            "source_session_id": self.source_session_id,
            "source_message_ids": self.source_message_ids,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }


@dataclass(frozen=True)
class AgentDecision:
    should_reply: bool
    reply_kind: str
    message_to_post: str
    memory_ops: list[MemoryOperation] = field(default_factory=list)
    scratchpad_op: ScratchpadOperation = field(default_factory=ScratchpadOperation)
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
        scratchpad_op = ScratchpadOperation.from_mapping(data.get("scratchpad_op"))

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
            scratchpad_op=scratchpad_op,
            confidence=confidence,
            raw=data,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "should_reply": self.should_reply,
            "reply_kind": self.reply_kind,
            "message_to_post": self.message_to_post,
            "memory_ops": [item.to_record() for item in self.memory_ops],
            "scratchpad_op": self.scratchpad_op.to_record(),
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
