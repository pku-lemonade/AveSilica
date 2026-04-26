from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any


REPLY_KINDS = {"chat", "draft_plan", "question", "report", "silent"}
UPDATE_MODES = {"append", "replace"}
MEMORY_FILES = {"durable.md", "open_questions.md", "tasks.md", "people.md"}
CONVERSATION_TYPES = {"stream", "private"}


DECISION_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": False,
    "required": [
        "should_reply",
        "reply_kind",
        "message_to_post",
        "memory_updates",
        "scratchpad_updates",
        "confidence",
    ],
    "properties": {
        "should_reply": {"type": "boolean"},
        "reply_kind": {"type": "string", "enum": sorted(REPLY_KINDS)},
        "message_to_post": {"type": "string"},
        "memory_updates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["file", "mode", "content"],
                "properties": {
                    "file": {"type": "string", "enum": sorted(MEMORY_FILES)},
                    "mode": {"type": "string", "enum": sorted(UPDATE_MODES)},
                    "content": {"type": "string"},
                },
            },
        },
        "scratchpad_updates": {
            "type": "array",
            "items": {
                "type": "object",
                "additionalProperties": False,
                "required": ["mode", "content"],
                "properties": {
                    "mode": {"type": "string", "enum": sorted(UPDATE_MODES)},
                    "content": {"type": "string"},
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


@dataclass(frozen=True)
class SessionKey:
    realm_id: str
    stream_id: int | None
    topic_hash: str
    conversation_type: str = "stream"
    private_user_key: str | None = None

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

    @property
    def session_key(self) -> SessionKey:
        return SessionKey(
            realm_id=self.realm_id,
            stream_id=self.stream_id,
            topic_hash=self.topic_hash,
            conversation_type=self.conversation_type,
            private_user_key=self.private_user_key,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "realm_id": self.realm_id,
            "message_id": self.message_id,
            "stream_id": self.stream_id,
            "stream": self.stream,
            "stream_slug": self.stream_slug,
            "topic": self.topic,
            "topic_hash": self.topic_hash,
            "conversation_type": self.conversation_type,
            "private_user_key": self.private_user_key,
            "reply_required": self.reply_required,
            "sender_email": self.sender_email,
            "sender_full_name": self.sender_full_name,
            "sender_id": self.sender_id,
            "content": self.content,
            "timestamp": self.timestamp,
            "received_at": self.received_at,
        }

    @classmethod
    def from_record(cls, record: dict[str, Any]) -> "NormalizedMessage":
        stream_id = record.get("stream_id")
        sender_id = record.get("sender_id")
        conversation_type = str(record.get("conversation_type") or "stream")
        private_key = record.get("private_user_key")
        return cls(
            realm_id=str(record["realm_id"]),
            message_id=int(record["message_id"]),
            stream_id=int(stream_id) if stream_id is not None else None,
            stream=str(record["stream"]),
            stream_slug=str(record.get("stream_slug") or safe_slug(str(record["stream"]))),
            topic=str(record["topic"]),
            topic_hash=str(record["topic_hash"]),
            conversation_type=conversation_type,
            private_user_key=str(private_key) if private_key is not None else None,
            reply_required=bool(record.get("reply_required") or conversation_type == "private"),
            sender_email=str(record.get("sender_email") or ""),
            sender_full_name=str(record.get("sender_full_name") or ""),
            sender_id=int(sender_id) if sender_id is not None else None,
            content=str(record.get("content") or ""),
            timestamp=record.get("timestamp"),
            received_at=str(record.get("received_at") or utc_now_iso()),
            raw=dict(record.get("raw") or {}),
        )


@dataclass(frozen=True)
class MemoryUpdate:
    file: str
    mode: str
    content: str

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "MemoryUpdate":
        file_name = str(value.get("file") or "")
        mode = str(value.get("mode") or "")
        content = str(value.get("content") or "")
        if file_name not in MEMORY_FILES:
            raise ValueError(f"invalid memory file: {file_name!r}")
        if mode not in UPDATE_MODES:
            raise ValueError(f"invalid memory update mode: {mode!r}")
        return cls(file=file_name, mode=mode, content=content)

    def to_record(self) -> dict[str, str]:
        return {"file": self.file, "mode": self.mode, "content": self.content}


@dataclass(frozen=True)
class ScratchpadUpdate:
    mode: str
    content: str

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> "ScratchpadUpdate":
        mode = str(value.get("mode") or "")
        content = str(value.get("content") or "")
        if mode not in UPDATE_MODES:
            raise ValueError(f"invalid scratchpad update mode: {mode!r}")
        return cls(mode=mode, content=content)

    def to_record(self) -> dict[str, str]:
        return {"mode": self.mode, "content": self.content}


@dataclass(frozen=True)
class AgentDecision:
    should_reply: bool
    reply_kind: str
    message_to_post: str
    memory_updates: list[MemoryUpdate] = field(default_factory=list)
    scratchpad_updates: list[ScratchpadUpdate] = field(default_factory=list)
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

        memory_updates = [
            MemoryUpdate.from_mapping(item)
            for item in data.get("memory_updates", [])
            if isinstance(item, dict)
        ]
        scratchpad_updates = [
            ScratchpadUpdate.from_mapping(item)
            for item in data.get("scratchpad_updates", [])
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
            memory_updates=memory_updates,
            scratchpad_updates=scratchpad_updates,
            confidence=confidence,
            raw=data,
        )

    def to_record(self) -> dict[str, Any]:
        return {
            "should_reply": self.should_reply,
            "reply_kind": self.reply_kind,
            "message_to_post": self.message_to_post,
            "memory_updates": [item.to_record() for item in self.memory_updates],
            "scratchpad_updates": [item.to_record() for item in self.scratchpad_updates],
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
