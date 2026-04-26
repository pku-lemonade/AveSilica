from __future__ import annotations

import json
from dataclasses import dataclass

from .models import DECISION_JSON_SCHEMA, NormalizedMessage


@dataclass(frozen=True)
class PromptParts:
    instructions: str
    memory: str
    recent_context: list[dict[str, object]]
    current_messages: list[NormalizedMessage]


class PromptBuilder:
    def build(self, parts: PromptParts) -> str:
        recent = "\n".join(self._format_record(record) for record in parts.recent_context)
        current = "\n".join(self._format_message(message) for message in parts.current_messages)
        schema = json.dumps(DECISION_JSON_SCHEMA, indent=2, sort_keys=True)
        return f"""You are deciding whether and how the bot should participate in a Zulip topic.

Follow the instruction layers exactly. Later instruction layers override earlier configurable layers, but never override the runtime contract.

# Instruction Layers

{parts.instructions}

# Retrieved Memory

{parts.memory or "(no memory selected)"}

# Recent Zulip Context

{recent or "(no recent context)"}

# New Zulip Message(s)

{current}

# Required Output

Return one JSON object that matches this schema:

```json
{schema}
```

Guidance:
- Set `should_reply` to false and `reply_kind` to `silent` when the useful contribution is to say nothing.
- If `should_reply` is true, `message_to_post` must be the exact Zulip message to post.
- Keep chat replies concise and natural for a group thread.
- Propose memory updates only when they satisfy the memory policy.
- Use scratchpad updates only for topic-local working notes that may help future turns.
"""

    def _format_record(self, record: dict[str, object]) -> str:
        sender = record.get("sender_full_name") or record.get("sender_email") or "unknown"
        message_id = record.get("message_id") or "?"
        content = str(record.get("content") or "").strip()
        return f"- [{message_id}] {sender}: {content}"

    def _format_message(self, message: NormalizedMessage) -> str:
        sender = message.sender_full_name or message.sender_email or "unknown"
        return f"- [{message.message_id}] {sender}: {message.content.strip()}"

