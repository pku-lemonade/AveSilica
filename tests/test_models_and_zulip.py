from __future__ import annotations

import json

from token_zulip.models import AgentDecision, normalized_topic_hash
from token_zulip.zulip_io import normalize_zulip_event


def test_normalize_zulip_stream_event_strips_html_and_hashes_topic():
    event = {
        "type": "message",
        "message": {
            "id": 42,
            "type": "stream",
            "stream_id": 7,
            "display_recipient": "Engineering",
            "subject": " Launch   Plan ",
            "sender_email": "alice@example.com",
            "sender_full_name": "Alice",
            "sender_id": 3,
            "content": "<p>Hello<br>world</p>",
            "timestamp": 1710000000,
        },
    }

    message = normalize_zulip_event(event, "realm")

    assert message is not None
    assert message.session_key.value == f"zulip:realm:stream:7:topic:{normalized_topic_hash(' Launch   Plan ')}"
    assert message.stream_slug == "engineering"
    assert message.content == "Hello\nworld"


def test_agent_decision_parses_fenced_json_and_validates_memory_updates():
    payload = {
        "should_reply": True,
        "reply_kind": "chat",
        "message_to_post": "Done.",
        "memory_updates": [{"file": "tasks.md", "mode": "append", "content": "- Follow up"}],
        "scratchpad_updates": [{"mode": "replace", "content": "notes"}],
        "confidence": 2,
    }

    decision = AgentDecision.from_json_text(f"```json\n{json.dumps(payload)}\n```")

    assert decision.should_reply is True
    assert decision.confidence == 1.0
    assert decision.memory_updates[0].file == "tasks.md"
    assert decision.scratchpad_updates[0].mode == "replace"

