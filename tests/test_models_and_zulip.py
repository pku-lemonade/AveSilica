from __future__ import annotations

import asyncio
import json

from token_zulip.models import AgentDecision, NormalizedMessage, normalized_topic_hash
from token_zulip.zulip_io import ZulipClientIO, normalize_zulip_event


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


def test_normalize_zulip_private_event_uses_sender_session_and_requires_reply():
    event = {
        "type": "message",
        "message": {
            "id": 43,
            "type": "private",
            "display_recipient": [
                {"id": 3, "email": "alice@example.com", "full_name": "Alice"},
                {"id": 99, "email": "bot@example.com", "full_name": "Bot"},
            ],
            "sender_email": "alice@example.com",
            "sender_full_name": "Alice",
            "sender_id": 3,
            "content": "<p>hi</p>",
            "timestamp": 1710000001,
        },
    }

    message = normalize_zulip_event(event, "realm")

    assert message is not None
    assert message.conversation_type == "private"
    assert message.reply_required is True
    assert message.session_key.value == "zulip:realm:private:user:3"
    assert message.stream_id is None
    assert message.content == "hi"


def test_normalize_zulip_group_private_event_is_ignored():
    event = {
        "type": "message",
        "message": {
            "id": 44,
            "type": "private",
            "display_recipient": [
                {"id": 3, "email": "alice@example.com", "full_name": "Alice"},
                {"id": 4, "email": "bob@example.com", "full_name": "Bob"},
                {"id": 99, "email": "bot@example.com", "full_name": "Bot"},
            ],
            "sender_email": "alice@example.com",
            "sender_full_name": "Alice",
            "sender_id": 3,
            "content": "hi all",
        },
    }

    assert normalize_zulip_event(event, "realm") is None


def test_zulip_private_reply_posts_to_sender_email():
    class FakeClient:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        def send_message(self, request: dict[str, object]) -> dict[str, str]:
            self.requests.append(request)
            return {"result": "success"}

    message = NormalizedMessage(
        realm_id="realm",
        message_id=43,
        stream_id=None,
        stream="private",
        stream_slug="private",
        topic="private",
        topic_hash="3",
        conversation_type="private",
        private_user_key="3",
        reply_required=True,
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=3,
        content="hi",
        timestamp=None,
        received_at="now",
        raw={},
    )
    client = FakeClient()

    result = asyncio.run(ZulipClientIO(client).post_reply(message, "Hello."))

    assert result["request"] == {
        "type": "private",
        "to": ["alice@example.com"],
        "content": "Hello.",
    }
    assert client.requests == [result["request"]]


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
