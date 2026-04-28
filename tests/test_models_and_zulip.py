from __future__ import annotations

import asyncio
import json
from pathlib import Path

from token_zulip.addressing import alias_is_directly_addressed
from token_zulip.models import AgentDecision, NormalizedMessage, normalized_topic_hash
from token_zulip.workspace import DECISION_SCHEMA_FILE
from token_zulip.zulip_io import ZulipClientIO, ZulipTypingNotifier, normalize_zulip_event


TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "workspace"


def _assert_required_matches_properties(schema: object, path: str = "$") -> None:
    if isinstance(schema, dict):
        properties = schema.get("properties")
        if isinstance(properties, dict):
            assert sorted(schema.get("required", [])) == sorted(properties), path
        for key, value in schema.items():
            _assert_required_matches_properties(value, f"{path}.{key}")
    elif isinstance(schema, list):
        for index, item in enumerate(schema):
            _assert_required_matches_properties(item, f"{path}[{index}]")


def test_decision_json_schema_requires_all_declared_object_properties():
    schema = json.loads((TEMPLATE_ROOT / DECISION_SCHEMA_FILE).read_text(encoding="utf-8"))
    _assert_required_matches_properties(schema)


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


def test_normalize_zulip_stream_event_preserves_raw_markdown_content():
    event = {
        "type": "message",
        "message": {
            "id": 42,
            "type": "stream",
            "stream_id": 7,
            "display_recipient": "Engineering",
            "subject": "Launch",
            "sender_email": "alice@example.com",
            "sender_full_name": "Alice",
            "sender_id": 3,
            "content": "see ![diagram](/user_uploads/7/Ab/diagram.png)",
            "content_type": "text/x-markdown",
        },
    }

    message = normalize_zulip_event(event, "realm")

    assert message is not None
    assert message.content == "see ![diagram](/user_uploads/7/Ab/diagram.png)"


def test_normalize_zulip_stream_event_preserves_markdown_autolinks_without_content_type():
    event = {
        "type": "message",
        "message": {
            "id": 42,
            "type": "stream",
            "stream_id": 7,
            "display_recipient": "Engineering",
            "subject": "Launch",
            "sender_email": "alice@example.com",
            "sender_full_name": "Alice",
            "sender_id": 3,
            "content": "see <https://example.com>",
        },
    }

    message = normalize_zulip_event(event, "realm")

    assert message is not None
    assert message.content == "see <https://example.com>"


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


def test_alias_direct_address_detection_is_case_insensitive_and_conservative():
    aliases = ("Silica", "Sili")

    assert alias_is_directly_addressed("sili 回答一下", aliases)
    assert alias_is_directly_addressed("SILICA: can you check this?", aliases)
    assert alias_is_directly_addressed("@**Silica** please check", aliases)
    assert not alias_is_directly_addressed("silicon substrate", aliases)
    assert not alias_is_directly_addressed("basili is not the bot", aliases)


def test_normalize_stream_event_detects_direct_bot_addressing():
    base_message = {
        "id": 45,
        "type": "stream",
        "stream_id": 7,
        "display_recipient": "Engineering",
        "subject": "Launch",
        "sender_email": "alice@example.com",
        "sender_full_name": "Alice",
        "sender_id": 3,
        "content": "<p>hello</p>",
    }

    mentioned = normalize_zulip_event(
        {"type": "message", "message": {**base_message, "flags": ["mentioned"]}},
        "realm",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
    )
    html_mention = normalize_zulip_event(
        {
            "type": "message",
            "message": {
                **base_message,
                "id": 46,
                "content": '<p><span class="user-mention" data-user-id="99">@Silica</span> help</p>',
            },
        },
        "realm",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
    )
    alias = normalize_zulip_event(
        {"type": "message", "message": {**base_message, "id": 47, "content": "<p>sili 回答一下</p>"}},
        "realm",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
    )
    wildcard = normalize_zulip_event(
        {"type": "message", "message": {**base_message, "id": 48, "flags": ["wildcard_mentioned"]}},
        "realm",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
    )
    ordinary = normalize_zulip_event(
        {"type": "message", "message": {**base_message, "id": 49}},
        "realm",
        bot_user_id=99,
        bot_aliases=("Silica", "Sili"),
    )

    assert mentioned is not None and mentioned.directly_addressed is True
    assert html_mention is not None and html_mention.directly_addressed is True
    assert alias is not None and alias.directly_addressed is True
    assert wildcard is not None and wildcard.directly_addressed is False
    assert ordinary is not None and ordinary.directly_addressed is False


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


def test_zulip_typing_notifier_sends_stream_and_private_requests():
    class FakeClient:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        def set_typing_status(self, request: dict[str, object]) -> dict[str, str]:
            self.requests.append(request)
            return {"result": "success"}

    stream_message = NormalizedMessage(
        realm_id="realm",
        message_id=43,
        stream_id=7,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic",
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=3,
        content="hi",
        timestamp=None,
        received_at="now",
        raw={},
    )
    private_message = NormalizedMessage(
        realm_id="realm",
        message_id=44,
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
    notifier = ZulipTypingNotifier(client)

    asyncio.run(notifier.start(stream_message))
    asyncio.run(notifier.stop(stream_message))
    asyncio.run(notifier.start(private_message))
    asyncio.run(notifier.stop(private_message))

    assert client.requests == [
        {"type": "stream", "op": "start", "stream_id": 7, "topic": "Launch"},
        {"type": "stream", "op": "stop", "stream_id": 7, "topic": "Launch"},
        {"type": "private", "op": "start", "to": [3]},
        {"type": "private", "op": "stop", "to": [3]},
    ]


def test_zulip_typing_notifier_skips_private_message_without_sender_id():
    class FakeClient:
        def __init__(self) -> None:
            self.requests: list[dict[str, object]] = []

        def set_typing_status(self, request: dict[str, object]) -> dict[str, str]:
            self.requests.append(request)
            return {"result": "success"}

    message = NormalizedMessage(
        realm_id="realm",
        message_id=44,
        stream_id=None,
        stream="private",
        stream_slug="private",
        topic="private",
        topic_hash="email",
        conversation_type="private",
        private_user_key="email",
        reply_required=True,
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=None,
        content="hi",
        timestamp=None,
        received_at="now",
        raw={},
    )
    client = FakeClient()

    asyncio.run(ZulipTypingNotifier(client).start(message))

    assert client.requests == []


def test_zulip_listener_can_request_all_public_stream_events():
    class FakeClient:
        def __init__(self) -> None:
            self.calls: list[dict[str, object]] = []

        def call_on_each_event(self, callback, **kwargs: object) -> None:
            self.calls.append({"callback": callback, **kwargs})

    client = FakeClient()

    def callback(event: dict[str, object]) -> None:
        return None

    ZulipClientIO(client).listen(callback, all_public_streams=True)

    assert client.calls == [
        {
            "callback": callback,
            "event_types": ["message"],
            "all_public_streams": True,
            "apply_markdown": False,
        }
    ]


def test_agent_decision_parses_fenced_json_and_validates_memory_ops():
    payload = {
        "should_reply": True,
        "reply_kind": "chat",
        "message_to_post": "Done.",
        "memory_ops": [
            {
                "op": "replace",
                "scope": "conversation",
                "content": "Follow up",
                "old_text": "Previous follow up",
            }
        ],
        "confidence": 2,
    }

    decision = AgentDecision.from_json_text(f"```json\n{json.dumps(payload)}\n```")

    assert decision.should_reply is True
    assert decision.confidence == 1.0
    assert decision.memory_ops[0].op == "replace"
    assert decision.memory_ops[0].old_text == "Previous follow up"
