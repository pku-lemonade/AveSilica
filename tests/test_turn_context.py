from __future__ import annotations

from token_zulip.models import NormalizedMessage
from token_zulip.turn_context import TurnContext


def _message(
    message_id: int,
    *,
    content: str = "hello",
    reply_required: bool = False,
    directly_addressed: bool = False,
) -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=message_id,
        stream_id=10,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic123",
        sender_email=f"user{message_id}@example.com",
        sender_full_name=f"User {message_id}",
        sender_id=message_id,
        content=content,
        timestamp=1_767_225_600 + message_id,
        received_at="2026-01-01T00:00:00+00:00",
        raw={},
        reply_required=reply_required,
        directly_addressed=directly_addressed,
    )


def test_turn_context_from_messages_preserves_message_and_runtime_fields() -> None:
    turn = TurnContext.from_messages(
        [_message(1, content="first"), _message(2, content="second")],
        runtime_context="# Existing Context\nbody",
        message_timezone="Asia/Shanghai",
    )

    assert turn.runtime_context == "# Existing Context\nbody"
    assert turn.message_timezone == "Asia/Shanghai"
    assert turn.messages[0].message_id == 1
    assert turn.messages[0].sender_label == "User 1"
    assert turn.messages[1].content == "second"
    assert turn.conversation.conversation_type == "stream"
    assert turn.conversation.stream == "Engineering"
    assert turn.conversation.topic == "Launch"


def test_turn_context_aggregates_reply_flags_across_messages() -> None:
    turn = TurnContext.from_messages(
        [
            _message(1, reply_required=False, directly_addressed=False),
            _message(2, reply_required=True, directly_addressed=True),
        ]
    )

    assert turn.conversation.reply_required is True
    assert turn.conversation.directly_addressed is True
