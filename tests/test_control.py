from __future__ import annotations

from token_zulip.control import parse_control_command
from token_zulip.models import NormalizedMessage


def _message(content: str, *, directly_addressed: bool = False, private: bool = False) -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=1,
        stream_id=None if private else 10,
        stream="private" if private else "Engineering",
        stream_slug="private" if private else "engineering",
        topic="private" if private else "Launch",
        topic_hash="1001" if private else "topic123",
        conversation_type="private" if private else "stream",
        private_recipient_key="1001" if private else None,
        private_recipients=[{"user_id": 1, "email": "alice@example.com", "full_name": "Alice"}] if private else [],
        post_required=private,
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content=content,
        timestamp=None,
        received_at="now",
        raw={},
        directly_addressed=directly_addressed,
    )


def _command_name(message: NormalizedMessage) -> str | None:
    command = parse_control_command(message, ("Silica", "Sili"))
    return command.name if command is not None else None


def test_control_commands_are_case_insensitive_and_alias_prefixed():
    assert _command_name(_message("SILI STATUS")) == "status"
    assert _command_name(_message("silica: Clear.")) == "clear"
    assert _command_name(_message("@**Silica|99** status")) == "status"


def test_private_messages_accept_bare_commands():
    assert _command_name(_message("STATUS", private=True)) == "status"
    assert _command_name(_message("clear?", private=True)) == "clear"


def test_stream_bare_command_must_be_directly_addressed():
    assert _command_name(_message("status")) is None
    assert _command_name(_message("status", directly_addressed=True)) == "status"


def test_control_parser_rejects_extra_words():
    assert _command_name(_message("sili status please")) is None
    assert _command_name(_message("silicon status")) is None
