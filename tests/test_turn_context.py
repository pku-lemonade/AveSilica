from __future__ import annotations

from token_zulip.models import NormalizedMessage
from token_zulip.turn_context import RenderContext, TurnContext, WorkflowDeltas


def _message(
    message_id: int,
    *,
    content: str = "hello",
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
    )


def test_turn_context_from_messages_preserves_message_conversation_and_render_fields() -> None:
    turn = TurnContext.from_messages(
        [_message(1, content="first"), _message(2, content="second")],
        deltas=WorkflowDeltas(reflection_context="# Reflection Scope\n\nbody"),
        render=RenderContext(message_timezone="Asia/Shanghai"),
    )

    assert turn.deltas.reflection_context == "# Reflection Scope\n\nbody"
    assert turn.render.message_timezone == "Asia/Shanghai"
    assert turn.messages[0].message_id == 1
    assert turn.messages[0].sender_label == "User 1"
    assert turn.messages[1].content == "second"
    assert turn.conversation.kind == "stream"
    assert turn.conversation.stream == "Engineering"
    assert turn.conversation.topic == "Launch"


def test_workflow_deltas_select_concise_role_sections() -> None:
    deltas = WorkflowDeltas(
        reflection_context="reflection",
        posted_bot_updates="posted",
        scheduling_context="time",
        current_schedules="jobs",
        mentionable_participants="people",
        same_turn_skill_changes="skill changes",
        applied_changes="applied",
    )

    assert deltas.sections_for_role("reflections") == ["reflection"]
    assert deltas.sections_for_role("skill") == []
    assert deltas.sections_for_role("schedule") == ["time", "jobs", "people", "skill changes"]
    assert deltas.sections_for_role("post") == ["posted", "applied"]
