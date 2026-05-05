from __future__ import annotations

from token_zulip.instructions import InstructionLoader
from token_zulip.models import normalized_topic_hash, private_scope_dir_name, stream_scope_dir_name, topic_scope_dir_name
from token_zulip.workspace import initialize_workspace


def test_instruction_layers_are_ordered(tmp_path):
    initialize_workspace(tmp_path)
    stream_dir = tmp_path / "instructions" / stream_scope_dir_name(10, "engineering")
    topic_hash = normalized_topic_hash("Launch Plan")
    topic_dir = stream_dir / topic_scope_dir_name(topic_hash, "launch-plan")
    topic_dir.mkdir(parents=True)
    (stream_dir / "AGENTS.md").write_text("stream rule", encoding="utf-8")
    (topic_dir / "AGENTS.md").write_text("topic rule", encoding="utf-8")

    text = InstructionLoader(tmp_path).compose("Engineering", topic_hash, topic="Launch Plan", stream_id=10)

    assert "## Source: references/system.md" in text
    assert text.index("## Source: references/system.md") < text.index("## Source: AGENTS.md")
    assert text.index("## Source: AGENTS.md") < text.index("## Source: references/reply/system.md")
    assert "Do not try to write files" not in text
    assert "references/reflections/system.md" not in text
    assert "references/schedule/system.md" not in text
    stream_label = "instructions/stream-engineering-10/AGENTS.md"
    topic_label = f"instructions/stream-engineering-10/topic-launch-plan-{topic_hash}/AGENTS.md"
    assert text.index("## Source: references/reply/system.md") < text.index(stream_label)
    assert text.index(stream_label) < text.index(topic_label)
    assert "stream rule" in text
    assert "topic rule" in text


def test_default_instruction_content_names_silica_and_research_guardrails(tmp_path):
    initialize_workspace(tmp_path)

    text = InstructionLoader(tmp_path).compose(
        "Engineering",
        normalized_topic_hash("Launch Plan"),
        stream_id=10,
    )

    assert "Silica" in text
    assert "Sili" in text
    assert "research coach" in text
    assert "Do not fabricate sources" in text
    assert "source verification" in text


def test_default_instruction_files_keep_style_and_participation_boundaries(tmp_path):
    initialize_workspace(tmp_path)

    global_text = (tmp_path / "AGENTS.md").read_text(encoding="utf-8")
    reply_system_text = (tmp_path / "references" / "reply" / "system.md").read_text(encoding="utf-8")
    reflections_system_text = (tmp_path / "references" / "reflections" / "system.md").read_text(encoding="utf-8")

    assert "```spoiler Details" in global_text
    assert "Keep replies chat-sized" in global_text
    assert "long useful public-channel replies" in global_text
    assert "supporting detail, caveats, or long checklists" in global_text
    assert "when Silica can materially improve" not in global_text
    assert "```spoiler Details" not in reply_system_text
    assert "when Silica can materially improve" in reply_system_text
    assert "use available lookup tools" in reply_system_text
    assert "named tools/frameworks" in reply_system_text
    assert "include source links in the visible reply" in reply_system_text
    assert "instead of suggesting search terms" in reply_system_text
    assert "unsupported claims" in reflections_system_text
    assert "reflection_ops: []" in reflections_system_text
    assert "Do not create topic-level reflections" in reflections_system_text
    assert "X asked/reported/confirmed Y" in reflections_system_text


def test_private_instruction_loads_scoped_agents(tmp_path):
    initialize_workspace(tmp_path)
    private_dir = tmp_path / "instructions" / private_scope_dir_name("42")
    private_dir.mkdir(parents=True)
    (private_dir / "AGENTS.md").write_text("private rule", encoding="utf-8")

    text = InstructionLoader(tmp_path).compose(
        "",
        normalized_topic_hash("private"),
        conversation_type="private",
        private_recipient_key="42",
    )

    assert f"instructions/{private_scope_dir_name('42')}/AGENTS.md" in text
    assert "private rule" in text


def test_worker_instruction_profiles_do_not_load_reply_policy(tmp_path):
    initialize_workspace(tmp_path)

    text = InstructionLoader(tmp_path).compose(
        "Engineering",
        normalized_topic_hash("Launch Plan"),
        role="schedule_worker",
        stream_id=10,
        template_values={"schedule_timezone": "Asia/Shanghai", "schedule_default_time": "09:00"},
    )

    assert "## Source: references/system.md" in text
    assert "## Source: references/schedule/system.md" in text
    assert "## Source: references/reply/system.md" not in text
    assert "## Source: AGENTS.md" not in text
    assert "schedule_ops" in text
    assert "mention_targets" in text
    assert "zero, one, or multiple person targets" in text
    assert "`@**topic**` mentions topic participants" in text
    assert "omitted timezone uses `Asia/Shanghai`" in text
    assert 'omitted clock time or "morning" uses `09:00`' in text
    assert '"every morning" uses `09:00` as a daily cron' in text
    assert "$schedule_timezone" not in text
    assert "$schedule_default_time" not in text
    assert "prefer an exact `job_id`" in text
    assert "Current Scheduled Tasks Here" in text


def test_shared_instruction_includes_zulip_mention_semantics_for_reply_and_workers(tmp_path):
    initialize_workspace(tmp_path)
    topic_hash = normalized_topic_hash("Launch Plan")

    reply_text = InstructionLoader(tmp_path).compose("Engineering", topic_hash, stream_id=10)
    schedule_text = InstructionLoader(tmp_path).compose(
        "Engineering",
        topic_hash,
        role="schedule_worker",
        stream_id=10,
    )

    for text in (reply_text, schedule_text):
        assert "Zulip mention Markdown" in text
        assert "`@**Full Name**`" in text
        assert "`@_**Full Name**`" in text
        assert "`@*group name*`" in text
        assert "`@**topic**`" in text


def test_scheduled_job_instruction_mentions_persisted_mentions_only(tmp_path):
    initialize_workspace(tmp_path)

    text = InstructionLoader(tmp_path).compose(
        "Engineering",
        normalized_topic_hash("Launch Plan"),
        role="scheduled_job",
        stream_id=10,
    )

    assert "## Source: references/scheduled_job/system.md" in text
    assert "persisted mention targets" in text
    assert "Never invent person, topic, channel, or all mentions" in text
