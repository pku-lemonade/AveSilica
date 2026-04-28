from __future__ import annotations

from token_zulip.instructions import InstructionLoader
from token_zulip.models import normalized_topic_hash, private_memory_dir_name, stream_memory_dir_name, topic_memory_dir_name
from token_zulip.workspace import initialize_workspace


def test_instruction_layers_are_ordered(tmp_path):
    initialize_workspace(tmp_path)
    (tmp_path / "memory" / "AGENTS.md").write_text("workspace memory rule", encoding="utf-8")
    stream_dir = tmp_path / "memory" / stream_memory_dir_name(10, "engineering")
    topic_hash = normalized_topic_hash("Launch Plan")
    topic_dir = stream_dir / topic_memory_dir_name(topic_hash, "launch-plan")
    topic_dir.mkdir(parents=True)
    (stream_dir / "AGENTS.md").write_text("stream rule", encoding="utf-8")
    (topic_dir / "AGENTS.md").write_text("topic rule", encoding="utf-8")

    text = InstructionLoader(tmp_path).compose("Engineering", topic_hash, topic="Launch Plan", stream_id=10)

    assert "## Source: references/runtime-contract.md" in text
    assert text.index("## Source: references/runtime-contract.md") < text.index("## Source: AGENTS.md")
    assert text.index("## Source: AGENTS.md") < text.index("## Source: references/participation.md")
    assert text.index("## Source: references/participation.md") < text.index("## Source: references/memory-policy.md")
    assert text.index("## Source: references/memory-policy.md") < text.index("## Source: memory/AGENTS.md")
    assert "workspace memory rule" in text
    assert "Do not try to write files" not in text
    assert "Propose memory changes in the structured fields only" not in text
    stream_label = "memory/stream-engineering-10/AGENTS.md"
    topic_label = f"memory/stream-engineering-10/topic-launch-plan-{topic_hash}/AGENTS.md"
    assert text.index("## Source: memory/AGENTS.md") < text.index(stream_label)
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
    participation_text = (tmp_path / "references" / "participation.md").read_text(encoding="utf-8")
    memory_policy_text = (tmp_path / "references" / "memory-policy.md").read_text(encoding="utf-8")

    assert "```spoiler Details" in global_text
    assert "Keep replies chat-sized" in global_text
    assert "long useful public-channel replies" in global_text
    assert "supporting detail, caveats, or long checklists" in global_text
    assert "when Silica can materially improve" not in global_text
    assert "```spoiler Details" not in participation_text
    assert "when Silica can materially improve" in participation_text
    assert "use available lookup tools" in participation_text
    assert "instead of suggesting search terms" in participation_text
    assert "unsupported claims" in memory_policy_text
    assert "MEMORY.md" in memory_policy_text
    assert "memory_ops: []" in memory_policy_text
    assert "stream-<slug>-<id>/MEMORY.md" in memory_policy_text
    assert "content` to an empty string" in memory_policy_text
    assert "add" in memory_policy_text
    assert "replace" in memory_policy_text
    assert "remove" in memory_policy_text


def test_private_instruction_loads_memory_scoped_agents(tmp_path):
    initialize_workspace(tmp_path)
    private_dir = tmp_path / "memory" / private_memory_dir_name("42")
    private_dir.mkdir(parents=True)
    (private_dir / "AGENTS.md").write_text("private rule", encoding="utf-8")

    text = InstructionLoader(tmp_path).compose(
        "",
        normalized_topic_hash("private"),
        conversation_type="private",
        private_user_key="42",
    )

    assert f"memory/{private_memory_dir_name('42')}/AGENTS.md" in text
    assert "private rule" in text
