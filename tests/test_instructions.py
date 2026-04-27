from __future__ import annotations

from token_zulip.instructions import InstructionLoader
from token_zulip.models import normalized_topic_hash
from token_zulip.workspace import initialize_workspace


def test_instruction_layers_are_ordered(tmp_path):
    initialize_workspace(tmp_path)
    stream_dir = tmp_path / "channels" / "engineering"
    topic_hash = normalized_topic_hash("Launch Plan")
    topic_dir = stream_dir / topic_hash
    topic_dir.mkdir(parents=True)
    (stream_dir / "AGENTS.md").write_text("stream rule", encoding="utf-8")
    (topic_dir / "AGENTS.md").write_text("topic rule", encoding="utf-8")

    text = InstructionLoader(tmp_path).compose("Engineering", topic_hash, role="default")

    assert "hardcoded safety contract" in text
    assert text.index("AGENTS.md") < text.index("roles/default.md")
    assert text.index("loop/memory.md") < text.index("channels/engineering/AGENTS.md")
    assert text.index("channels/engineering/AGENTS.md") < text.index(f"channels/engineering/{topic_hash}/AGENTS.md")
    assert "stream rule" in text
    assert "topic rule" in text


def test_default_instruction_content_names_silica_and_research_guardrails(tmp_path):
    initialize_workspace(tmp_path)

    text = InstructionLoader(tmp_path).compose("Engineering", normalized_topic_hash("Launch Plan"), role="default")

    assert "Silica" in text
    assert "Sili" in text
    assert "research coach" in text
    assert "Do not fabricate sources" in text
    assert "source verification" in text


def test_default_instruction_files_keep_style_and_participation_boundaries(tmp_path):
    initialize_workspace(tmp_path)

    global_text = (tmp_path / "AGENTS.md").read_text(encoding="utf-8")
    role_text = (tmp_path / "roles" / "default.md").read_text(encoding="utf-8")
    participation_text = (tmp_path / "loop" / "participation.md").read_text(encoding="utf-8")

    assert "```spoiler Details" in role_text
    assert "Keep replies chat-sized" in role_text
    assert "```spoiler Details" not in global_text
    assert "Keep replies chat-sized" not in global_text
    assert "```spoiler Details" not in participation_text
    assert "when Silica can materially improve" in participation_text
