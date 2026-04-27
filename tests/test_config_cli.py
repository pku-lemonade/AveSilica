from __future__ import annotations

from pathlib import Path

from token_zulip.cli import main
from token_zulip.config import BotConfig
from token_zulip.workspace import WORKSPACE_TEMPLATE_FILES


TEMPLATE_ROOT = Path(__file__).resolve().parents[1] / "workspace"


def test_default_workspace_is_workspace(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TOKENZULIP_WORKSPACE", raising=False)
    monkeypatch.delenv("TOKENZULIP_CODEX_CWD", raising=False)
    monkeypatch.delenv("TOKENZULIP_CODEX_REASONING_EFFORT", raising=False)
    monkeypatch.delenv("TOKENZULIP_LISTEN_ALL_PUBLIC_STREAMS", raising=False)
    monkeypatch.delenv("TOKENZULIP_BOT_ALIASES", raising=False)
    monkeypatch.delenv("TOKENZULIP_TYPING_ENABLED", raising=False)
    monkeypatch.delenv("TOKENZULIP_TYPING_REFRESH_SECONDS", raising=False)

    config = BotConfig.from_env()

    assert config.workspace_dir == tmp_path / "workspace"
    assert config.codex_cwd == tmp_path / "workspace"
    assert config.codex_reasoning_effort == "medium"
    assert config.listen_all_public_streams is True
    assert config.bot_aliases == ("Silica", "Sili")
    assert config.typing_enabled is True
    assert config.typing_refresh_seconds == 8.0


def test_listen_all_public_streams_can_be_disabled(monkeypatch):
    monkeypatch.setenv("TOKENZULIP_LISTEN_ALL_PUBLIC_STREAMS", "false")

    config = BotConfig.from_env()

    assert config.listen_all_public_streams is False


def test_cli_init_creates_workspace_layout(tmp_path):
    workspace = tmp_path / "workspace"

    assert main(["--workspace", str(workspace), "init"]) == 0

    assert (workspace / "AGENTS.md").exists()
    assert (workspace / "references" / "participation.md").exists()
    assert (workspace / "references" / "memory-policy.md").exists()
    assert (workspace / "memory" / "AGENTS.md").exists()
    assert (workspace / "memory" / "MEMORY.md").exists()
    assert (workspace / "memory" / "seeds.jsonl").exists()
    assert not (workspace / "roles").exists()
    assert not (workspace / "loop").exists()
    for relative in WORKSPACE_TEMPLATE_FILES:
        assert (workspace / relative).read_bytes() == (TEMPLATE_ROOT / relative).read_bytes()
