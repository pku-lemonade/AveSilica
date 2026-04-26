from __future__ import annotations

from token_zulip.cli import main
from token_zulip.config import BotConfig


def test_default_workspace_is_workspace(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("TOKENZULIP_WORKSPACE", raising=False)
    monkeypatch.delenv("TOKENZULIP_CODEX_CWD", raising=False)

    config = BotConfig.from_env()

    assert config.workspace_dir == tmp_path / "workspace"
    assert config.codex_cwd == tmp_path / "workspace"


def test_cli_init_creates_workspace_layout(tmp_path):
    workspace = tmp_path / "workspace"

    assert main(["--workspace", str(workspace), "init"]) == 0

    assert (workspace / "AGENTS.md").exists()
    assert (workspace / "roles" / "default.md").exists()
    assert (workspace / "memory" / "index.json").exists()

