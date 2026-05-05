from __future__ import annotations

from pathlib import Path

import pytest

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
    monkeypatch.delenv("TOKENZULIP_UPLOAD_MAX_BYTES", raising=False)
    monkeypatch.delenv("TOKENZULIP_RECENT_MESSAGES", raising=False)
    monkeypatch.delenv("TOKENZULIP_SCHEDULE_DEFAULT_TIME", raising=False)
    monkeypatch.delenv("TOKENZULIP_TRACE_RETENTION_DAYS", raising=False)
    monkeypatch.delenv("TOKENZULIP_TRACE_AUTO_CLEANUP", raising=False)
    monkeypatch.delenv("TOKENZULIP_TRACE_CLEANUP_INTERVAL_HOURS", raising=False)

    config = BotConfig.from_env()

    assert config.workspace_dir == tmp_path / "workspace"
    assert config.codex_cwd == tmp_path / "workspace"
    assert config.codex_reasoning_effort == "medium"
    assert config.listen_all_public_streams is True
    assert config.bot_aliases == ("Silica", "Sili")
    assert config.typing_enabled is True
    assert config.typing_refresh_seconds == 8.0
    assert config.upload_max_bytes == 25_000_000
    assert config.max_recent_messages == 100
    assert config.schedule_default_time == "09:00"
    assert config.trace_retention_days == 30
    assert config.trace_auto_cleanup is False
    assert config.trace_cleanup_interval_hours == 24.0


def test_listen_all_public_streams_can_be_disabled(monkeypatch):
    monkeypatch.setenv("TOKENZULIP_LISTEN_ALL_PUBLIC_STREAMS", "false")

    config = BotConfig.from_env()

    assert config.listen_all_public_streams is False


def test_schedule_default_time_env(monkeypatch):
    monkeypatch.setenv("TOKENZULIP_SCHEDULE_DEFAULT_TIME", "08:30")

    config = BotConfig.from_env()

    assert config.schedule_default_time == "08:30"


def test_schedule_default_time_env_requires_hh_mm(monkeypatch):
    monkeypatch.setenv("TOKENZULIP_SCHEDULE_DEFAULT_TIME", "9am")

    with pytest.raises(ValueError, match="TOKENZULIP_SCHEDULE_DEFAULT_TIME must be HH:MM"):
        BotConfig.from_env()


def test_trace_cleanup_env(monkeypatch):
    monkeypatch.setenv("TOKENZULIP_TRACE_RETENTION_DAYS", "7")
    monkeypatch.setenv("TOKENZULIP_TRACE_AUTO_CLEANUP", "true")
    monkeypatch.setenv("TOKENZULIP_TRACE_CLEANUP_INTERVAL_HOURS", "6")

    config = BotConfig.from_env()

    assert config.trace_retention_days == 7
    assert config.trace_auto_cleanup is True
    assert config.trace_cleanup_interval_hours == 6.0


def test_cli_init_creates_workspace_layout(tmp_path):
    workspace = tmp_path / "workspace"

    assert main(["--workspace", str(workspace), "init"]) == 0

    assert (workspace / "realm" / "AGENTS.md").exists()
    assert (workspace / "realm" / "REFLECTIONS.md").exists()
    assert (workspace / "references" / "post" / "system.md").exists()
    assert (workspace / "references" / "reflections" / "system.md").exists()
    assert (workspace / "references" / "schedule" / "user.md").exists()
    assert not (workspace / "instructions").exists()
    assert not (workspace / "reflections").exists()
    assert not (workspace / "records").exists()
    assert (workspace / "realm" / "runtime" / "errors").exists()
    assert not (workspace / "state").exists()
    assert not (workspace / "roles").exists()
    assert not (workspace / "loop").exists()
    for relative in WORKSPACE_TEMPLATE_FILES:
        assert (workspace / relative).read_bytes() == (TEMPLATE_ROOT / relative).read_bytes()
