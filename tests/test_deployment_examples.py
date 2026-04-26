from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def test_env_example_has_codex_yolo_proxy_and_mount_settings():
    text = (ROOT / "examples" / ".env.example").read_text(encoding="utf-8")

    assert "TOKENZULIP_CODEX_SANDBOX=danger-full-access" in text
    assert "TOKENZULIP_CODEX_APPROVAL_POLICY=never" in text
    assert "TOKENZULIP_CODEX_REASONING_EFFORT=medium" in text
    assert "YOUR_USER" not in text
    assert "HOST_CODEX_HOME" not in text
    assert "HTTP_PROXY=http://127.0.0.1:50834" in text
    assert "http_proxy=http://127.0.0.1:50834" in text


def test_systemd_example_mounts_runtime_and_codex_with_host_network():
    text = (ROOT / "examples" / "systemd" / "token-zulip.service").read_text(encoding="utf-8")

    assert "--network host" in text
    assert "--volume /opt/token-zulip:/runtime" in text
    assert "--volume %h/.codex:/root/.codex" in text
    assert "WantedBy=default.target" in text
