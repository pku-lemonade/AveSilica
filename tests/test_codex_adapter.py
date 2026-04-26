from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass

from token_zulip.codex_adapter import CodexSdkAdapter


def test_codex_adapter_uses_installed_sdk_api(monkeypatch, tmp_path):
    @dataclass
    class FakeAppServerConfig:
        codex_bin: str
        cwd: str | None = None

    class FakeRunResult:
        final_text = '{"should_reply": false}'
        final_response = '{"should_reply": false}'

    class FakeThread:
        id = "thread-2"

        def __init__(self) -> None:
            self.run_kwargs: dict[str, object] = {}

        async def run(self, prompt: str, **kwargs) -> FakeRunResult:
            self.run_kwargs = {"prompt": prompt, **kwargs}
            return FakeRunResult()

    class FakeAsyncCodex:
        last: "FakeAsyncCodex | None" = None

        def __init__(self, *, config: FakeAppServerConfig) -> None:
            self.config = config
            self.thread_kwargs: dict[str, object] = {}
            self.thread = FakeThread()
            FakeAsyncCodex.last = self

        async def __aenter__(self) -> "FakeAsyncCodex":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def thread_resume(self, thread_id: str, **kwargs) -> FakeThread:
            self.thread_kwargs = {"thread_id": thread_id, **kwargs}
            return self.thread

    fake_sdk = types.SimpleNamespace(
        AppServerConfig=FakeAppServerConfig,
        AsyncCodex=FakeAsyncCodex,
    )
    monkeypatch.setitem(sys.modules, "codex_app_server", fake_sdk)
    monkeypatch.setattr("token_zulip.codex_adapter.shutil.which", lambda name: "/usr/local/bin/codex")

    adapter = CodexSdkAdapter(
        model="gpt-test",
        cwd=tmp_path,
        reasoning_effort="low",
        sandbox="danger-full-access",
        approval_policy="never",
    )

    result = asyncio.run(adapter.run_decision("prompt", "thread-1"))

    assert result.raw_text == '{"should_reply": false}'
    assert result.thread_id == "thread-2"
    assert FakeAsyncCodex.last is not None
    assert FakeAsyncCodex.last.config == FakeAppServerConfig(
        codex_bin="/usr/local/bin/codex",
        cwd=str(tmp_path),
    )
    assert FakeAsyncCodex.last.thread_kwargs == {
        "thread_id": "thread-1",
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
    }
    assert FakeAsyncCodex.last.thread.run_kwargs["prompt"] == "prompt"
    assert FakeAsyncCodex.last.thread.run_kwargs["effort"] == "low"
    assert FakeAsyncCodex.last.thread.run_kwargs["output_schema"]
