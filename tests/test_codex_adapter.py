from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass

from token_zulip.codex_adapter import CodexSdkAdapter
from token_zulip.workspace import initialize_workspace


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
        instances: list["FakeAsyncCodex"] = []

        def __init__(self, *, config: FakeAppServerConfig) -> None:
            self.config = config
            self.thread_kwargs: dict[str, object] = {}
            self.thread = FakeThread()
            FakeAsyncCodex.last = self
            FakeAsyncCodex.instances.append(self)

        async def __aenter__(self) -> "FakeAsyncCodex":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def thread_start(self, **kwargs) -> FakeThread:
            self.thread_kwargs = kwargs
            return self.thread

        async def thread_resume(self, thread_id: str, **kwargs) -> FakeThread:
            self.thread_kwargs = {"thread_id": thread_id, **kwargs}
            return self.thread

    fake_sdk = types.SimpleNamespace(
        AppServerConfig=FakeAppServerConfig,
        AsyncCodex=FakeAsyncCodex,
    )
    monkeypatch.setitem(sys.modules, "codex_app_server", fake_sdk)
    monkeypatch.setattr("token_zulip.codex_adapter.shutil.which", lambda name: "/usr/local/bin/codex")
    initialize_workspace(tmp_path)

    adapter = CodexSdkAdapter(
        model="gpt-test",
        cwd=tmp_path,
        reasoning_effort="low",
        sandbox="danger-full-access",
        approval_policy="never",
    )

    started = asyncio.run(adapter.run_decision("prompt", None, developer_instructions="dev instructions"))

    assert started.raw_text == '{"should_reply": false}'
    assert started.thread_id == "thread-2"
    assert FakeAsyncCodex.last is not None
    assert FakeAsyncCodex.last.config == FakeAppServerConfig(
        codex_bin="/usr/local/bin/codex",
        cwd=str(tmp_path),
    )
    assert FakeAsyncCodex.last.thread_kwargs == {
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
        "developer_instructions": "dev instructions",
    }
    assert FakeAsyncCodex.last.thread.run_kwargs["prompt"] == "prompt"
    assert FakeAsyncCodex.last.thread.run_kwargs["effort"] == "low"
    assert FakeAsyncCodex.last.thread.run_kwargs["output_schema"]

    resumed = asyncio.run(adapter.run_decision("prompt", "thread-1", developer_instructions="ignored"))

    assert resumed.raw_text == '{"should_reply": false}'
    assert FakeAsyncCodex.last.thread_kwargs == {
        "thread_id": "thread-1",
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
    }
