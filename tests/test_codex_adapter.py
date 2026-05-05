from __future__ import annotations

import asyncio
import sys
import types
from dataclasses import dataclass

from token_zulip.codex_adapter import CodexSdkAdapter, CodexWorkerSpec
from token_zulip.workspace import initialize_workspace


def test_codex_adapter_uses_installed_sdk_api(monkeypatch, tmp_path):
    @dataclass
    class FakeAppServerConfig:
        codex_bin: str
        cwd: str | None = None

    @dataclass
    class FakeUsageBreakdown:
        input_tokens: int = 11
        cached_input_tokens: int = 3
        output_tokens: int = 7
        reasoning_output_tokens: int = 2
        total_tokens: int = 20

    @dataclass
    class FakeUsage:
        last: FakeUsageBreakdown
        total: FakeUsageBreakdown
        model_context_window: int = 128000

    class FakeRunResult:
        final_text = '{"should_reply": false}'
        final_response = '{"should_reply": false}'
        usage = FakeUsage(
            last=FakeUsageBreakdown(),
            total=FakeUsageBreakdown(
                input_tokens=101,
                cached_input_tokens=30,
                output_tokens=70,
                reasoning_output_tokens=20,
                total_tokens=191,
            ),
        )

    class FakeThread:
        events: list[tuple[str, str, str]] = []
        next_id = 2

        def __init__(self, thread_id: str | None = None) -> None:
            self.id = thread_id or f"thread-{FakeThread.next_id}"
            FakeThread.next_id += 1

            self.run_kwargs: dict[str, object] = {}

        async def run(self, prompt: str, **kwargs) -> FakeRunResult:
            FakeThread.events.append(("run", self.id, prompt))
            self.run_kwargs = {"prompt": prompt, **kwargs}
            return FakeRunResult()

    class FakeAsyncCodex:
        last: "FakeAsyncCodex | None" = None
        instances: list["FakeAsyncCodex"] = []

        def __init__(self, *, config: FakeAppServerConfig) -> None:
            self.config = config
            self.thread_kwargs: dict[str, object] = {}
            self.thread = FakeThread("thread-2")
            self.fork_kwargs: list[dict[str, object]] = []
            self.forks: list[FakeThread] = []
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
            self.thread = FakeThread(thread_id)
            return self.thread

        async def thread_fork(self, thread_id: str, **kwargs) -> FakeThread:
            fork = FakeThread(f"fork-{len(self.forks) + 1}")
            FakeThread.events.append(
                ("fork", thread_id, str(kwargs.get("developer_instructions") or ""))
            )
            self.fork_kwargs.append({"thread_id": thread_id, **kwargs})
            self.forks.append(fork)
            return fork

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
    assert started.stats is not None
    assert started.stats["operation"] == "run_decision"
    assert started.stats["model"] == "gpt-test"
    assert started.stats["effort"] == "low"
    assert started.stats["api_call_count"] == 1
    assert started.stats["tokens"]["last"]["input_tokens"] == 11
    assert started.stats["tokens"]["last"]["cached_input_tokens"] == 3
    assert started.stats["tokens"]["last"]["reasoning_output_tokens"] == 2
    assert started.stats["tokens"]["total"]["total_tokens"] == 191
    assert started.stats["tokens"]["model_context_window"] == 128000

    resumed = asyncio.run(adapter.run_decision("prompt", "thread-1", developer_instructions="ignored"))

    assert resumed.raw_text == '{"should_reply": false}'
    assert FakeAsyncCodex.last.thread_kwargs == {
        "thread_id": "thread-1",
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
    }

    FakeThread.events.clear()
    ensured = asyncio.run(adapter.ensure_thread(None, developer_instructions="dev instructions"))

    assert ensured.raw_text == ""
    assert ensured.thread_id == "thread-2"
    assert ensured.stats is not None
    assert ensured.stats["operation"] == "ensure_thread"
    assert ensured.stats["api_call_count"] == 0
    assert "tokens" not in ensured.stats
    assert FakeThread.events == []
    assert FakeAsyncCodex.last.thread_kwargs == {
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
        "developer_instructions": "dev instructions",
    }

    ensured_resumed = asyncio.run(adapter.ensure_thread("thread-1", developer_instructions="ignored"))

    assert ensured_resumed.raw_text == ""
    assert ensured_resumed.thread_id == "thread-1"
    assert FakeAsyncCodex.last.thread_kwargs == {
        "thread_id": "thread-1",
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
    }

    FakeThread.events.clear()
    forked = asyncio.run(
        adapter.run_turn_with_forks(
            "prompt",
            "thread-1",
            developer_instructions=None,
            main_output_schema_path=tmp_path / "references" / "reply" / "schema.json",
            worker_specs=[
                CodexWorkerSpec(
                    kind="reflections",
                    prompt="reflections prompt",
                    developer_instructions="reflections instructions",
                    output_schema_path=tmp_path / "references" / "reflections" / "schema.json",
                )
            ],
        )
    )

    assert forked.main.thread_id == "thread-1"
    assert set(forked.workers) == {"reflections"}
    assert FakeThread.events[:3] == [
        ("run", "thread-1", "prompt"),
        ("fork", "thread-1", "reflections instructions"),
        ("run", "fork-1", "reflections prompt"),
    ]
    assert FakeAsyncCodex.last.fork_kwargs == [
        {
            "thread_id": "thread-1",
            "model": "gpt-test",
            "cwd": str(tmp_path),
            "approval_policy": "never",
            "sandbox": "danger-full-access",
            "developer_instructions": "reflections instructions",
            "ephemeral": True,
            "exclude_turns": True,
        }
    ]
    assert FakeAsyncCodex.last.forks[0].run_kwargs["output_schema"]
    assert FakeAsyncCodex.last.forks[0].run_kwargs["prompt"] == "reflections prompt"
    assert forked.main.stats is not None
    assert forked.main.stats["operation"] == "run_decision"
    assert forked.workers["reflections"].stats is not None
    assert forked.workers["reflections"].stats["operation"] == "worker_fork"
    assert forked.workers["reflections"].stats["parent_thread_id"] == "thread-1"

    FakeThread.events.clear()
    fresh_forked = asyncio.run(
        adapter.run_turn_with_forks(
            "fresh prompt",
            None,
            developer_instructions="reply instructions",
            main_output_schema_path=tmp_path / "references" / "reply" / "schema.json",
            worker_specs=[
                CodexWorkerSpec(
                    kind="reflections",
                    prompt="fresh reflections prompt",
                    developer_instructions="fresh reflections instructions",
                    output_schema_path=tmp_path / "references" / "reflections" / "schema.json",
                )
            ],
        )
    )

    assert fresh_forked.main.thread_id == "thread-2"
    assert FakeAsyncCodex.last.thread_kwargs == {
        "model": "gpt-test",
        "cwd": str(tmp_path),
        "approval_policy": "never",
        "sandbox": "danger-full-access",
        "developer_instructions": "reply instructions",
    }
    assert FakeThread.events[0] == ("run", "thread-2", "fresh prompt")
    assert FakeThread.events[1] == ("fork", "thread-2", "fresh reflections instructions")
    assert FakeThread.events[2] == ("run", "fork-1", "fresh reflections prompt")
    assert FakeAsyncCodex.last.forks[0].run_kwargs["prompt"] == "fresh reflections prompt"

    worker = asyncio.run(
        adapter.run_worker_fork(
            "thread-1",
            CodexWorkerSpec(
                kind="schedule",
                prompt="schedule prompt",
                developer_instructions="schedule instructions",
                output_schema_path=tmp_path / "references" / "schedule" / "schema.json",
            ),
        )
    )

    assert worker.thread_id == "fork-1"
    assert worker.stats is not None
    assert worker.stats["operation"] == "worker_fork"
    assert worker.stats["parent_thread_id"] == "thread-1"
    assert worker.stats["tokens"]["last"]["output_tokens"] == 7
    assert FakeAsyncCodex.last.fork_kwargs == [
        {
            "thread_id": "thread-1",
            "model": "gpt-test",
            "cwd": str(tmp_path),
            "approval_policy": "never",
            "sandbox": "danger-full-access",
            "developer_instructions": "schedule instructions",
            "ephemeral": True,
            "exclude_turns": True,
        }
    ]
    assert FakeAsyncCodex.last.forks[0].run_kwargs["prompt"] == "schedule prompt"


def test_run_turn_with_forks_runs_workers_sequentially(monkeypatch, tmp_path):
    @dataclass
    class FakeAppServerConfig:
        codex_bin: str
        cwd: str | None = None

    class FakeRunResult:
        def __init__(self, text: str) -> None:
            self.final_response = text

    class FakeThread:
        active_runs = 0
        events: list[tuple[str, str, str]] = []

        def __init__(self, thread_id: str) -> None:
            self.id = thread_id

        async def run(self, prompt: str, **kwargs) -> FakeRunResult:
            if FakeThread.active_runs:
                raise RuntimeError("concurrent runs are not supported")
            FakeThread.active_runs += 1
            try:
                FakeThread.events.append(("run", self.id, prompt))
                await asyncio.sleep(0)
                return FakeRunResult(f'{{"thread":"{self.id}"}}')
            finally:
                FakeThread.active_runs -= 1

    class FakeAsyncCodex:
        def __init__(self, *, config: FakeAppServerConfig) -> None:
            self.config = config
            self.fork_count = 0

        async def __aenter__(self) -> "FakeAsyncCodex":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def thread_resume(self, thread_id: str, **kwargs) -> FakeThread:
            return FakeThread(thread_id)

        async def thread_fork(self, thread_id: str, **kwargs) -> FakeThread:
            self.fork_count += 1
            fork = FakeThread(f"fork-{self.fork_count}")
            FakeThread.events.append(("fork", thread_id, str(kwargs.get("developer_instructions") or "")))
            return fork

    monkeypatch.setitem(
        sys.modules,
        "codex_app_server",
        types.SimpleNamespace(AppServerConfig=FakeAppServerConfig, AsyncCodex=FakeAsyncCodex),
    )
    monkeypatch.setattr("token_zulip.codex_adapter.shutil.which", lambda name: "/usr/local/bin/codex")
    initialize_workspace(tmp_path)
    adapter = CodexSdkAdapter(model="gpt-test", cwd=tmp_path)

    result = asyncio.run(
        adapter.run_turn_with_forks(
            "reply prompt",
            "thread-1",
            developer_instructions=None,
            main_output_schema_path=tmp_path / "references" / "reply" / "schema.json",
            worker_specs=[
                CodexWorkerSpec(
                    kind="reflections",
                    prompt="reflections prompt",
                    developer_instructions="reflections instructions",
                    output_schema_path=tmp_path / "references" / "reflections" / "schema.json",
                ),
                CodexWorkerSpec(
                    kind="skill",
                    prompt="skill prompt",
                    developer_instructions="skill instructions",
                    output_schema_path=tmp_path / "references" / "skill" / "schema.json",
                ),
            ],
        )
    )

    assert set(result.workers) == {"reflections", "skill"}
    assert result.worker_errors == {}
    assert FakeThread.events == [
        ("run", "thread-1", "reply prompt"),
        ("fork", "thread-1", "reflections instructions"),
        ("run", "fork-1", "reflections prompt"),
        ("fork", "thread-1", "skill instructions"),
        ("run", "fork-2", "skill prompt"),
    ]


def test_run_turn_with_forks_continues_after_worker_failure(monkeypatch, tmp_path):
    @dataclass
    class FakeAppServerConfig:
        codex_bin: str
        cwd: str | None = None

    class FakeRunResult:
        final_response = '{"ok": true}'

    class FakeThread:
        events: list[tuple[str, str]] = []

        def __init__(self, thread_id: str, *, fail: bool = False) -> None:
            self.id = thread_id
            self.fail = fail

        async def run(self, prompt: str, **kwargs) -> FakeRunResult:
            FakeThread.events.append((self.id, prompt))
            if self.fail:
                raise RuntimeError(f"{self.id} failed")
            return FakeRunResult()

    class FakeAsyncCodex:
        def __init__(self, *, config: FakeAppServerConfig) -> None:
            self.config = config
            self.fork_count = 0

        async def __aenter__(self) -> "FakeAsyncCodex":
            return self

        async def __aexit__(self, exc_type, exc, tb) -> None:
            return None

        async def thread_resume(self, thread_id: str, **kwargs) -> FakeThread:
            return FakeThread(thread_id)

        async def thread_fork(self, thread_id: str, **kwargs) -> FakeThread:
            self.fork_count += 1
            return FakeThread(f"fork-{self.fork_count}", fail=self.fork_count == 1)

    monkeypatch.setitem(
        sys.modules,
        "codex_app_server",
        types.SimpleNamespace(AppServerConfig=FakeAppServerConfig, AsyncCodex=FakeAsyncCodex),
    )
    monkeypatch.setattr("token_zulip.codex_adapter.shutil.which", lambda name: "/usr/local/bin/codex")
    initialize_workspace(tmp_path)
    adapter = CodexSdkAdapter(model="gpt-test", cwd=tmp_path)

    result = asyncio.run(
        adapter.run_turn_with_forks(
            "reply prompt",
            "thread-1",
            developer_instructions=None,
            main_output_schema_path=tmp_path / "references" / "reply" / "schema.json",
            worker_specs=[
                CodexWorkerSpec(
                    kind="reflections",
                    prompt="reflections prompt",
                    developer_instructions="reflections instructions",
                    output_schema_path=tmp_path / "references" / "reflections" / "schema.json",
                ),
                CodexWorkerSpec(
                    kind="skill",
                    prompt="skill prompt",
                    developer_instructions="skill instructions",
                    output_schema_path=tmp_path / "references" / "skill" / "schema.json",
                ),
            ],
        )
    )

    assert "fork-1 failed" in result.worker_errors["reflections"]
    assert set(result.workers) == {"skill"}
    assert FakeThread.events == [
        ("thread-1", "reply prompt"),
        ("fork-1", "reflections prompt"),
        ("fork-2", "skill prompt"),
    ]
