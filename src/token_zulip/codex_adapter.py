from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .telemetry import CodexCallTimer
from .workspace import DECISION_SCHEMA_FILE


@dataclass(frozen=True)
class CodexRunResult:
    raw_text: str
    thread_id: str | None
    raw_result: Any = None
    stats: dict[str, Any] | None = None


@dataclass(frozen=True)
class CodexWorkerSpec:
    kind: str
    prompt: str
    developer_instructions: str
    output_schema_path: Path


@dataclass(frozen=True)
class CodexTurnWithForksResult:
    main: CodexRunResult
    workers: dict[str, CodexRunResult]
    worker_errors: dict[str, str]


class CodexAdapter(Protocol):
    async def ensure_thread(
        self,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        ...

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        ...

    async def run_turn_with_forks(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None,
        main_output_schema_path: Path,
        worker_specs: list[CodexWorkerSpec],
    ) -> CodexTurnWithForksResult:
        ...

    async def run_worker_fork(
        self,
        parent_thread_id: str,
        worker_spec: CodexWorkerSpec,
    ) -> CodexRunResult:
        ...


class CodexSdkAdapter:
    def __init__(
        self,
        *,
        model: str,
        cwd: Path,
        reasoning_effort: str | None = None,
        sandbox: str | None = "read-only",
        approval_policy: str = "never",
        output_schema_path: Path | None = None,
    ) -> None:
        self.model = model
        self.cwd = cwd.expanduser().resolve()
        self.reasoning_effort = reasoning_effort
        self.sandbox = sandbox
        self.approval_policy = approval_policy
        self.output_schema_path = output_schema_path.expanduser().resolve() if output_schema_path else None

    async def ensure_thread(
        self,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
    ) -> CodexRunResult:
        try:
            from codex_app_server import AppServerConfig, AsyncCodex  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Codex Python SDK is not installed. Install the optional SDK dependency "
                "or provide a custom CodexAdapter."
            ) from exc

        self.cwd.mkdir(parents=True, exist_ok=True)
        timer = CodexCallTimer(
            operation="ensure_thread",
            model=self.model,
            effort=self.reasoning_effort,
            model_call=False,
            input_thread_id=thread_id,
        )
        async with AsyncCodex(config=AppServerConfig(codex_bin=self._codex_bin(), cwd=str(self.cwd))) as codex:
            thread_kwargs = self._thread_kwargs()
            if thread_id:
                with timer.phase("thread_resume"):
                    thread = await codex.thread_resume(thread_id, **thread_kwargs)
            else:
                if developer_instructions:
                    thread_kwargs["developer_instructions"] = developer_instructions
                with timer.phase("thread_start"):
                    thread = await codex.thread_start(**thread_kwargs)

            resolved_thread_id = str(getattr(thread, "id", "") or thread_id or "") or None
            if not resolved_thread_id:
                raise RuntimeError("Codex parent thread did not provide a thread id")
            return CodexRunResult(
                raw_text="",
                thread_id=resolved_thread_id,
                raw_result=thread,
                stats=timer.finish(resolved_thread_id=resolved_thread_id),
            )

    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
        output_schema_path: Path | None = None,
    ) -> CodexRunResult:
        try:
            from codex_app_server import AppServerConfig, AsyncCodex  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Codex Python SDK is not installed. Install the optional SDK dependency "
                "or provide a custom CodexAdapter."
            ) from exc

        self.cwd.mkdir(parents=True, exist_ok=True)
        timer = CodexCallTimer(
            operation="run_decision",
            model=self.model,
            effort=self.reasoning_effort,
            model_call=True,
            input_thread_id=thread_id,
        )
        async with AsyncCodex(config=AppServerConfig(codex_bin=self._codex_bin(), cwd=str(self.cwd))) as codex:
            thread_kwargs = self._thread_kwargs()
            if thread_id:
                with timer.phase("thread_resume"):
                    thread = await codex.thread_resume(thread_id, **thread_kwargs)
            else:
                if developer_instructions:
                    thread_kwargs["developer_instructions"] = developer_instructions
                with timer.phase("thread_start"):
                    thread = await codex.thread_start(**thread_kwargs)

            with timer.phase("model_run"):
                result = await thread.run(prompt, **self._run_kwargs(output_schema_path=output_schema_path))

            raw_text = str(getattr(result, "final_response", "") or "")
            resolved_thread_id = str(getattr(thread, "id", "") or thread_id or "") or None
            return CodexRunResult(
                raw_text=raw_text,
                thread_id=resolved_thread_id,
                raw_result=result,
                stats=timer.finish(raw_result=result, resolved_thread_id=resolved_thread_id),
            )

    async def run_turn_with_forks(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None,
        main_output_schema_path: Path,
        worker_specs: list[CodexWorkerSpec],
    ) -> CodexTurnWithForksResult:
        try:
            from codex_app_server import AppServerConfig, AsyncCodex  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Codex Python SDK is not installed. Install the optional SDK dependency "
                "or provide a custom CodexAdapter."
            ) from exc

        self.cwd.mkdir(parents=True, exist_ok=True)
        main_timer = CodexCallTimer(
            operation="run_decision",
            model=self.model,
            effort=self.reasoning_effort,
            model_call=True,
            input_thread_id=thread_id,
        )
        async with AsyncCodex(config=AppServerConfig(codex_bin=self._codex_bin(), cwd=str(self.cwd))) as codex:
            thread_kwargs = self._thread_kwargs()
            if thread_id:
                with main_timer.phase("thread_resume"):
                    parent = await codex.thread_resume(thread_id, **thread_kwargs)
            else:
                if developer_instructions:
                    thread_kwargs["developer_instructions"] = developer_instructions
                with main_timer.phase("thread_start"):
                    parent = await codex.thread_start(**thread_kwargs)

            parent_id = str(getattr(parent, "id", "") or thread_id or "") or None
            if not parent_id:
                raise RuntimeError("Codex parent thread did not provide a thread id")

            with main_timer.phase("model_run"):
                main_result = await parent.run(
                    prompt,
                    **self._run_kwargs(output_schema_path=main_output_schema_path),
                )
            main_stats = main_timer.finish(raw_result=main_result, resolved_thread_id=parent_id)

            workers: dict[str, CodexRunResult] = {}
            worker_errors: dict[str, str] = {}
            for spec in worker_specs:
                worker_timer = CodexCallTimer(
                    operation="worker_fork",
                    model=self.model,
                    effort=self.reasoning_effort,
                    model_call=True,
                    parent_thread_id=parent_id,
                )
                try:
                    fork_kwargs = {
                        **self._thread_kwargs(),
                        "developer_instructions": spec.developer_instructions,
                        "ephemeral": True,
                        "exclude_turns": True,
                    }
                    with worker_timer.phase("thread_fork"):
                        fork = await codex.thread_fork(parent_id, **fork_kwargs)
                    with worker_timer.phase("model_run"):
                        result = await fork.run(
                            spec.prompt,
                            **self._run_kwargs(output_schema_path=spec.output_schema_path),
                        )
                    worker_thread_id = str(getattr(fork, "id", "") or "") or None
                    workers[spec.kind] = CodexRunResult(
                        raw_text=str(getattr(result, "final_response", "") or ""),
                        thread_id=worker_thread_id,
                        raw_result=result,
                        stats=worker_timer.finish(raw_result=result, resolved_thread_id=worker_thread_id),
                    )
                except Exception as exc:
                    worker_errors[spec.kind] = str(exc)

            return CodexTurnWithForksResult(
                main=CodexRunResult(
                    raw_text=str(getattr(main_result, "final_response", "") or ""),
                    thread_id=parent_id,
                    raw_result=main_result,
                    stats=main_stats,
                ),
                workers=workers,
                worker_errors=worker_errors,
            )

    async def run_worker_fork(
        self,
        parent_thread_id: str,
        worker_spec: CodexWorkerSpec,
    ) -> CodexRunResult:
        try:
            from codex_app_server import AppServerConfig, AsyncCodex  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Codex Python SDK is not installed. Install the optional SDK dependency "
                "or provide a custom CodexAdapter."
            ) from exc

        parent_id = parent_thread_id.strip()
        if not parent_id:
            raise RuntimeError("Codex parent thread id is required for worker fork")

        self.cwd.mkdir(parents=True, exist_ok=True)
        timer = CodexCallTimer(
            operation="worker_fork",
            model=self.model,
            effort=self.reasoning_effort,
            model_call=True,
            parent_thread_id=parent_id,
        )
        async with AsyncCodex(config=AppServerConfig(codex_bin=self._codex_bin(), cwd=str(self.cwd))) as codex:
            fork_kwargs = {
                **self._thread_kwargs(),
                "developer_instructions": worker_spec.developer_instructions,
                "ephemeral": True,
                "exclude_turns": True,
            }
            with timer.phase("thread_fork"):
                fork = await codex.thread_fork(parent_id, **fork_kwargs)
            with timer.phase("model_run"):
                result = await fork.run(
                    worker_spec.prompt,
                    **self._run_kwargs(output_schema_path=worker_spec.output_schema_path),
                )
            worker_thread_id = str(getattr(fork, "id", "") or "") or None
            return CodexRunResult(
                raw_text=str(getattr(result, "final_response", "") or ""),
                thread_id=worker_thread_id,
                raw_result=result,
                stats=timer.finish(raw_result=result, resolved_thread_id=worker_thread_id),
            )

    def _thread_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "cwd": str(self.cwd),
            "approval_policy": self.approval_policy,
        }
        if self.sandbox:
            kwargs["sandbox"] = self.sandbox
        return kwargs

    def _run_kwargs(self, *, output_schema_path: Path | None = None) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"output_schema": self._output_schema(output_schema_path=output_schema_path)}
        if self.reasoning_effort:
            kwargs["effort"] = self.reasoning_effort
        return kwargs

    def _output_schema(self, *, output_schema_path: Path | None = None) -> dict[str, Any]:
        path = output_schema_path or self.output_schema_path or (self.cwd / DECISION_SCHEMA_FILE)
        path = path.expanduser().resolve() if not path.is_absolute() else path
        if not path.exists():
            raise FileNotFoundError(f"decision schema file missing: {path}")
        schema = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(schema, dict):
            raise ValueError(f"decision schema must be a JSON object: {path}")
        return schema

    def _codex_bin(self) -> str:
        codex_bin = shutil.which("codex")
        if not codex_bin:
            raise RuntimeError("Codex CLI is not installed or is not on PATH.")
        return codex_bin
