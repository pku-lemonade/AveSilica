from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .workspace import DECISION_SCHEMA_FILE


@dataclass(frozen=True)
class CodexRunResult:
    raw_text: str
    thread_id: str | None
    raw_result: Any = None


class CodexAdapter(Protocol):
    async def run_decision(
        self,
        prompt: str,
        thread_id: str | None,
        *,
        developer_instructions: str | None = None,
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

    async def run_decision(
        self,
        prompt: str,
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
        async with AsyncCodex(config=AppServerConfig(codex_bin=self._codex_bin(), cwd=str(self.cwd))) as codex:
            thread_kwargs = self._thread_kwargs()
            if thread_id:
                thread = await codex.thread_resume(thread_id, **thread_kwargs)
            else:
                if developer_instructions:
                    thread_kwargs["developer_instructions"] = developer_instructions
                thread = await codex.thread_start(**thread_kwargs)

            result = await thread.run(prompt, **self._run_kwargs())

            raw_text = str(getattr(result, "final_response", "") or "")
            resolved_thread_id = str(getattr(thread, "id", "") or thread_id or "") or None
            return CodexRunResult(raw_text=raw_text, thread_id=resolved_thread_id, raw_result=result)

    def _thread_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {
            "model": self.model,
            "cwd": str(self.cwd),
            "approval_policy": self.approval_policy,
        }
        if self.sandbox:
            kwargs["sandbox"] = self.sandbox
        return kwargs

    def _run_kwargs(self) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"output_schema": self._output_schema()}
        if self.reasoning_effort:
            kwargs["effort"] = self.reasoning_effort
        return kwargs

    def _output_schema(self) -> dict[str, Any]:
        path = self.output_schema_path or (self.cwd / DECISION_SCHEMA_FILE)
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
