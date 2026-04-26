from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from .models import DECISION_JSON_SCHEMA


@dataclass(frozen=True)
class CodexRunResult:
    raw_text: str
    thread_id: str | None
    raw_result: Any = None


class CodexAdapter(Protocol):
    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
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
    ) -> None:
        self.model = model
        self.cwd = cwd.expanduser().resolve()
        self.reasoning_effort = reasoning_effort
        self.sandbox = sandbox
        self.approval_policy = approval_policy

    async def run_decision(self, prompt: str, thread_id: str | None) -> CodexRunResult:
        try:
            from codex_app_server import AsyncCodex  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError(
                "Codex Python SDK is not installed. Install the optional SDK dependency "
                "or provide a custom CodexAdapter."
            ) from exc

        self.cwd.mkdir(parents=True, exist_ok=True)
        async with AsyncCodex() as codex:
            thread_kwargs = self._thread_kwargs()
            if thread_id:
                thread = await codex.thread_resume(thread_id, **thread_kwargs)
            else:
                thread = await codex.thread_start(**thread_kwargs)

            run_kwargs: dict[str, Any] = {"output_schema": DECISION_JSON_SCHEMA}
            if self.reasoning_effort:
                run_kwargs["effort"] = self.reasoning_effort
            result = await thread.run(prompt, **run_kwargs)

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

