from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _optional_path(value: str | None) -> Path | None:
    if not value:
        return None
    return Path(value).expanduser().resolve()


def _bool_env(name: str, default: bool) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _int_env(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


@dataclass(frozen=True)
class BotConfig:
    workspace_dir: Path
    zulip_config_file: Path | None
    realm_id: str
    bot_email: str | None
    role: str
    codex_model: str
    codex_reasoning_effort: str | None
    codex_cwd: Path
    codex_sandbox: str | None
    codex_approval_policy: str
    max_recent_messages: int
    queue_limit: int
    worker_count: int
    instruction_max_bytes: int
    post_replies: bool

    @classmethod
    def from_env(cls) -> "BotConfig":
        workspace = Path(os.getenv("TOKENZULIP_WORKSPACE", "workspace")).expanduser().resolve()
        codex_cwd = Path(os.getenv("TOKENZULIP_CODEX_CWD", str(workspace))).expanduser().resolve()
        return cls(
            workspace_dir=workspace,
            zulip_config_file=_optional_path(os.getenv("TOKENZULIP_ZULIPRC")),
            realm_id=os.getenv("TOKENZULIP_REALM_ID", "unknown"),
            bot_email=os.getenv("TOKENZULIP_BOT_EMAIL") or None,
            role=os.getenv("TOKENZULIP_ROLE", "default"),
            codex_model=os.getenv("TOKENZULIP_CODEX_MODEL", "gpt-5.4"),
            codex_reasoning_effort=os.getenv("TOKENZULIP_CODEX_REASONING_EFFORT") or None,
            codex_cwd=codex_cwd,
            codex_sandbox=os.getenv("TOKENZULIP_CODEX_SANDBOX", "read-only") or None,
            codex_approval_policy=os.getenv("TOKENZULIP_CODEX_APPROVAL_POLICY", "never"),
            max_recent_messages=_int_env("TOKENZULIP_RECENT_MESSAGES", 40),
            queue_limit=_int_env("TOKENZULIP_QUEUE_LIMIT", 64),
            worker_count=_int_env("TOKENZULIP_WORKERS", 4),
            instruction_max_bytes=_int_env("TOKENZULIP_INSTRUCTION_MAX_BYTES", 96_000),
            post_replies=_bool_env("TOKENZULIP_POST_REPLIES", True),
        )
