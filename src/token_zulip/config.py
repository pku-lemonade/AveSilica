from __future__ import annotations

import os
import re
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


def _positive_int_env(name: str, default: int) -> int:
    result = _int_env(name, default)
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _float_env(name: str, default: float) -> float:
    value = os.getenv(name)
    if value is None:
        return default
    try:
        result = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a number") from exc
    if result <= 0:
        raise ValueError(f"{name} must be positive")
    return result


def _optional_int_env(name: str) -> int | None:
    value = os.getenv(name)
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc


def _aliases_env(name: str, default: tuple[str, ...]) -> tuple[str, ...]:
    value = os.getenv(name)
    if value is None:
        return default
    aliases = tuple(alias.strip() for alias in value.split(",") if alias.strip())
    return aliases


def _time_env(name: str, default: str) -> str:
    value = os.getenv(name)
    result = default if value is None else value.strip()
    if not re.fullmatch(r"(?:[01]\d|2[0-3]):[0-5]\d", result):
        raise ValueError(f"{name} must be HH:MM in 24-hour time")
    return result


@dataclass(frozen=True)
class BotConfig:
    workspace_dir: Path
    zulip_config_file: Path | None
    realm_id: str
    bot_email: str | None
    bot_user_id: int | None
    bot_aliases: tuple[str, ...]
    codex_model: str
    codex_reasoning_effort: str | None
    codex_cwd: Path
    codex_sandbox: str | None
    codex_approval_policy: str
    max_recent_messages: int
    queue_limit: int
    worker_count: int
    instruction_max_bytes: int
    upload_max_bytes: int
    post_replies: bool
    listen_all_public_streams: bool
    typing_enabled: bool
    typing_refresh_seconds: float
    schedules_enabled: bool = True
    schedule_tick_seconds: float = 60.0
    schedule_timezone: str = "UTC"
    schedule_default_time: str = "09:00"
    schedule_run_timeout_seconds: float = 600.0
    schedule_skill_max_bytes: int = 32_000
    schedule_skill_max_count: int = 4
    trace_retention_days: int = 30
    trace_auto_cleanup: bool = False
    trace_cleanup_interval_hours: float = 24.0

    @classmethod
    def from_env(cls) -> "BotConfig":
        workspace = Path(os.getenv("TOKENZULIP_WORKSPACE", "workspace")).expanduser().resolve()
        codex_cwd = Path(os.getenv("TOKENZULIP_CODEX_CWD", str(workspace))).expanduser().resolve()
        return cls(
            workspace_dir=workspace,
            zulip_config_file=_optional_path(os.getenv("TOKENZULIP_ZULIPRC")),
            realm_id=os.getenv("TOKENZULIP_REALM_ID", "unknown"),
            bot_email=os.getenv("TOKENZULIP_BOT_EMAIL") or None,
            bot_user_id=_optional_int_env("TOKENZULIP_BOT_USER_ID"),
            bot_aliases=_aliases_env("TOKENZULIP_BOT_ALIASES", ("Silica", "Sili")),
            codex_model=os.getenv("TOKENZULIP_CODEX_MODEL", "gpt-5.4"),
            codex_reasoning_effort=os.getenv("TOKENZULIP_CODEX_REASONING_EFFORT") or "medium",
            codex_cwd=codex_cwd,
            codex_sandbox=os.getenv("TOKENZULIP_CODEX_SANDBOX", "read-only") or None,
            codex_approval_policy=os.getenv("TOKENZULIP_CODEX_APPROVAL_POLICY", "never"),
            max_recent_messages=_int_env("TOKENZULIP_RECENT_MESSAGES", 100),
            queue_limit=_int_env("TOKENZULIP_QUEUE_LIMIT", 64),
            worker_count=_int_env("TOKENZULIP_WORKERS", 4),
            instruction_max_bytes=_int_env("TOKENZULIP_INSTRUCTION_MAX_BYTES", 96_000),
            upload_max_bytes=_int_env("TOKENZULIP_UPLOAD_MAX_BYTES", 25_000_000),
            post_replies=_bool_env("TOKENZULIP_POST_REPLIES", True),
            listen_all_public_streams=_bool_env("TOKENZULIP_LISTEN_ALL_PUBLIC_STREAMS", True),
            typing_enabled=_bool_env("TOKENZULIP_TYPING_ENABLED", True),
            typing_refresh_seconds=_float_env("TOKENZULIP_TYPING_REFRESH_SECONDS", 8.0),
            schedules_enabled=_bool_env("TOKENZULIP_SCHEDULES_ENABLED", True),
            schedule_tick_seconds=_float_env("TOKENZULIP_SCHEDULE_TICK_SECONDS", 60.0),
            schedule_timezone=os.getenv("TOKENZULIP_SCHEDULE_TIMEZONE") or os.getenv("TZ") or "UTC",
            schedule_default_time=_time_env("TOKENZULIP_SCHEDULE_DEFAULT_TIME", "09:00"),
            schedule_run_timeout_seconds=_float_env("TOKENZULIP_SCHEDULE_RUN_TIMEOUT_SECONDS", 600.0),
            schedule_skill_max_bytes=_int_env("TOKENZULIP_SCHEDULE_SKILL_MAX_BYTES", 32_000),
            schedule_skill_max_count=_int_env("TOKENZULIP_SCHEDULE_SKILL_MAX_COUNT", 4),
            trace_retention_days=_positive_int_env("TOKENZULIP_TRACE_RETENTION_DAYS", 30),
            trace_auto_cleanup=_bool_env("TOKENZULIP_TRACE_AUTO_CLEANUP", False),
            trace_cleanup_interval_hours=_float_env("TOKENZULIP_TRACE_CLEANUP_INTERVAL_HOURS", 24.0),
        )
