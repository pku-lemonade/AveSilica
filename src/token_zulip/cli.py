from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from dataclasses import replace
from datetime import timedelta
from pathlib import Path
from typing import Any

from .codex_adapter import CodexSdkAdapter
from .config import BotConfig
from .instructions import InstructionLoader
from .loop import AgentLoop
from .prompt import PromptBuilder
from .reflections import ReflectionStore
from .turn_context import RenderContext, TurnContext
from .storage import WorkspaceStorage
from .typing_status import NoOpTypingNotifier, TypingStatusManager
from .workspace import DECISION_SCHEMA_FILE, initialize_workspace
from .zulip_io import ZulipClientIO, ZulipTypingNotifier, normalize_zulip_event


def main(argv: list[str] | None = None) -> int:
    parser = _parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper()))

    if args.command == "init":
        config = _config_from_args(args)
        created = initialize_workspace(config.workspace_dir, overwrite=args.overwrite)
        print(f"Workspace: {config.workspace_dir}")
        print(f"Created/updated {len(created)} file(s).")
        return 0

    if args.command == "render-prompt":
        return _render_prompt(args)

    if args.command == "traces":
        return _traces(args)

    if args.command == "run":
        return asyncio.run(_run(args))

    parser.print_help()
    return 2


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="token-zulip")
    parser.add_argument("--workspace", type=Path, help="Bot workspace directory")
    parser.add_argument("--log-level", default="INFO")
    subparsers = parser.add_subparsers(dest="command")

    init = subparsers.add_parser("init", help="Create editable workspace files")
    init.add_argument("--overwrite", action="store_true", help="Overwrite existing default files")

    run = subparsers.add_parser("run", help="Run the Zulip listener")
    run.add_argument("--dry-run", action="store_true", help="Do not post Zulip replies")

    render = subparsers.add_parser("render-prompt", help="Render the prompt for one saved Zulip event")
    render.add_argument("event_file", type=Path)

    traces = subparsers.add_parser("traces", help="Inspect or clean prompt traces")
    trace_subparsers = traces.add_subparsers(dest="trace_command")
    trace_list = trace_subparsers.add_parser("list", help="List recent prompt traces")
    trace_list.add_argument("--limit", type=int, default=20)
    trace_inspect = trace_subparsers.add_parser("inspect", help="Print one prompt trace manifest")
    trace_inspect.add_argument("trace_id")
    trace_inspect.add_argument("--role", help="Print file paths for a single role")
    trace_cleanup = trace_subparsers.add_parser("cleanup", help="Delete old prompt traces only")
    trace_cleanup.add_argument("--older-than", help="Age such as 30d, 12h, or 90m")
    return parser


def _config_from_args(args: argparse.Namespace) -> BotConfig:
    config = BotConfig.from_env()
    updates: dict[str, Any] = {}
    if getattr(args, "workspace", None):
        updates["workspace_dir"] = args.workspace.expanduser().resolve()
        if str(config.codex_cwd) == str(config.workspace_dir):
            updates["codex_cwd"] = args.workspace.expanduser().resolve()
    if getattr(args, "dry_run", False):
        updates["post_replies"] = False
    return replace(config, **updates) if updates else config


async def _run(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    initialize_workspace(config.workspace_dir)

    zulip = ZulipClientIO.from_config(config)
    bot_profile = zulip.bot_profile()
    bot_email = config.bot_email or bot_profile.email
    bot_user_id = config.bot_user_id or bot_profile.user_id
    realm_id = config.realm_id
    if realm_id == "unknown":
        realm_id = bot_profile.realm_id or zulip.realm_id() or realm_id
    config = replace(config, bot_email=bot_email, bot_user_id=bot_user_id, realm_id=realm_id)

    typing_notifier = ZulipTypingNotifier(zulip.client) if config.typing_enabled else NoOpTypingNotifier()

    storage = WorkspaceStorage(config.workspace_dir)
    loop = AgentLoop(
        config=config,
        storage=storage,
        instructions=InstructionLoader(config.workspace_dir, max_bytes=config.instruction_max_bytes),
        reflections=ReflectionStore(config.workspace_dir / "reflections"),
        codex=CodexSdkAdapter(
            model=config.codex_model,
            cwd=config.codex_cwd,
            reasoning_effort=config.codex_reasoning_effort,
            sandbox=config.codex_sandbox,
            approval_policy=config.codex_approval_policy,
            output_schema_path=config.workspace_dir / DECISION_SCHEMA_FILE,
        ),
        zulip=zulip,
        typing=TypingStatusManager(
            typing_notifier,
            enabled=config.typing_enabled,
            refresh_seconds=config.typing_refresh_seconds,
        ),
    )

    running_loop = asyncio.get_running_loop()

    def callback(event: dict[str, Any]) -> None:
        future = asyncio.run_coroutine_threadsafe(loop.enqueue_event(event), running_loop)

        def report_result(done: asyncio.Future[Any]) -> None:
            try:
                result = done.result()
            except Exception:
                logging.exception("Unable to enqueue Zulip event")
                return
            logging.debug("Zulip event enqueue result: %s", result)

        future.add_done_callback(report_result)

    workers = asyncio.create_task(loop.run_workers())
    scheduler: asyncio.Task[None] | None = None
    trace_cleanup: asyncio.Task[None] | None = None
    if config.schedules_enabled:
        scheduler = asyncio.create_task(loop.run_scheduler())
    if config.trace_auto_cleanup:
        trace_cleanup = asyncio.create_task(_run_trace_cleanup(storage, config))
    try:
        await asyncio.to_thread(
            zulip.listen,
            callback,
            all_public_streams=config.listen_all_public_streams,
        )
    finally:
        workers.cancel()
        if scheduler is not None:
            scheduler.cancel()
        if trace_cleanup is not None:
            trace_cleanup.cancel()
    return 0


async def _run_trace_cleanup(storage: WorkspaceStorage, config: BotConfig) -> None:
    max_age = timedelta(days=config.trace_retention_days)
    interval = config.trace_cleanup_interval_hours * 3600
    while True:
        summary = storage.cleanup_traces_older_than(max_age)
        deleted = int(summary.get("deleted") or 0)
        if deleted:
            logging.info("Deleted %s prompt trace(s) older than %s day(s)", deleted, config.trace_retention_days)
        await asyncio.sleep(interval)


def _render_prompt(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    initialize_workspace(config.workspace_dir)
    event = json.loads(args.event_file.read_text(encoding="utf-8"))
    message = normalize_zulip_event(
        event,
        config.realm_id,
        bot_user_id=config.bot_user_id,
        bot_email=config.bot_email,
        bot_aliases=config.bot_aliases,
    )
    if message is None:
        raise SystemExit("Event file does not contain a supported message event.")

    prompt = PromptBuilder(config.workspace_dir).build(
        TurnContext.from_messages(
            [message],
            render=RenderContext(message_timezone=config.schedule_timezone),
        ),
        role="reply",
    )
    print(prompt)
    return 0


def _traces(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    storage = WorkspaceStorage(config.workspace_dir)
    command = getattr(args, "trace_command", None)
    if command == "cleanup":
        age = _parse_age(getattr(args, "older_than", None)) or timedelta(days=config.trace_retention_days)
        summary = storage.cleanup_traces_older_than(age)
        print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if command == "list":
        records = storage.list_traces(limit=max(0, int(getattr(args, "limit", 20))))
        print(json.dumps(records, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if command == "inspect":
        manifest = storage.read_trace_manifest(str(args.trace_id))
        if manifest is None:
            raise SystemExit(f"Trace not found: {args.trace_id}")
        role = str(getattr(args, "role", "") or "").strip()
        if role:
            roles = [item for item in manifest.get("roles", []) if isinstance(item, dict)]
            selected = next((item for item in roles if item.get("role") == role), None)
            if selected is None:
                raise SystemExit(f"Role not found in trace {args.trace_id}: {role}")
            print(json.dumps(selected, ensure_ascii=False, indent=2, sort_keys=True))
        else:
            print(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    raise SystemExit("Use one of: traces list, traces inspect, traces cleanup")


def _parse_age(value: str | None) -> timedelta | None:
    if value is None or not value.strip():
        return None
    text = value.strip().lower()
    match = re.fullmatch(r"(\d+)\s*([mhd])?", text)
    if not match:
        raise SystemExit("--older-than must look like 30d, 12h, or 90m")
    amount = int(match.group(1))
    if amount <= 0:
        raise SystemExit("--older-than must be positive")
    unit = match.group(2) or "d"
    if unit == "m":
        return timedelta(minutes=amount)
    if unit == "h":
        return timedelta(hours=amount)
    return timedelta(days=amount)


if __name__ == "__main__":
    raise SystemExit(main())
