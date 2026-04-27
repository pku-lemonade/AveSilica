from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any

from .codex_adapter import CodexSdkAdapter
from .config import BotConfig
from .instructions import InstructionLoader
from .loop import AgentLoop
from .memory import MemoryStore
from .prompt import PromptBuilder, PromptParts
from .storage import WorkspaceStorage
from .typing_status import NoOpTypingNotifier, TypingStatusManager
from .workspace import initialize_workspace
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
    memory = MemoryStore(config.workspace_dir / "memory")
    loop = AgentLoop(
        config=config,
        storage=storage,
        instructions=InstructionLoader(config.workspace_dir, max_bytes=config.instruction_max_bytes),
        memory=memory,
        codex=CodexSdkAdapter(
            model=config.codex_model,
            cwd=config.codex_cwd,
            reasoning_effort=config.codex_reasoning_effort,
            sandbox=config.codex_sandbox,
            approval_policy=config.codex_approval_policy,
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
    try:
        await asyncio.to_thread(
            zulip.listen,
            callback,
            all_public_streams=config.listen_all_public_streams,
        )
    finally:
        workers.cancel()
    return 0


def _render_prompt(args: argparse.Namespace) -> int:
    config = _config_from_args(args)
    initialize_workspace(config.workspace_dir)
    event = json.loads(args.event_file.read_text(encoding="utf-8"))
    message = normalize_zulip_event(
        event,
        config.realm_id,
        bot_user_id=config.bot_user_id,
        bot_aliases=config.bot_aliases,
    )
    if message is None:
        raise SystemExit("Event file does not contain a supported message event.")

    instructions = InstructionLoader(config.workspace_dir, config.instruction_max_bytes).compose(
        stream=message.stream,
        topic_hash=message.topic_hash,
        stream_id=message.stream_id,
        conversation_type=message.conversation_type,
        private_user_key=message.private_user_key,
    )
    memory = MemoryStore(config.workspace_dir / "memory").render_selected(message.session_key)
    prompt = PromptBuilder().build(
        PromptParts(
            instructions=instructions,
            memory=memory,
            recent_context=[],
            current_messages=[message],
        )
    )
    print(prompt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
