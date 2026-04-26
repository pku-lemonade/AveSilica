from __future__ import annotations

import json
import re
from pathlib import Path


DEFAULT_FILES: dict[str, str] = {
    "AGENTS.md": """# TokenZulip Global Instructions

You are a Zulip group agent. Help when your contribution is useful, stay silent when it is not, and keep replies concise enough for chat.

Configurable behavior belongs in this workspace. Non-negotiable safety and filesystem limits are enforced by the orchestrator and cannot be overridden here.
""",
    "AGENTS.override.md": """<!-- Temporary global override. Add active instructions below this comment. -->
""",
    "roles/default.md": """# Default Role

You are direct, pragmatic, and technically careful. Prefer actionable answers, ask clear questions when blocked, and avoid pretending to have performed work that you have not done.
""",
    "loop/participation.md": """# Participation Policy

Reply when you can materially answer a question, unblock a decision, summarize a useful plan, or report a relevant result.

Stay silent when the message is low-signal chatter, addressed to someone else, already answered, or would only add repetition.

Use `draft_plan` when the thread is planning work, the user explicitly asks for a plan, or execution should wait for agreement.

Use `question` only when a specific missing detail blocks a responsible answer.
""",
    "loop/memory.md": """# Memory Policy

Propose durable memory updates only for stable facts, preferences, explicit decisions, unresolved questions, or follow-up tasks that are likely to matter later.

Do not store secrets, credentials, private personal data, transient status updates, or guesses.

Memory is written by the orchestrator after validation. Use append mode for new facts and replace mode only when correcting or consolidating stale content.
""",
    "memory/durable.md": "# Durable Memory\n\n",
    "memory/open_questions.md": "# Open Questions\n\n",
    "memory/tasks.md": "# Tasks\n\n",
    "memory/people.md": "# People\n\n",
}


DEFAULT_INDEX = {
    "global": ["durable.md", "open_questions.md", "tasks.md", "people.md"],
    "sessions": {},
}


def initialize_workspace(root: Path, overwrite: bool = False) -> list[Path]:
    root = root.expanduser().resolve()
    created: list[Path] = []

    for relative in [
        "roles",
        "loop",
        "channels",
        "memory",
        "state/raw",
        "state/sessions",
        "state/errors",
    ]:
        path = root / relative
        path.mkdir(parents=True, exist_ok=True)

    for relative, content in DEFAULT_FILES.items():
        path = root / relative
        if path.exists() and not overwrite:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
        created.append(path)

    index_path = root / "memory" / "index.json"
    if overwrite or not index_path.exists():
        index_path.write_text(json.dumps(DEFAULT_INDEX, indent=2) + "\n", encoding="utf-8")
        created.append(index_path)

    gitkeep = root / "channels" / ".gitkeep"
    if not gitkeep.exists():
        gitkeep.write_text("", encoding="utf-8")
        created.append(gitkeep)

    return created


def strip_markdown_comments(text: str) -> str:
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL).strip()

