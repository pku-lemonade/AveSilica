from __future__ import annotations

import re
import shutil
from pathlib import Path


WORKSPACE_DIRS: tuple[str, ...] = (
    "references",
    "skills",
    "schedules",
    "memory",
    "records",
    "records/errors",
    "records/scheduled",
)

CODEX_THREAD_CONTRACT_FILE = "references/codex-thread-contract.md"
REPLY_TURN_PROMPT_FILE = "references/reply-turn-prompt.md"
MEMORY_WORKER_PROMPT_FILE = "references/memory-worker-prompt.md"
SKILL_WORKER_PROMPT_FILE = "references/skill-worker-prompt.md"
SCHEDULE_WORKER_PROMPT_FILE = "references/schedule-worker-prompt.md"
SCHEDULED_JOB_PROMPT_FILE = "references/scheduled-job-prompt.md"
REPLY_DECISION_SCHEMA_FILE = "references/reply-decision-schema.json"
MEMORY_DECISION_SCHEMA_FILE = "references/memory-decision-schema.json"
SKILL_DECISION_SCHEMA_FILE = "references/skill-decision-schema.json"
SCHEDULE_DECISION_SCHEMA_FILE = "references/schedule-decision-schema.json"
SCHEDULED_JOB_DECISION_SCHEMA_FILE = "references/scheduled-job-decision-schema.json"
DECISION_SCHEMA_FILE = REPLY_DECISION_SCHEMA_FILE

WORKSPACE_TEMPLATE_FILES: tuple[str, ...] = (
    CODEX_THREAD_CONTRACT_FILE,
    REPLY_TURN_PROMPT_FILE,
    MEMORY_WORKER_PROMPT_FILE,
    SKILL_WORKER_PROMPT_FILE,
    SCHEDULE_WORKER_PROMPT_FILE,
    SCHEDULED_JOB_PROMPT_FILE,
    REPLY_DECISION_SCHEMA_FILE,
    MEMORY_DECISION_SCHEMA_FILE,
    SKILL_DECISION_SCHEMA_FILE,
    SCHEDULE_DECISION_SCHEMA_FILE,
    SCHEDULED_JOB_DECISION_SCHEMA_FILE,
    "AGENTS.md",
    "references/reply-thread-policy.md",
    "references/memory-worker-policy.md",
    "references/skill-worker-policy.md",
    "references/schedule-worker-policy.md",
    "references/scheduled-job-policy.md",
    "memory/AGENTS.md",
    "memory/MEMORY.md",
)


def initialize_workspace(root: Path, overwrite: bool = False) -> list[Path]:
    root = root.expanduser().resolve()
    template_root = _workspace_template_root(root)
    created: list[Path] = []

    for relative in WORKSPACE_DIRS:
        (root / relative).mkdir(parents=True, exist_ok=True)

    same_tree = _same_path(root, template_root)
    for relative in WORKSPACE_TEMPLATE_FILES:
        destination = root / relative
        source = template_root / relative

        if destination.exists() and not overwrite:
            continue
        if same_tree:
            continue
        if not source.exists():
            raise FileNotFoundError(f"workspace template file missing: {source}")

        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
        created.append(destination)

    return created


def strip_markdown_comments(text: str) -> str:
    return re.sub(r"<!--.*?-->", "", text, flags=re.DOTALL).strip()


def _workspace_template_root(target: Path) -> Path:
    for candidate in _workspace_template_candidates(target):
        if _same_path(candidate, target):
            return candidate
        if all((candidate / relative).exists() for relative in WORKSPACE_TEMPLATE_FILES):
            return candidate
    raise FileNotFoundError("unable to locate checked-in workspace template files")


def _workspace_template_candidates(target: Path) -> list[Path]:
    module_path = Path(__file__).resolve()
    candidates = [
        module_path.parents[2] / "workspace",
        Path.cwd() / "workspace",
        Path("/app/workspace"),
    ]
    unique: list[Path] = []
    seen: set[Path] = set()
    for candidate in candidates:
        resolved = candidate.expanduser().resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        unique.append(resolved)
    return unique


def _same_path(left: Path, right: Path) -> bool:
    return left.expanduser().resolve() == right.expanduser().resolve()
