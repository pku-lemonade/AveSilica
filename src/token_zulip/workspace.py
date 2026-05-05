from __future__ import annotations

import re
import shutil
from pathlib import Path


WORKSPACE_DIRS: tuple[str, ...] = (
    "references",
    "references/reply",
    "references/reflections",
    "references/skill",
    "references/schedule",
    "references/scheduled_job",
    "instructions",
    "reflections",
    "skills",
    "schedules",
    "records",
    "records/errors",
    "records/scheduled",
)

SHARED_SYSTEM_FILE = "references/system.md"
REPLY_TURN_USER_PROMPT_FILE = "references/reply/user.md"
REFLECTIONS_WORKER_USER_PROMPT_FILE = "references/reflections/user.md"
SKILL_WORKER_USER_PROMPT_FILE = "references/skill/user.md"
SCHEDULE_WORKER_USER_PROMPT_FILE = "references/schedule/user.md"
SCHEDULED_JOB_USER_PROMPT_FILE = "references/scheduled_job/user.md"
REPLY_DECISION_SCHEMA_FILE = "references/reply/schema.json"
REFLECTIONS_DECISION_SCHEMA_FILE = "references/reflections/schema.json"
SKILL_DECISION_SCHEMA_FILE = "references/skill/schema.json"
SCHEDULE_DECISION_SCHEMA_FILE = "references/schedule/schema.json"
SCHEDULED_JOB_DECISION_SCHEMA_FILE = "references/scheduled_job/schema.json"
DECISION_SCHEMA_FILE = REPLY_DECISION_SCHEMA_FILE

WORKSPACE_TEMPLATE_FILES: tuple[str, ...] = (
    SHARED_SYSTEM_FILE,
    REPLY_TURN_USER_PROMPT_FILE,
    REFLECTIONS_WORKER_USER_PROMPT_FILE,
    SKILL_WORKER_USER_PROMPT_FILE,
    SCHEDULE_WORKER_USER_PROMPT_FILE,
    SCHEDULED_JOB_USER_PROMPT_FILE,
    REPLY_DECISION_SCHEMA_FILE,
    REFLECTIONS_DECISION_SCHEMA_FILE,
    SKILL_DECISION_SCHEMA_FILE,
    SCHEDULE_DECISION_SCHEMA_FILE,
    SCHEDULED_JOB_DECISION_SCHEMA_FILE,
    "AGENTS.md",
    "references/reply/system.md",
    "references/reflections/system.md",
    "references/skill/system.md",
    "references/schedule/system.md",
    "references/scheduled_job/system.md",
    "reflections/REFLECTIONS.md",
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
