from __future__ import annotations

import re
import shutil
from pathlib import Path


WORKSPACE_DIRS: tuple[str, ...] = (
    "references",
    "memory",
    "records",
    "records/errors",
)

WORKSPACE_TEMPLATE_FILES: tuple[str, ...] = (
    "AGENTS.md",
    "references/participation.md",
    "references/memory-policy.md",
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
