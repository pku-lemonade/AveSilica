from __future__ import annotations

import re
from pathlib import Path
from typing import Any

from .models import SkillOperation


SKILL_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9-]{0,62}[a-z0-9]$")
DEFAULT_SKILL_MAX_BYTES = 32_000
DEFAULT_SKILL_MAX_COUNT = 4


class SkillStore:
    def __init__(
        self,
        skills_dir: Path,
        *,
        max_bytes: int = DEFAULT_SKILL_MAX_BYTES,
        max_count: int = DEFAULT_SKILL_MAX_COUNT,
    ) -> None:
        self.skills_dir = skills_dir.expanduser().resolve()
        self.max_bytes = max_bytes
        self.max_count = max_count
        self.skills_dir.mkdir(parents=True, exist_ok=True)

    def apply_ops(self, ops: list[SkillOperation]) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for op in ops:
            try:
                if op.action == "remove":
                    results.append(self.remove_skill(op.name))
                else:
                    results.append(self.write_skill(op))
            except Exception as exc:
                results.append(
                    {
                        "action": op.action,
                        "name": op.name,
                        "status": "rejected",
                        "reason": str(exc),
                    }
                )
        return results

    def write_skill(self, op: SkillOperation) -> dict[str, Any]:
        name = self.validate_name(op.name)
        description = op.description.strip()
        content = op.content.strip()
        if not description:
            return self._rejected(op.action, name, "description is required")
        if not content:
            return self._rejected(op.action, name, "content is required")

        text = self._skill_text(name, description, content)
        size = len(text.encode("utf-8"))
        if size > self.max_bytes:
            return self._rejected(op.action, name, f"skill exceeds {self.max_bytes} bytes ({size} bytes)")

        path = self.skill_path(name)
        if op.action == "create" and path.exists():
            return self._rejected(op.action, name, "skill already exists")
        self._write_text_atomic(path, text)
        return {
            "action": op.action,
            "name": name,
            "status": "applied",
            "path": str(path.relative_to(self.skills_dir.parent)),
        }

    def remove_skill(self, name: str) -> dict[str, Any]:
        name = self.validate_name(name)
        directory = self.skills_dir / name
        path = directory / "SKILL.md"
        if not path.exists():
            return self._rejected("remove", name, "skill not found")
        path.unlink()
        try:
            directory.rmdir()
        except OSError:
            pass
        return {
            "action": "remove",
            "name": name,
            "status": "applied",
            "path": str(path.relative_to(self.skills_dir.parent)),
        }

    def render_for_prompt(self, skill_names: list[str] | tuple[str, ...]) -> tuple[str, list[str]]:
        names: list[str] = []
        errors: list[str] = []
        for raw_name in skill_names:
            try:
                name = self.validate_name(str(raw_name))
            except ValueError as exc:
                errors.append(str(exc))
                continue
            if name not in names:
                names.append(name)
        if len(names) > self.max_count:
            errors.append(f"too many skills requested ({len(names)} > {self.max_count})")
            return "", errors

        blocks: list[str] = []
        total = 0
        for name in names:
            path = self.skill_path(name)
            if not path.exists():
                errors.append(f"skill not found: {name}")
                continue
            text = path.read_text(encoding="utf-8").strip()
            total += len(text.encode("utf-8"))
            if total > self.max_bytes:
                errors.append(f"loaded skills exceed {self.max_bytes} bytes")
                break
            blocks.append(f'## Skill: {name}\n\n{text}')
        return "\n\n".join(blocks), errors

    def list_summaries(self) -> list[dict[str, str]]:
        summaries: list[dict[str, str]] = []
        for path in sorted(self.skills_dir.glob("*/SKILL.md")):
            try:
                name = self.validate_name(path.parent.name)
            except ValueError:
                continue
            try:
                text = path.read_text(encoding="utf-8")
            except OSError:
                continue
            summaries.append(
                {
                    "name": name,
                    "description": self._description_from_text(text),
                }
            )
        return summaries

    def skill_exists(self, name: str) -> bool:
        try:
            return self.skill_path(name).exists()
        except ValueError:
            return False

    def skill_path(self, name: str) -> Path:
        name = self.validate_name(name)
        return self.skills_dir / name / "SKILL.md"

    def validate_name(self, name: str) -> str:
        normalized = name.strip().lower()
        if not SKILL_NAME_RE.fullmatch(normalized):
            raise ValueError(f"invalid skill name: {name!r}")
        return normalized

    def _skill_text(self, name: str, description: str, content: str) -> str:
        body = content
        if body.startswith("---"):
            return body.rstrip() + "\n"
        return (
            "---\n"
            f"name: {name}\n"
            f"description: {description}\n"
            "---\n\n"
            f"{body}\n"
        )

    def _description_from_text(self, text: str) -> str:
        lines = text.splitlines()
        if not lines or lines[0].strip() != "---":
            return ""
        for line in lines[1:]:
            stripped = line.strip()
            if stripped == "---":
                break
            if stripped.startswith("description:"):
                return stripped.removeprefix("description:").strip().strip("\"'")
        return ""

    def _rejected(self, action: str, name: str, reason: str) -> dict[str, Any]:
        return {
            "action": action,
            "name": name,
            "status": "rejected",
            "reason": reason,
        }

    def _write_text_atomic(self, path: Path, text: str) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(text, encoding="utf-8")
        tmp.replace(path)
