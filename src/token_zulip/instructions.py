from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from string import Template

from .layout import WorkspaceLayout
from .models import SessionKey, safe_slug
from .workspace import GLOBAL_AGENTS_FILE, SHARED_SYSTEM_FILE, strip_markdown_comments


ROLE_SYSTEM_FILES: dict[str, tuple[str, ...]] = {
    "post": (
        GLOBAL_AGENTS_FILE,
        "references/post/system.md",
    ),
    "reflections_worker": (
        GLOBAL_AGENTS_FILE,
        "references/reflections/system.md",
    ),
    "skill_worker": (
        GLOBAL_AGENTS_FILE,
        "references/skill/system.md",
    ),
    "schedule_worker": (
        GLOBAL_AGENTS_FILE,
        "references/schedule/system.md",
    ),
    "scheduled_job": (
        GLOBAL_AGENTS_FILE,
        "references/scheduled_job/system.md",
    ),
}


@dataclass(frozen=True)
class InstructionSource:
    label: str
    path: Path | None
    content: str


class InstructionLoader:
    def __init__(self, root: Path, max_bytes: int = 96_000) -> None:
        self.root = root.expanduser().resolve()
        self.layout = WorkspaceLayout(self.root)
        self.max_bytes = max_bytes

    def compose(
        self,
        stream: str,
        topic_hash: str,
        *,
        role: str = "post",
        topic: str | None = None,
        stream_id: int | None = None,
        conversation_type: str = "stream",
        private_recipient_key: str | None = None,
        template_values: Mapping[str, object] | None = None,
    ) -> str:
        sources = self.sources(
            stream=stream,
            topic_hash=topic_hash,
            role=role,
            topic=topic,
            stream_id=stream_id,
            conversation_type=conversation_type,
            private_recipient_key=private_recipient_key,
        )
        rendered: list[str] = []
        total = 0
        substitutions = (
            {key: str(value).strip() for key, value in template_values.items()} if template_values else {}
        )
        for source in sources:
            content = source.content.strip()
            if substitutions:
                content = Template(content).safe_substitute(substitutions)
            block = f"\n\n## Source: {source.label}\n\n{content}\n"
            encoded_size = len(block.encode("utf-8"))
            if total + encoded_size > self.max_bytes:
                remaining = self.max_bytes - total
                if remaining <= 0:
                    break
                block = block.encode("utf-8")[:remaining].decode("utf-8", errors="ignore")
                rendered.append(block)
                break
            rendered.append(block)
            total += encoded_size
        return "".join(rendered).strip()

    def sources(
        self,
        stream: str,
        topic_hash: str,
        *,
        role: str = "post",
        topic: str | None = None,
        stream_id: int | None = None,
        conversation_type: str = "stream",
        private_recipient_key: str | None = None,
    ) -> list[InstructionSource]:
        if role not in ROLE_SYSTEM_FILES:
            raise ValueError(f"unknown instruction role: {role!r}")
        candidates: list[tuple[str, Path]] = [
            (SHARED_SYSTEM_FILE, self.root / SHARED_SYSTEM_FILE),
        ]
        candidates.extend((relative, self.root / relative) for relative in ROLE_SYSTEM_FILES[role])
        candidates.extend(
            self._local_candidates(stream, topic_hash, topic, stream_id, conversation_type, private_recipient_key)
        )

        sources: list[InstructionSource] = []
        for index, (label, path) in enumerate(candidates):
            if not path.exists():
                if index == 0:
                    raise FileNotFoundError(f"shared system instruction file missing: {path}")
                continue
            content = path.read_text(encoding="utf-8")
            if not strip_markdown_comments(content):
                if index == 0:
                    raise ValueError(f"shared system instruction file is empty: {path}")
                continue
            sources.append(InstructionSource(label, path, content))
        return sources

    def _local_candidates(
        self,
        stream: str,
        topic_hash: str,
        topic: str | None,
        stream_id: int | None,
        conversation_type: str,
        private_recipient_key: str | None,
    ) -> list[tuple[str, Path]]:
        key = SessionKey(
            realm_id="instructions",
            stream_id=stream_id,
            topic_hash=topic_hash,
            conversation_type=conversation_type,
            private_recipient_key=private_recipient_key,
            stream_slug=safe_slug(stream),
            topic_slug=safe_slug(topic or topic_hash),
        )
        if conversation_type == "private":
            private_path = self.layout.source_dir(key)
            return [
                (
                    f"{private_path.relative_to(self.root).as_posix()}/AGENTS.md",
                    private_path / "AGENTS.md",
                )
            ]

        stream_path = self.layout.source_dir(key)
        return [
            (
                f"{stream_path.relative_to(self.root).as_posix()}/AGENTS.md",
                stream_path / "AGENTS.md",
            ),
        ]
