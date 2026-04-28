from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .models import SessionKey, safe_slug, scoped_conversation_dir, scoped_stream_dir
from .workspace import RUNTIME_CONTRACT_FILE, strip_markdown_comments


@dataclass(frozen=True)
class InstructionSource:
    label: str
    path: Path | None
    content: str


class InstructionLoader:
    def __init__(self, root: Path, max_bytes: int = 96_000) -> None:
        self.root = root.expanduser().resolve()
        self.max_bytes = max_bytes

    def compose(
        self,
        stream: str,
        topic_hash: str,
        *,
        topic: str | None = None,
        stream_id: int | None = None,
        conversation_type: str = "stream",
        private_user_key: str | None = None,
    ) -> str:
        sources = self.sources(
            stream=stream,
            topic_hash=topic_hash,
            topic=topic,
            stream_id=stream_id,
            conversation_type=conversation_type,
            private_user_key=private_user_key,
        )
        rendered: list[str] = []
        total = 0
        for source in sources:
            block = f"\n\n## Source: {source.label}\n\n{source.content.strip()}\n"
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
        topic: str | None = None,
        stream_id: int | None = None,
        conversation_type: str = "stream",
        private_user_key: str | None = None,
    ) -> list[InstructionSource]:
        candidates: list[tuple[str, Path]] = [
            (RUNTIME_CONTRACT_FILE, self.root / RUNTIME_CONTRACT_FILE),
            ("AGENTS.md", self.root / "AGENTS.md"),
            ("references/participation.md", self.root / "references" / "participation.md"),
            ("references/memory-policy.md", self.root / "references" / "memory-policy.md"),
            ("memory/AGENTS.md", self.root / "memory" / "AGENTS.md"),
        ]
        candidates.extend(self._local_candidates(stream, topic_hash, topic, stream_id, conversation_type, private_user_key))

        sources: list[InstructionSource] = []
        for index, (label, path) in enumerate(candidates):
            if not path.exists():
                if index == 0:
                    raise FileNotFoundError(f"runtime contract file missing: {path}")
                continue
            content = path.read_text(encoding="utf-8")
            if not strip_markdown_comments(content):
                if index == 0:
                    raise ValueError(f"runtime contract file is empty: {path}")
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
        private_user_key: str | None,
    ) -> list[tuple[str, Path]]:
        key = SessionKey(
            realm_id="instructions",
            stream_id=stream_id,
            topic_hash=topic_hash,
            conversation_type=conversation_type,
            private_user_key=private_user_key,
            stream_slug=safe_slug(stream),
            topic_slug=safe_slug(topic or topic_hash),
        )
        if conversation_type == "private":
            private_path = scoped_conversation_dir(self.root / "memory", key)
            return [
                (
                    f"{private_path.relative_to(self.root).as_posix()}/AGENTS.md",
                    private_path / "AGENTS.md",
                )
            ]

        stream_path = scoped_stream_dir(self.root / "memory", key)
        topic_path = scoped_conversation_dir(self.root / "memory", key)
        return [
            (
                f"{stream_path.relative_to(self.root).as_posix()}/AGENTS.md",
                stream_path / "AGENTS.md",
            ),
            (
                f"{topic_path.relative_to(self.root).as_posix()}/AGENTS.md",
                topic_path / "AGENTS.md",
            ),
        ]
