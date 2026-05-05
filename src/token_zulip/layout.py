from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .models import SessionKey, scoped_conversation_dir, scoped_private_dir, scoped_stream_dir


REALM_DIRNAME = "realm"
RUNTIME_DIRNAME = "runtime"
AGENTS_FILENAME = "AGENTS.md"
REFLECTIONS_FILENAME = "REFLECTIONS.md"
CODEX_STATS_DIRNAME = "codex_stats"
TRACES_DIRNAME = "traces"


@dataclass(frozen=True)
class WorkspaceLayout:
    workspace_dir: Path

    def __post_init__(self) -> None:
        object.__setattr__(self, "workspace_dir", self.workspace_dir.expanduser().resolve())

    @property
    def realm_dir(self) -> Path:
        return self.workspace_dir / REALM_DIRNAME

    @property
    def runtime_dir(self) -> Path:
        return self.realm_dir / RUNTIME_DIRNAME

    @property
    def errors_dir(self) -> Path:
        return self.runtime_dir / "errors"

    @property
    def codex_stats_dir(self) -> Path:
        return self.runtime_dir / CODEX_STATS_DIRNAME

    @property
    def scheduled_runs_dir(self) -> Path:
        return self.runtime_dir / "scheduled"

    @property
    def global_agents_path(self) -> Path:
        return self.realm_dir / AGENTS_FILENAME

    @property
    def global_reflections_path(self) -> Path:
        return self.realm_dir / REFLECTIONS_FILENAME

    def source_dir(self, key: SessionKey) -> Path:
        if key.conversation_type == "private":
            return scoped_private_dir(self.realm_dir, key)
        return scoped_stream_dir(self.realm_dir, key)

    def session_dir(self, key: SessionKey) -> Path:
        if key.conversation_type == "private":
            return self.source_dir(key)
        return scoped_conversation_dir(self.realm_dir, key, readable_topic=True)

    def source_agents_path(self, key: SessionKey) -> Path:
        return self.source_dir(key) / AGENTS_FILENAME

    def source_reflections_path(self, key: SessionKey) -> Path:
        return self.source_dir(key) / REFLECTIONS_FILENAME

    def relative(self, path: Path) -> str:
        return path.relative_to(self.workspace_dir).as_posix()
