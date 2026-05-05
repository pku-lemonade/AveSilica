from __future__ import annotations

import json
import shutil
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import (
    SessionKey,
    safe_slug,
    scoped_conversation_dir,
    scoped_private_dir,
    scoped_stream_dir,
)


REALM_DIRNAME = "realm"
RUNTIME_DIRNAME = "runtime"
AGENTS_FILENAME = "AGENTS.md"
REFLECTIONS_FILENAME = "REFLECTIONS.md"
CODEX_STATS_DIRNAME = "codex_stats"
TRACES_DIRNAME = "traces"

LEGACY_RECORDS_DIRNAME = "records"
LEGACY_INSTRUCTIONS_DIRNAME = "instructions"
LEGACY_REFLECTIONS_DIRNAME = "reflections"


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
    def scheduled_records_dir(self) -> Path:
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


def migrate_legacy_workspace(workspace_dir: Path) -> dict[str, Any]:
    workspace = workspace_dir.expanduser().resolve()
    layout = WorkspaceLayout(workspace)
    summary: dict[str, Any] = {"migrated": False, "paths": []}

    def note(path: Path) -> None:
        summary["migrated"] = True
        summary["paths"].append(path.relative_to(workspace).as_posix())

    layout.realm_dir.mkdir(parents=True, exist_ok=True)
    layout.runtime_dir.mkdir(parents=True, exist_ok=True)

    root_agents = workspace / AGENTS_FILENAME
    if root_agents.exists():
        _merge_markdown_file(root_agents, layout.global_agents_path)
        note(layout.global_agents_path)

    legacy_reflections = workspace / LEGACY_REFLECTIONS_DIRNAME
    if legacy_reflections.exists():
        global_reflections = legacy_reflections / REFLECTIONS_FILENAME
        if global_reflections.exists():
            _merge_markdown_file(global_reflections, layout.global_reflections_path)
            note(layout.global_reflections_path)
        for source in sorted(legacy_reflections.iterdir()):
            if not source.is_dir():
                continue
            if source.name.startswith(("stream-", "private-recipient-")):
                reflection_file = source / REFLECTIONS_FILENAME
                if reflection_file.exists():
                    destination = layout.realm_dir / source.name / REFLECTIONS_FILENAME
                    _merge_markdown_file(reflection_file, destination)
                    note(destination)
        shutil.rmtree(legacy_reflections)

    legacy_instructions = workspace / LEGACY_INSTRUCTIONS_DIRNAME
    if legacy_instructions.exists():
        for source in sorted(legacy_instructions.iterdir()):
            if not source.is_dir():
                continue
            if source.name.startswith(("stream-", "private-recipient-")):
                agents_file = source / AGENTS_FILENAME
                if agents_file.exists():
                    destination = layout.realm_dir / source.name / AGENTS_FILENAME
                    _merge_markdown_file(agents_file, destination)
                    note(destination)
        shutil.rmtree(legacy_instructions)

    legacy_records = workspace / LEGACY_RECORDS_DIRNAME
    if legacy_records.exists():
        for name, destination in [
            ("errors", layout.errors_dir),
            (CODEX_STATS_DIRNAME, layout.codex_stats_dir),
            ("scheduled", layout.scheduled_records_dir),
        ]:
            source = legacy_records / name
            if source.exists():
                _merge_tree(source, destination, workspace=workspace, old_base=source, new_base=destination)
                note(destination)

        for source in sorted(legacy_records.iterdir()):
            if not source.is_dir() or not source.name.startswith(("stream-", "private-recipient-")):
                continue
            destination = layout.realm_dir / source.name
            _merge_tree(source, destination, workspace=workspace, old_base=source, new_base=destination)
            _rewrite_message_records(destination, workspace=workspace, old_base=source, new_base=destination)
            note(destination)
        shutil.rmtree(legacy_records)

    return summary


def _merge_tree(source: Path, destination: Path, *, workspace: Path, old_base: Path, new_base: Path) -> None:
    if not source.exists():
        return
    if source.is_file():
        _merge_file(source, destination, workspace=workspace, old_base=old_base, new_base=new_base)
        return
    destination.mkdir(parents=True, exist_ok=True)
    for child in sorted(source.iterdir()):
        target = destination / child.name
        if child.is_dir():
            _merge_tree(child, target, workspace=workspace, old_base=old_base, new_base=new_base)
        else:
            _merge_file(child, target, workspace=workspace, old_base=old_base, new_base=new_base)
    _remove_empty_parents(source, stop=old_base.parent)


def _merge_file(source: Path, destination: Path, *, workspace: Path, old_base: Path, new_base: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    if source.name == "messages.jsonl":
        records = [
            _rewrite_record(record, workspace=workspace, old_base=old_base, new_base=new_base)
            for record in _read_jsonl(source)
        ]
        _merge_message_records(destination, records)
        source.unlink()
        return
    if source.name in {AGENTS_FILENAME, REFLECTIONS_FILENAME}:
        _merge_markdown_file(source, destination)
        return
    if not destination.exists():
        source.rename(destination)
        return
    if source.read_bytes() == destination.read_bytes():
        source.unlink()
        return
    _merge_text_file(source, destination)


def _merge_markdown_file(source: Path, destination: Path) -> None:
    destination.parent.mkdir(parents=True, exist_ok=True)
    source_text = source.read_text(encoding="utf-8").strip()
    if not destination.exists():
        source.rename(destination)
        return
    destination_text = destination.read_text(encoding="utf-8").strip()
    if not source_text or source_text == destination_text:
        source.unlink()
        return
    merged = "\n\n".join(part for part in [destination_text, source_text] if part).rstrip() + "\n"
    _write_text_atomic(destination, merged)
    source.unlink()


def _merge_text_file(source: Path, destination: Path) -> None:
    existing = destination.read_text(encoding="utf-8") if destination.exists() else ""
    extra = source.read_text(encoding="utf-8")
    _write_text_atomic(destination, existing + extra)
    source.unlink()


def _merge_message_records(path: Path, records: list[dict[str, Any]]) -> None:
    merged: dict[int, dict[str, Any]] = {}
    without_id: list[dict[str, Any]] = []
    for record in [*_read_jsonl(path), *records]:
        message_id = _optional_message_id(record)
        if message_id is None:
            without_id.append(record)
        else:
            merged[message_id] = record
    _write_jsonl(path, [*without_id, *[merged[key] for key in sorted(merged)]])


def _rewrite_message_records(directory: Path, *, workspace: Path, old_base: Path, new_base: Path) -> None:
    for path in sorted(directory.glob("**/messages.jsonl")):
        records = _read_jsonl(path)
        if records:
            _write_jsonl(
                path,
                [_rewrite_record(record, workspace=workspace, old_base=old_base, new_base=new_base) for record in records],
            )


def _rewrite_record(record: dict[str, Any], *, workspace: Path, old_base: Path, new_base: Path) -> dict[str, Any]:
    rewritten = dict(record)
    if "reply_required" in rewritten:
        rewritten["post_required"] = bool(rewritten.pop("reply_required"))
    old_rel = old_base.relative_to(workspace).as_posix()
    new_rel = new_base.relative_to(workspace).as_posix()
    if isinstance(rewritten.get("content"), str):
        rewritten["content"] = rewritten["content"].replace(old_rel, new_rel)
    uploads: list[dict[str, Any]] = []
    changed_uploads = False
    for item in rewritten.get("uploads") or []:
        if not isinstance(item, dict):
            continue
        upload = dict(item)
        if isinstance(upload.get("local_path"), str):
            upload["local_path"] = upload["local_path"].replace(old_rel, new_rel)
            changed_uploads = True
        if isinstance(upload.get("rewritten_target"), str):
            upload["rewritten_target"] = upload["rewritten_target"].replace(old_rel, new_rel)
            changed_uploads = True
        uploads.append(upload)
    if uploads and changed_uploads:
        rewritten["uploads"] = uploads
    return rewritten


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records


def _write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    text = "".join(json.dumps(record, ensure_ascii=False, sort_keys=False) + "\n" for record in records)
    _write_text_atomic(path, text)


def _write_text_atomic(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def _optional_message_id(record: dict[str, Any]) -> int | None:
    try:
        return int(record["message_id"])
    except (KeyError, TypeError, ValueError):
        return None


def _remove_empty_parents(path: Path, *, stop: Path) -> None:
    current = path
    while current.exists() and current != stop:
        try:
            current.rmdir()
        except OSError:
            return
        current = current.parent
