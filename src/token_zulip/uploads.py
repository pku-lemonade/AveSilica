from __future__ import annotations

import re
import urllib.parse
from dataclasses import dataclass, replace
from pathlib import Path, PurePosixPath
from typing import Any

from markdown_it import MarkdownIt
from markdown_it.token import Token
from mdformat.renderer import MDRenderer

from .models import NormalizedMessage, safe_slug
from .storage import WorkspaceStorage


UPLOAD_PATH_PREFIX = "/user_uploads/"


@dataclass(frozen=True)
class UploadTarget:
    original_target: str
    upload_path: str
    filename: str


class MessageUploadProcessor:
    def __init__(
        self,
        *,
        storage: WorkspaceStorage,
        zulip: object,
        codex_cwd: Path,
        max_bytes: int,
    ) -> None:
        self.storage = storage
        self.zulip = zulip
        self.codex_cwd = codex_cwd.expanduser().resolve()
        self.max_bytes = max_bytes
        self.markdown = MarkdownIt("commonmark")
        self.renderer = MDRenderer()

    async def process_messages(self, messages: list[NormalizedMessage]) -> list[NormalizedMessage]:
        processed: list[NormalizedMessage] = []
        for message in messages:
            processed.append(await self.process_message(message))
        return processed

    async def process_message(self, message: NormalizedMessage) -> NormalizedMessage:
        targets = self.extract_upload_targets(message.content)
        if not targets:
            return message

        replacements: dict[str, str] = {}
        uploads: list[dict[str, Any]] = []
        session_dir = self.storage.session_dir(message.session_key)
        downloader = getattr(self.zulip, "download_upload", None)

        for index, target in enumerate(targets, start=1):
            filename = f"{index:02d}-{target.filename}"
            local_path = session_dir / "uploads" / str(message.message_id) / filename
            prompt_path = self._prompt_path(local_path)
            record: dict[str, Any] = {
                "status": "pending",
                "original_target": target.original_target,
                "upload_path": target.upload_path,
                "filename": target.filename,
                "local_path": str(local_path),
                "rewritten_target": prompt_path,
            }
            if downloader is None:
                record.update(
                    {
                        "status": "failed",
                        "reason": "zulip client does not support upload downloads",
                    }
                )
                uploads.append(record)
                continue

            try:
                result = await downloader(target.upload_path, local_path, self.max_bytes)
            except Exception as exc:  # Keep the turn alive; the prompt still has the original link.
                record.update({"status": "failed", "reason": repr(exc)})
                uploads.append(record)
                continue

            record.update(result)
            record["status"] = str(result.get("status") or "downloaded")
            replacements[target.original_target] = prompt_path
            uploads.append(record)

        rewritten = self.rewrite_upload_targets(message.content, replacements)
        rewritten = self._append_failure_notes(rewritten, uploads)
        if rewritten == message.content and uploads == message.uploads:
            return message
        return self._replace_message(message, content=rewritten, uploads=uploads)

    def extract_upload_targets(self, content: str) -> list[UploadTarget]:
        targets: list[UploadTarget] = []
        seen: set[str] = set()
        for token in self._inline_children(content):
            target = self._token_target(token)
            if target is None or target in seen:
                continue
            upload_path = upload_path_from_target(target)
            if upload_path is None:
                continue
            seen.add(target)
            targets.append(
                UploadTarget(
                    original_target=target,
                    upload_path=upload_path,
                    filename=safe_filename(upload_path),
                )
            )
        return targets

    def rewrite_upload_targets(self, content: str, replacements: dict[str, str]) -> str:
        if not replacements:
            return content
        tokens = self.markdown.parse(content)
        for token in self._children(tokens):
            if token.type == "link_open":
                target = token.attrGet("href")
                if target in replacements:
                    token.attrSet("href", replacements[target])
            elif token.type == "image":
                target = token.attrGet("src")
                if target in replacements:
                    token.attrSet("src", replacements[target])
        return self.renderer.render(tokens, self.markdown.options, {}).rstrip("\n")

    def _inline_children(self, content: str) -> list[Token]:
        return self._children(self.markdown.parse(content))

    def _children(self, tokens: list[Token]) -> list[Token]:
        children: list[Token] = []
        for token in tokens:
            if token.children:
                children.extend(token.children)
        return children

    def _token_target(self, token: Token) -> str | None:
        if token.type == "link_open":
            return token.attrGet("href")
        if token.type == "image":
            return token.attrGet("src")
        return None

    def _prompt_path(self, local_path: Path) -> str:
        resolved = local_path.expanduser().resolve()
        try:
            return resolved.relative_to(self.codex_cwd).as_posix()
        except ValueError:
            return resolved.as_posix()

    def _append_failure_notes(self, content: str, uploads: list[dict[str, Any]]) -> str:
        failures = [upload for upload in uploads if upload.get("status") == "failed"]
        if not failures:
            return content
        lines = ["", "Attachment download failures:"]
        for upload in failures:
            name = upload.get("filename") or "upload"
            upload_path = upload.get("upload_path") or upload.get("original_target") or "unknown"
            reason = upload.get("reason") or "unknown error"
            lines.append(f"- {name}: {upload_path} ({reason})")
        return content.rstrip() + "\n" + "\n".join(lines)

    def _replace_message(
        self,
        message: NormalizedMessage,
        *,
        content: str,
        uploads: list[dict[str, Any]],
    ) -> NormalizedMessage:
        return replace(message, content=content, uploads=uploads)


def upload_path_from_target(target: str) -> str | None:
    parsed = urllib.parse.urlsplit(target)
    if not parsed.path.startswith(UPLOAD_PATH_PREFIX):
        return None
    upload_path = parsed.path
    if parsed.query:
        upload_path += f"?{parsed.query}"
    return upload_path


def safe_filename(upload_path: str) -> str:
    path = urllib.parse.urlsplit(upload_path).path
    original = urllib.parse.unquote(PurePosixPath(path).name).strip()
    if not original:
        return "upload"

    suffix = Path(original).suffix.lower()
    if suffix:
        stem = original[: -len(suffix)]
        safe_suffix = "." + re.sub(r"[^a-z0-9]+", "", suffix[1:])
        if safe_suffix == ".":
            safe_suffix = ""
    else:
        stem = original
        safe_suffix = ""

    safe_stem = safe_slug(stem)
    if safe_stem == "unnamed" and not stem.strip():
        safe_stem = "upload"
    return f"{safe_stem}{safe_suffix}" or "upload"
