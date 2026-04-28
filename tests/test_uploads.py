from __future__ import annotations

import asyncio
from pathlib import Path

from token_zulip.models import NormalizedMessage
from token_zulip.storage import WorkspaceStorage
from token_zulip.uploads import MessageUploadProcessor, safe_filename, upload_path_from_target
from token_zulip.workspace import initialize_workspace


def _message(content: str) -> NormalizedMessage:
    return NormalizedMessage(
        realm_id="realm",
        message_id=42,
        stream_id=10,
        stream="Engineering",
        stream_slug="engineering",
        topic="Launch",
        topic_hash="topic123",
        sender_email="alice@example.com",
        sender_full_name="Alice",
        sender_id=1,
        content=content,
        timestamp=None,
        received_at="now",
        raw={},
    )


class FakeDownloader:
    def __init__(self) -> None:
        self.calls: list[tuple[str, Path, int]] = []

    async def download_upload(self, upload_path: str, destination: Path, max_bytes: int) -> dict[str, object]:
        self.calls.append((upload_path, destination, max_bytes))
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(b"file bytes")
        return {"status": "downloaded", "content_type": "image/png", "byte_size": 10}


def test_upload_path_and_filename_helpers():
    assert upload_path_from_target("/user_uploads/7/Ab/diagram.png") == "/user_uploads/7/Ab/diagram.png"
    assert upload_path_from_target("https://zulip.example/user_uploads/7/Ab/report.pdf") == (
        "/user_uploads/7/Ab/report.pdf"
    )
    assert upload_path_from_target("/not_uploads/file.txt") is None
    assert safe_filename("/user_uploads/7/Ab/My Diagram.PNG") == "my-diagram.png"
    assert safe_filename("/user_uploads/7/Ab/%E5%9B%BE.png") == "图.png"


def test_markdown_uploads_are_downloaded_and_rewritten(tmp_path):
    async def scenario() -> None:
        initialize_workspace(tmp_path)
        storage = WorkspaceStorage(tmp_path)
        downloader = FakeDownloader()
        processor = MessageUploadProcessor(
            storage=storage,
            zulip=downloader,
            codex_cwd=tmp_path,
            max_bytes=1234,
        )
        message = _message(
            "see [report](/user_uploads/7/Ab/report.pdf) and "
            "![diagram](/user_uploads/7/Ab/diagram.png); "
            "[external](https://example.com/file.pdf)"
        )

        processed = await processor.process_message(message)

        assert processed.content == (
            "see [report](records/stream-engineering-10/topic-launch-topic123/uploads/42/01-report.pdf) "
            "and ![diagram](records/stream-engineering-10/topic-launch-topic123/uploads/42/02-diagram.png); "
            "[external](https://example.com/file.pdf)"
        )
        assert [call[0] for call in downloader.calls] == [
            "/user_uploads/7/Ab/report.pdf",
            "/user_uploads/7/Ab/diagram.png",
        ]
        assert downloader.calls[0][1].read_bytes() == b"file bytes"
        assert processed.uploads[0]["rewritten_target"].endswith("/01-report.pdf")
        assert processed.uploads[1]["content_type"] == "image/png"

    asyncio.run(scenario())


def test_failed_upload_download_leaves_original_markdown(tmp_path):
    class FailingDownloader:
        async def download_upload(self, upload_path: str, destination: Path, max_bytes: int) -> dict[str, object]:
            raise RuntimeError("download failed")

    async def scenario() -> None:
        initialize_workspace(tmp_path)
        processor = MessageUploadProcessor(
            storage=WorkspaceStorage(tmp_path),
            zulip=FailingDownloader(),
            codex_cwd=tmp_path,
            max_bytes=1234,
        )
        message = _message("see [report](/user_uploads/7/Ab/report.pdf)")

        processed = await processor.process_message(message)

        assert processed.content.startswith(message.content)
        assert "Attachment download failures:" in processed.content
        assert "report.pdf: /user_uploads/7/Ab/report.pdf" in processed.content
        assert processed.uploads[0]["status"] == "failed"
        assert "download failed" in processed.uploads[0]["reason"]

    asyncio.run(scenario())
