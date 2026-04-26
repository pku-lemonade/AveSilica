from __future__ import annotations

import asyncio
import html
import logging
import re
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Any, Callable

from .config import BotConfig
from .models import NormalizedMessage, normalized_topic_hash, private_user_key, safe_slug, utc_now_iso

LOGGER = logging.getLogger(__name__)


class _HTMLToText(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"br", "p", "li", "div"}:
            self.parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "li", "div"}:
            self.parts.append("\n")

    def text(self) -> str:
        value = html.unescape("".join(self.parts))
        value = re.sub(r"[ \t]+", " ", value)
        value = re.sub(r"\n\s*\n+", "\n", value)
        return value.strip()


def html_to_text(value: str) -> str:
    parser = _HTMLToText()
    parser.feed(value or "")
    return parser.text()


def normalize_zulip_event(event: dict[str, Any], realm_id: str) -> NormalizedMessage | None:
    if event.get("type") != "message":
        return None

    message = event.get("message")
    if not isinstance(message, dict):
        return None

    message_type = str(message.get("type") or "")
    if message_type in {"stream", "channel"}:
        return _normalize_stream_message(event, message, realm_id)
    if message_type == "private":
        return _normalize_private_message(event, message, realm_id)
    return None


def _normalize_stream_message(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
) -> NormalizedMessage | None:
    stream_id = message.get("stream_id")
    if stream_id is None:
        return None

    stream = _stream_name(message)
    topic = str(message.get("subject") or message.get("topic") or "")
    common = _common_message_fields(event, message, realm_id)
    if common is None:
        return None
    content, message_id, resolved_realm_id = common

    return NormalizedMessage(
        realm_id=resolved_realm_id,
        message_id=message_id,
        stream_id=int(stream_id),
        stream=stream,
        stream_slug=safe_slug(stream),
        topic=topic,
        topic_hash=normalized_topic_hash(topic),
        sender_email=str(message.get("sender_email") or ""),
        sender_full_name=str(message.get("sender_full_name") or ""),
        sender_id=_optional_int(message.get("sender_id")),
        content=content,
        timestamp=message.get("timestamp"),
        received_at=utc_now_iso(),
        raw=event,
    )


def _normalize_private_message(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
) -> NormalizedMessage | None:
    recipient = message.get("display_recipient")
    if not isinstance(recipient, list) or len(recipient) != 2:
        return None

    common = _common_message_fields(event, message, realm_id)
    if common is None:
        return None
    content, message_id, resolved_realm_id = common

    sender_email = str(message.get("sender_email") or "")
    sender_id = _optional_int(message.get("sender_id"))
    user_key = private_user_key(sender_id, sender_email)
    topic = str(message.get("subject") or message.get("topic") or "private")

    return NormalizedMessage(
        realm_id=resolved_realm_id,
        message_id=message_id,
        stream_id=None,
        stream="private",
        stream_slug="private",
        topic=topic,
        topic_hash=user_key,
        conversation_type="private",
        private_user_key=user_key,
        reply_required=True,
        sender_email=sender_email,
        sender_full_name=str(message.get("sender_full_name") or ""),
        sender_id=sender_id,
        content=content,
        timestamp=message.get("timestamp"),
        received_at=utc_now_iso(),
        raw=event,
    )


def _common_message_fields(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
) -> tuple[str, int, str] | None:
    message_id = message.get("id")
    if message_id is None:
        return None

    content_html = str(message.get("content") or "")
    content = html_to_text(content_html) or content_html
    resolved_realm_id = str(event.get("realm_id") or message.get("realm_id") or realm_id or "unknown")
    return content, int(message_id), resolved_realm_id


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _stream_name(message: dict[str, Any]) -> str:
    recipient = message.get("display_recipient")
    if isinstance(recipient, str):
        return recipient
    return str(message.get("stream") or message.get("stream_name") or "unknown")


@dataclass
class ZulipPostResult:
    request: dict[str, Any]
    response: dict[str, Any]

    def to_record(self) -> dict[str, Any]:
        return {"request": self.request, "response": self.response}


class ZulipClientIO:
    def __init__(self, client: Any) -> None:
        self.client = client

    @classmethod
    def from_config(cls, config: BotConfig) -> "ZulipClientIO":
        try:
            import zulip  # type: ignore[import-not-found]
        except ImportError as exc:
            raise RuntimeError("Install the Zulip Python package with `pip install zulip`.") from exc

        kwargs: dict[str, Any] = {}
        if config.zulip_config_file is not None:
            kwargs["config_file"] = str(config.zulip_config_file)
        return cls(zulip.Client(**kwargs))

    def bot_email(self) -> str | None:
        try:
            profile = self.client.get_profile()
        except Exception:
            LOGGER.exception("Unable to fetch Zulip profile")
            return None
        if isinstance(profile, dict):
            return profile.get("email")
        return None

    def realm_id(self) -> str | None:
        for method_name in ["get_server_settings", "get_profile"]:
            method = getattr(self.client, method_name, None)
            if method is None:
                continue
            try:
                data = method()
            except Exception:
                continue
            if isinstance(data, dict) and data.get("realm_id") is not None:
                return str(data["realm_id"])
        return None

    async def post_reply(self, message: NormalizedMessage, content: str) -> dict[str, Any]:
        if message.conversation_type == "private":
            if not message.sender_email:
                raise RuntimeError("Cannot reply to private Zulip message without sender_email")
            request = {
                "type": "private",
                "to": [message.sender_email],
                "content": content,
            }
        else:
            request = {
                "type": "stream",
                "to": message.stream,
                "topic": message.topic,
                "content": content,
            }
        response = await asyncio.to_thread(self.client.send_message, request)
        if not isinstance(response, dict):
            response = {"result": "unknown", "raw": response}
        if response.get("result") not in {None, "success"}:
            raise RuntimeError(f"Zulip send_message failed: {response!r}")
        return ZulipPostResult(request=request, response=response).to_record()

    def listen(self, callback: Callable[[dict[str, Any]], None], *, all_public_streams: bool = False) -> None:
        self.client.call_on_each_event(
            callback,
            event_types=["message"],
            all_public_streams=all_public_streams,
        )
