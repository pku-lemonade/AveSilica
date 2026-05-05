from __future__ import annotations

import asyncio
import html
import logging
import re
import urllib.parse
import urllib.request
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Callable, Sequence

from .addressing import is_directly_addressed
from .config import BotConfig
from .models import (
    NormalizedMessage,
    NormalizedMessageMove,
    NormalizedReaction,
    normalized_topic_hash,
    safe_slug,
    utc_now_iso,
)

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


def normalize_zulip_event(
    event: dict[str, Any],
    realm_id: str,
    *,
    bot_user_id: int | None = None,
    bot_email: str | None = None,
    bot_aliases: Sequence[str] = (),
) -> NormalizedMessage | None:
    if event.get("type") != "message":
        return None

    message = event.get("message")
    if not isinstance(message, dict):
        return None

    message_type = str(message.get("type") or "")
    if message_type in {"stream", "channel"}:
        return _normalize_stream_message(
            event,
            message,
            realm_id,
            bot_user_id=bot_user_id,
            bot_aliases=bot_aliases,
        )
    if message_type == "private":
        return _normalize_private_message(
            event,
            message,
            realm_id,
            bot_user_id=bot_user_id,
            bot_email=bot_email,
        )
    return None


def normalize_zulip_reaction_event(
    event: dict[str, Any],
    realm_id: str,
) -> NormalizedReaction | None:
    if event.get("type") != "reaction":
        return None

    op = str(event.get("op") or "")
    if op not in {"add", "remove"}:
        return None

    message_id = _optional_int(event.get("message_id"))
    if message_id is None:
        return None

    emoji_name = str(event.get("emoji_name") or "")
    if not emoji_name:
        return None

    user = event.get("user") if isinstance(event.get("user"), dict) else {}
    user_id = _optional_int(event.get("user_id") or user.get("id"))
    user_email = str(event.get("user_email") or user.get("email") or "")
    user_full_name = str(event.get("user_full_name") or user.get("full_name") or "")
    resolved_realm_id = str(event.get("realm_id") or realm_id or "unknown")

    return NormalizedReaction(
        realm_id=resolved_realm_id,
        message_id=message_id,
        op=op,
        emoji_name=emoji_name,
        emoji_code=str(event.get("emoji_code") or ""),
        reaction_type=str(event.get("reaction_type") or ""),
        user_id=user_id,
        user_email=user_email,
        user_full_name=user_full_name,
        timestamp=_optional_int(event.get("timestamp")),
        received_at=utc_now_iso(),
        raw=event,
    )


def normalize_zulip_update_message_event(
    event: dict[str, Any],
    realm_id: str,
) -> NormalizedMessageMove | None:
    if event.get("type") != "update_message":
        return None

    stream_id = _optional_int(event.get("stream_id"))
    if stream_id is None:
        return None

    orig_subject = str(event.get("orig_subject") or "")
    if not orig_subject:
        return None

    new_stream_id = _optional_int(event.get("new_stream_id")) or stream_id
    subject = str(event.get("subject") or orig_subject)
    if subject == orig_subject and new_stream_id == stream_id:
        return None

    message_ids = _message_ids_for_update_event(event)
    if not message_ids:
        return None

    return NormalizedMessageMove(
        realm_id=str(event.get("realm_id") or realm_id or "unknown"),
        message_id=message_ids[0],
        message_ids=message_ids,
        stream_id=stream_id,
        stream_name=str(event.get("stream_name") or event.get("stream") or "unknown"),
        orig_subject=orig_subject,
        new_stream_id=new_stream_id,
        subject=subject,
        propagate_mode=str(event.get("propagate_mode") or "change_one"),
        raw=event,
    )


def _message_ids_for_update_event(event: dict[str, Any]) -> list[int]:
    raw_ids = event.get("message_ids")
    values = raw_ids if isinstance(raw_ids, list) else [event.get("message_id")]
    ids: list[int] = []
    for value in values:
        message_id = _optional_int(value)
        if message_id is not None and message_id not in ids:
            ids.append(message_id)
    return ids


def _normalize_stream_message(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
    *,
    bot_user_id: int | None,
    bot_aliases: Sequence[str],
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
        directly_addressed=is_directly_addressed(
            event,
            message,
            content,
            bot_user_id=bot_user_id,
            bot_aliases=bot_aliases,
        ),
    )


def _normalize_private_message(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
    *,
    bot_user_id: int | None,
    bot_email: str | None,
) -> NormalizedMessage | None:
    recipient = message.get("display_recipient")
    if not isinstance(recipient, list) or len(recipient) < 2:
        return None

    recipient_id = message.get("recipient_id")
    if recipient_id is None:
        return None

    common = _common_message_fields(event, message, realm_id)
    if common is None:
        return None
    content, message_id, resolved_realm_id = common

    sender_email = str(message.get("sender_email") or "")
    sender_id = _optional_int(message.get("sender_id"))
    recipient_key = str(recipient_id).strip()
    if not recipient_key:
        return None
    private_recipients = _private_delivery_recipients(
        recipient,
        message,
        bot_user_id=bot_user_id,
        bot_email=bot_email,
    )
    if not private_recipients:
        return None
    topic = str(message.get("subject") or message.get("topic") or "private")

    return NormalizedMessage(
        realm_id=resolved_realm_id,
        message_id=message_id,
        stream_id=None,
        stream="private",
        stream_slug="private",
        topic=topic,
        topic_hash=recipient_key,
        conversation_type="private",
        private_recipient_key=recipient_key,
        private_recipients=private_recipients,
        post_required=True,
        sender_email=sender_email,
        sender_full_name=str(message.get("sender_full_name") or ""),
        sender_id=sender_id,
        content=content,
        timestamp=message.get("timestamp"),
        received_at=utc_now_iso(),
        raw=event,
    )


def _private_delivery_recipients(
    display_recipient: list[object],
    message: dict[str, Any],
    *,
    bot_user_id: int | None,
    bot_email: str | None,
) -> list[dict[str, Any]]:
    recipients: list[dict[str, Any]] = []

    def add_user(raw: dict[str, Any]) -> None:
        user_id = _optional_int(raw.get("id") or raw.get("user_id"))
        email = str(raw.get("email") or "").strip()
        full_name = str(raw.get("full_name") or "").strip()
        if user_id is None and not email:
            return
        if bot_user_id is not None and user_id == bot_user_id:
            return
        if bot_email and email.casefold() == bot_email.casefold():
            return
        key = ("id", str(user_id)) if user_id is not None else ("email", email.casefold())
        for existing in recipients:
            existing_key = (
                ("id", str(existing.get("user_id")))
                if existing.get("user_id") is not None
                else ("email", str(existing.get("email") or "").casefold())
            )
            if existing_key == key:
                return
        recipients.append(
            {
                "user_id": user_id,
                "email": email,
                "full_name": full_name,
            }
        )

    for user in display_recipient:
        if isinstance(user, dict):
            add_user(user)

    add_user(
        {
            "id": message.get("sender_id"),
            "email": message.get("sender_email"),
            "full_name": message.get("sender_full_name"),
        }
    )
    return recipients


def _common_message_fields(
    event: dict[str, Any],
    message: dict[str, Any],
    realm_id: str,
) -> tuple[str, int, str] | None:
    message_id = message.get("id")
    if message_id is None:
        return None

    content_raw = str(message.get("content") or "")
    if message.get("content_type") == "text/html" or _looks_like_rendered_html(content_raw):
        content = html_to_text(content_raw) or content_raw
    else:
        content = content_raw
    resolved_realm_id = str(event.get("realm_id") or message.get("realm_id") or realm_id or "unknown")
    return content, int(message_id), resolved_realm_id


def _looks_like_rendered_html(value: str) -> bool:
    return re.search(r"<(?:p|br|div|span|a|ul|ol|li|blockquote|pre|code|img)\b", value, re.IGNORECASE) is not None


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


@dataclass(frozen=True)
class ZulipBotProfile:
    email: str | None = None
    user_id: int | None = None
    full_name: str | None = None
    realm_id: str | None = None


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

    def bot_profile(self) -> ZulipBotProfile:
        try:
            profile = self.client.get_profile()
        except Exception:
            LOGGER.exception("Unable to fetch Zulip profile")
            return ZulipBotProfile()
        if isinstance(profile, dict):
            user_id = _optional_int(profile.get("user_id") or profile.get("id"))
            return ZulipBotProfile(
                email=profile.get("email"),
                user_id=user_id,
                full_name=profile.get("full_name"),
                realm_id=str(profile["realm_id"]) if profile.get("realm_id") is not None else None,
            )
        return ZulipBotProfile()

    def bot_email(self) -> str | None:
        return self.bot_profile().email

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

    async def post_message(self, message: NormalizedMessage, content: str) -> dict[str, Any]:
        if message.conversation_type == "private":
            recipients = _private_send_recipients(message)
            if not recipients:
                raise RuntimeError("Cannot post to private Zulip message without recipients")
            request = {
                "type": "private",
                "to": recipients,
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

    async def download_upload(self, upload_path: str, destination: Path, max_bytes: int) -> dict[str, Any]:
        return await asyncio.to_thread(self._download_upload_sync, upload_path, destination, max_bytes)

    def _download_upload_sync(self, upload_path: str, destination: Path, max_bytes: int) -> dict[str, Any]:
        request_path = upload_path.lstrip("/")
        response = self.client.call_endpoint(url=request_path, method="GET")
        if not isinstance(response, dict):
            raise RuntimeError(f"Zulip upload URL request returned non-dict response: {response!r}")
        if response.get("result") not in {None, "success"}:
            raise RuntimeError(f"Zulip upload URL request failed: {response!r}")
        url = response.get("url")
        if not isinstance(url, str) or not url:
            raise RuntimeError(f"Zulip upload URL response missing url: {response!r}")
        return self._download_temporary_url(self._absolute_zulip_url(url), destination, max_bytes)

    def _download_temporary_url(self, url: str, destination: Path, max_bytes: int) -> dict[str, Any]:
        destination.parent.mkdir(parents=True, exist_ok=True)
        tmp = destination.with_name(destination.name + ".tmp")
        total = 0
        try:
            with urllib.request.urlopen(url, timeout=30) as response:  # noqa: S310
                content_length = response.headers.get("Content-Length")
                if content_length is not None and int(content_length) > max_bytes:
                    raise RuntimeError(f"upload exceeds maximum size ({content_length} > {max_bytes} bytes)")
                content_type = response.headers.get_content_type()
                with tmp.open("wb") as handle:
                    while True:
                        chunk = response.read(1024 * 256)
                        if not chunk:
                            break
                        total += len(chunk)
                        if total > max_bytes:
                            raise RuntimeError(f"upload exceeds maximum size ({total} > {max_bytes} bytes)")
                        handle.write(chunk)
            tmp.replace(destination)
        except Exception:
            if tmp.exists():
                tmp.unlink()
            raise
        return {
            "status": "downloaded",
            "content_type": content_type,
            "byte_size": total,
        }

    def _absolute_zulip_url(self, url: str) -> str:
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme and parsed.netloc:
            return url
        base_url = str(getattr(self.client, "base_url", ""))
        site_url = base_url.split("/api/", 1)[0] if "/api/" in base_url else base_url
        return urllib.parse.urljoin(site_url.rstrip("/") + "/", url)

    def listen(self, callback: Callable[[dict[str, Any]], None], *, all_public_streams: bool = False) -> None:
        self.client.call_on_each_event(
            callback,
            event_types=["message", "reaction", "update_message"],
            all_public_streams=all_public_streams,
            apply_markdown=False,
        )


class ZulipTypingNotifier:
    def __init__(self, client: Any) -> None:
        self.client = client

    async def start(self, message: NormalizedMessage) -> None:
        await self._set_typing(message, "start")

    async def stop(self, message: NormalizedMessage) -> None:
        await self._set_typing(message, "stop")

    async def _set_typing(self, message: NormalizedMessage, op: str) -> None:
        request = self._request(message, op)
        if request is None:
            return
        response = await asyncio.to_thread(self.client.set_typing_status, request)
        if not isinstance(response, dict):
            response = {"result": "unknown", "raw": response}
        if response.get("result") not in {None, "success"}:
            raise RuntimeError(f"Zulip set_typing_status failed: {response!r}")

    def _request(self, message: NormalizedMessage, op: str) -> dict[str, Any] | None:
        if message.conversation_type == "private":
            recipient_ids = _private_typing_recipient_ids(message)
            if not recipient_ids:
                return None
            return {
                "type": "direct",
                "op": op,
                "to": recipient_ids,
            }
        if message.stream_id is None:
            return None
        return {
            "type": "stream",
            "op": op,
            "stream_id": message.stream_id,
            "topic": message.topic,
        }


def _private_send_recipients(message: NormalizedMessage) -> list[int | str]:
    recipients: list[int | str] = []
    for recipient in message.private_recipients:
        user_id = _optional_int(recipient.get("user_id"))
        email = str(recipient.get("email") or "").strip()
        value: int | str | None = user_id if user_id is not None else email or None
        if value is not None and value not in recipients:
            recipients.append(value)
    return recipients


def _private_typing_recipient_ids(message: NormalizedMessage) -> list[int]:
    recipient_ids: list[int] = []
    for recipient in message.private_recipients:
        user_id = _optional_int(recipient.get("user_id"))
        if user_id is not None and user_id not in recipient_ids:
            recipient_ids.append(user_id)
    return recipient_ids
