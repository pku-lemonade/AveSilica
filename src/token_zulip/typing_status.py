from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator, Protocol

from .models import NormalizedMessage

LOGGER = logging.getLogger(__name__)


class TypingNotifier(Protocol):
    async def start(self, message: NormalizedMessage) -> None:
        ...

    async def stop(self, message: NormalizedMessage) -> None:
        ...


class NoOpTypingNotifier:
    async def start(self, message: NormalizedMessage) -> None:
        return None

    async def stop(self, message: NormalizedMessage) -> None:
        return None


class TypingStatusManager:
    def __init__(
        self,
        notifier: TypingNotifier | None = None,
        *,
        enabled: bool = True,
        refresh_seconds: float = 8.0,
    ) -> None:
        self.notifier = notifier or NoOpTypingNotifier()
        self.enabled = enabled
        self.refresh_seconds = max(0.1, refresh_seconds)

    def should_show_typing(self, _message: NormalizedMessage, *, posting_enabled: bool) -> bool:
        if not self.enabled or not posting_enabled:
            return False
        return True

    @asynccontextmanager
    async def active(self, message: NormalizedMessage) -> AsyncIterator[None]:
        refresh_task: asyncio.Task[None] | None = None
        stop_event = asyncio.Event()
        await self._safe_start(message)
        refresh_task = asyncio.create_task(self._refresh(message, stop_event))
        try:
            yield
        finally:
            stop_event.set()
            if refresh_task is not None:
                await refresh_task
            await self._safe_stop(message)

    async def _refresh(self, message: NormalizedMessage, stop_event: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=self.refresh_seconds)
                return
            except asyncio.TimeoutError:
                pass
            await self._safe_start(message)

    async def _safe_start(self, message: NormalizedMessage) -> None:
        try:
            await self.notifier.start(message)
        except Exception:
            LOGGER.warning("Unable to start Zulip typing status for message %s", message.message_id, exc_info=True)

    async def _safe_stop(self, message: NormalizedMessage) -> None:
        try:
            await self.notifier.stop(message)
        except Exception:
            LOGGER.warning("Unable to stop Zulip typing status for message %s", message.message_id, exc_info=True)
