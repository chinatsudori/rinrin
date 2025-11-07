from __future__ import annotations

import asyncio
import logging
import time
from typing import Dict, Iterable, List, Optional, Tuple

import discord

from ..models import settings

IGNORED_USER_IDS: set[int] = {
    1211781489931452447,  # Wordle
}

log = logging.getLogger(__name__)


def channel_from_id(
    guild: discord.Guild, channel_id: Optional[int]
) -> Optional[discord.abc.GuildChannel]:
    if not channel_id:
        return None
    return guild.get_channel(channel_id)


class BotLogCache:
    def __init__(self, ttl_seconds: float = 60.0):
        self.ttl = ttl_seconds
        self._store: Dict[int, Tuple[Optional[int], float]] = {}

    def get_channel_id(self, guild_id: int) -> Optional[int]:
        now = time.monotonic()
        cached = self._store.get(guild_id)
        if cached and now - cached[1] < self.ttl:
            return cached[0]
        try:
            channel_id = settings.get_bot_logs_channel(guild_id)
            self._store[guild_id] = (channel_id, now)
            return channel_id
        except Exception:
            log.exception("botlog.lookup_failed", extra={"guild_id": guild_id})
            self._store[guild_id] = (None, now)
            return None