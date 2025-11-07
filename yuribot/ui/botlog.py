from __future__ import annotations

from datetime import datetime
from typing import Iterable, Optional

import discord

from ..config import LOCAL_TZ
from ..strings import S


def build_embed(title_key: str, color: discord.Color) -> discord.Embed:
    return discord.Embed(
        title=S(title_key),
        color=color,
        timestamp=datetime.now(tz=LOCAL_TZ),
    )


def safe_add_field(
    emb: discord.Embed,
    *,
    name_key: str,
    value: Optional[str],
    inline: bool,
) -> None:
    if not value:
        return
    emb.add_field(name=S(name_key), value=value[:1024], inline=inline)


def format_roles(roles: Iterable[discord.Role]) -> str:
    items = [r.mention for r in roles if not r.is_default()]
    if not items:
        return S("botlog.common.none")
    return ", ".join(items)


def channel_reference(ch: Optional[discord.abc.GuildChannel]) -> str:
    if ch is None:
        return S("botlog.common.unknown")
    if isinstance(
        ch,
        (
            discord.TextChannel,
            discord.VoiceChannel,
            discord.StageChannel,
            discord.ForumChannel,
        ),
    ):
        return f"{ch.mention} (`{ch.id}`)"
    return f"{getattr(ch, 'name', 'channel')} (`{ch.id}`)"