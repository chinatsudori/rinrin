from __future__ import annotations

from typing import Iterable, Sequence, Tuple

import discord

from ..strings import S


def build_emoji_embed(
    month: str,
    rows: Sequence[Tuple[str, str, bool, bool, int]],
    *,
    limit: int,
) -> discord.Embed:
    lines = []
    for key, name, _is_custom, via_reaction, count in rows[:limit]:
        src = S("emoji.src.reaction") if via_reaction else S("emoji.src.message")
        display = name if name else key.split(":", 1)[-1]
        lines.append(S("emoji.row", display=display, count=count, src=src))
    embed = discord.Embed(
        title=S("emoji.title", month=month),
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )
    return embed


def build_sticker_embed(
    month: str,
    rows: Sequence[Tuple[str, str, int]],
    *,
    limit: int,
) -> discord.Embed:
    lines = [
        S("sticker.row", name=(name or sid), count=count)
        for sid, name, count in rows[:limit]
    ]
    embed = discord.Embed(
        title=S("sticker.title", month=month),
        description="\n".join(lines),
        color=discord.Color.green(),
    )
    return embed
