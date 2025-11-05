from __future__ import annotations

from typing import Iterable, Tuple

import discord

from ..strings import S


def build_series_list_embed(*, club: str, rows: Iterable[Tuple[int, str, str, str]]) -> discord.Embed:
    embed = discord.Embed(title=S("series.list.title", club=club), color=discord.Color.pink())
    for series_id, title, link, status in rows:
        embed.add_field(
            name=S("series.list.row_title", id=series_id, title=title, status=status),
            value=link or S("series.list.no_link"),
            inline=False,
        )
    return embed
