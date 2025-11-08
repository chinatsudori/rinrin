from __future__ import annotations

import discord

from ..strings import S


def build_dm_embed(
    *, guild_name: str, reason: str, until_timestamp: int, duration_display: str
) -> discord.Embed:
    embed = discord.Embed(
        title=S("timeout.dm.title", guild=guild_name),
        description=reason,
        color=discord.Color.orange(),
    )
    embed.add_field(
        name=S("timeout.dm.field.duration"),
        value=duration_display,
        inline=True,
    )
    embed.add_field(
        name=S("timeout.dm.field.until"),
        value=f"<t:{until_timestamp}:F>",
        inline=True,
    )
    return embed
