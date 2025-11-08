from __future__ import annotations

import discord

from ..strings import S


def build_role_welcome_embed(guild_name: str) -> discord.Embed:
    embed = discord.Embed(
        title=S("rolewelcome.title"),
        description=S("rolewelcome.desc"),
        color=discord.Color.green(),
    )
    embed.set_footer(text=S("rolewelcome.footer", guild=guild_name))
    return embed
