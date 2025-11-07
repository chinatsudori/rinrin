from __future__ import annotations

from typing import Optional

import discord

from ..strings import S
from ..utils.welcome import ordinal


def build_welcome_embed(member: discord.Member, ordinal_str: str) -> discord.Embed:
    embed = discord.Embed(
        title=S("welcome.title"),
        description=S("welcome.desc", mention=member.mention, ordinal=ordinal_str),
        color=discord.Color.green(),
    )
    embed.timestamp = discord.utils.utcnow()
    return embed


def welcome_content(member: discord.Member) -> str:
    return S("welcome.content", mention=member.mention)