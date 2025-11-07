from __future__ import annotations

from typing import Iterable, Tuple

import discord

from ..strings import S


def build_club_config_embed(*, guild: discord.Guild, club_pairs: Iterable[Tuple[str, str]]) -> discord.Embed:
    embed = discord.Embed(
        title=S("admin.club_config.title", guild=guild.name),
        color=discord.Color.blue(),
    )
    lines = [f"**{label}:** {value}" for label, value in club_pairs]
    embed.description = "\n".join(lines) if lines else S("admin.club_config.empty")
    return embed