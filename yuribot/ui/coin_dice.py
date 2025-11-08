from __future__ import annotations

from typing import Iterable, Tuple

import discord

from ..strings import S


def build_coin_embed(*, heads: int, tails: int, sequence: str) -> discord.Embed:
    embed = discord.Embed(title=S("fun.coin.title"), color=discord.Color.gold())
    embed.add_field(
        name=S("fun.coin.results", heads=heads, tails=tails),
        value=S("fun.coin.sequence", seq=sequence),
        inline=False,
    )
    return embed


def build_dice_embed(
    spec_lines: Iterable[Tuple[str, str, str, int]], grand_total: int
) -> discord.Embed:
    lines = [
        S("fun.dice.rolls_line", spec=spec, rolls=rolls, mod_text=mod_text, total=total)
        for spec, rolls, mod_text, total in spec_lines
    ]
    embed = discord.Embed(
        title=S("fun.dice.title"),
        description="\n".join(lines),
        color=discord.Color.blurple(),
    )
    embed.add_field(name="​", value=S("fun.dice.total", total=grand_total), inline=False)
    return embed
