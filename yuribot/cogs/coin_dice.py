from __future__ import annotations
import re
import random
from typing import List, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from ..strings import S  # uses your S(key, **kwargs) helper

MAX_COINS = 20
MAX_DICE_TOTAL = 50  # across all specs in one /roll

DICE_SPEC_RE = re.compile(
    r"""
    ^\s*
    (?:(\d*)d)?       # optional N before 'd' ('' or digits) - default 1
    (\d+)             # sides M (required)
    ([+-]\d+)?        # optional modifier like +2 or -1
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)

def _parse_specs(text: str) -> List[Tuple[int, int, int, str]]:

    if not text:
        raise ValueError("empty")
    tokens = [t.strip() for t in re.split(r"[, \u3000]+", text) if t.strip()]
    out: List[Tuple[int, int, int, str]] = []
    for tok in tokens:
        m = DICE_SPEC_RE.match(tok)
        if not m:
            raise ValueError(tok)
        n_str, sides_str, mod_str = m.groups()
        n = int(n_str) if n_str else 1
        sides = int(sides_str)
        mod = int(mod_str) if mod_str else 0
        if n <= 0 or sides <= 0:
            raise ValueError(tok)
        spec = f"{n}d{sides}{'+'+str(mod) if mod>0 else str(mod) if mod<0 else ''}"
        out.append((n, sides, mod, spec))
    return out

class CoinDiceCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="coin", description="Flip one or more coins.")
    @app_commands.describe(count=f"How many coins to flip (1–{MAX_COINS})")
    async def coin(self, interaction: discord.Interaction, count: app_commands.Range[int, 1, MAX_COINS] = 1):
        if count < 1 or count > MAX_COINS:
            return await interaction.response.send_message(
                S("fun.coin.limit", max=MAX_COINS), ephemeral=True
            )

        results = [random.choice(("Heads", "Tails")) for _ in range(count)]
        heads = sum(1 for r in results if r == "Heads")
        tails = count - heads

        emap = {"Heads": "H", "Tails": "T"}
        seq = " ".join(emap[r] for r in results)

        embed = discord.Embed(
            title=S("fun.coin.title"),
            color=discord.Color.gold()
        )
        embed.add_field(
            name=S("fun.coin.results", heads=heads, tails=tails),
            value=S("fun.coin.sequence", seq=seq),
            inline=False,
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="roll", description="Roll dice like d20, 2d6, or multiple specs: d20 2d6+1")
    @app_commands.describe(specs="One or more dice specs (e.g., d20, 2d6, 3d8+2)")
    async def roll(self, interaction: discord.Interaction, specs: str):
        try:
            parsed = _parse_specs(specs)
        except ValueError as e:
            bad = str(e)
            return await interaction.response.send_message(
                S("fun.dice.invalid_spec", text=bad if bad != "empty" else specs or "…"),
                ephemeral=True,
            )

        total_dice_requested = sum(n for n, _s, _m, _spec in parsed)
        if total_dice_requested > MAX_DICE_TOTAL:
            return await interaction.response.send_message(
                S("fun.dice.limit", max_dice=MAX_DICE_TOTAL),
                ephemeral=True,
            )

        lines: List[str] = []
        grand_total = 0
        for n, sides, mod, spec in parsed:
            rolls = [random.randint(1, sides) for _ in range(n)]
            subtotal = sum(rolls) + mod
            grand_total += subtotal

            rolls_text = "[" + ", ".join(str(r) for r in rolls) + "]"
            mod_text = S("fun.dice.mod_text", mod=mod) if mod != 0 else ""
            lines.append(S("fun.dice.rolls_line", spec=spec, rolls=rolls_text, mod_text=mod_text, total=subtotal))

        embed = discord.Embed(
            title=S("fun.dice.title"),
            description="\n".join(lines),
            color=discord.Color.blurple(),
        )
        embed.add_field(name="\u200b", value=S("fun.dice.total", total=grand_total), inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(CoinDiceCog(bot))

