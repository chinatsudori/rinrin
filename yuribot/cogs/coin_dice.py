from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..ui.coin_dice import build_coin_embed, build_dice_embed
from ..utils.coin_dice import (
    MAX_COINS,
    MAX_DICE_TOTAL,
    DiceSpecError,
    flip_coins,
    parse_specs,
    roll_dice,
)

log = logging.getLogger(__name__)


class CoinDiceCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="coin", description="Flip one or more coins.")
    @app_commands.describe(
        count=f"How many coins to flip (1-{MAX_COINS})",
        post="If true, post publicly in this channel",
    )
    async def coin(
        self,
        interaction: discord.Interaction,
        count: app_commands.Range[int, 1, MAX_COINS] = 1,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post)
        try:
            results, heads, tails = flip_coins(int(count))
            sequence = " ".join("H" if r == "Heads" else "T" for r in results)
            embed = build_coin_embed(heads=heads, tails=tails, sequence=sequence)
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "fun.coin.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": interaction.user.id,
                    "count": int(count),
                    "post": post,
                    "heads": heads,
                    "tails": tails,
                },
            )
        except Exception:
            log.exception(
                "fun.coin.failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "count": int(count),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(
        name="roll",
        description="Roll dice like d20, 2d6, or multiple specs: d20 2d6+1",
    )
    @app_commands.describe(
        specs="One or more dice specs (e.g., d20, 2d6, 3d8+2)",
        post="If true, post publicly in this channel",
    )
    async def roll(self, interaction: discord.Interaction, specs: str, post: bool = False):
        await interaction.response.defer(ephemeral=not post)
        try:
            parsed = parse_specs(specs)
        except DiceSpecError as exc:
            bad = str(exc)
            return await interaction.followup.send(
                S("fun.dice.invalid_spec", text=bad if bad != "empty" else specs or "."),
                ephemeral=not post,
            )

        lines, total_dice_requested, grand_total = roll_dice(parsed)
        if total_dice_requested > MAX_DICE_TOTAL:
            return await interaction.followup.send(
                S("fun.dice.limit", max_dice=MAX_DICE_TOTAL),
                ephemeral=not post,
            )

        spec_lines = []
        for spec, rolls, modifier, subtotal in lines:
            rolls_text = "[" + ", ".join(str(r) for r in rolls) + "]"
            mod_text = S("fun.dice.mod_text", mod=modifier) if modifier != 0 else ""
            spec_lines.append((spec, rolls_text, mod_text, subtotal))

        embed = build_dice_embed(spec_lines=spec_lines, grand_total=grand_total)
        await interaction.followup.send(embed=embed, ephemeral=not post)

        log.info(
            "fun.dice.used",
            extra={
                "guild_id": getattr(interaction, "guild_id", None),
                "channel_id": getattr(interaction.channel, "id", None),
                "user_id": interaction.user.id,
                "specs": specs,
                "spec_count": len(parsed),
                "dice_total": total_dice_requested,
                "grand_total": grand_total,
                "post": post,
            },
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(CoinDiceCog(bot))