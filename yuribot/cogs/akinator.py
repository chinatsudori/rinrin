from __future__ import annotations

import logging

import discord
from discord import app_commands
from discord.ext import commands

from ..ui.akinator import AkinatorView

log = logging.getLogger(__name__)


class AkinatorCog(commands.Cog):
    """20-questions style guessing game with an optional yuri focused mode."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="rinrinator",
        description="Play Rinrinator, a fast guessing game with an optional yuri mode.",
    )
    @app_commands.describe(yuri="Enable Rinrinator mode focused on yuri media")
    async def rinrinator(self, interaction: discord.Interaction, yuri: bool = False) -> None:
        if not interaction.guild:
            # Works in DMs, but keep response ephemeral for parity.
            pass
        view = AkinatorView(user=interaction.user, yuri_mode=yuri)
        await view.start(interaction)

        log.info(
            "fun.rinrinator.start",
            extra={
                "guild_id": getattr(interaction, "guild_id", None),
                "channel_id": getattr(interaction.channel, "id", None),
                "user_id": interaction.user.id,
                "yuri": yuri,
            },
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AkinatorCog(bot))

