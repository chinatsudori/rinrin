from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S  

log = logging.getLogger(__name__)

MAX_OPTIONS = 6
MAX_HOURS = 168  # 7 days per current API guidance

class PollsCog(commands.Cog):
    """Create native Discord polls with custom durations and up to 6 options."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    poll = app_commands.Group(
        name="poll",
        description=S("poll.native.group_desc"),
    )

    @poll.command(name="create", description=S("poll.native.create_desc"))
    @app_commands.describe(
        question=S("poll.native.arg.question"),
        opt1=S("poll.native.arg.opt1"),
        opt2=S("poll.native.arg.opt2"),
        opt3=S("poll.native.arg.opt3"),
        opt4=S("poll.native.arg.opt4"),
        opt5=S("poll.native.arg.opt5"),
        opt6=S("poll.native.arg.opt6"),
        hours=S("poll.native.arg.hours"),
        multi=S("poll.native.arg.multi"),
        ephemeral=S("poll.native.arg.ephemeral"),
    )
    async def create(
        self,
        interaction: discord.Interaction,
        question: str,
        opt1: str,
        opt2: str,
        opt3: Optional[str] = None,
        opt4: Optional[str] = None,
        opt5: Optional[str] = None,
        opt6: Optional[str] = None,
        hours: app_commands.Range[int, 1, MAX_HOURS] = 48,
        multi: bool = False,
        ephemeral: bool = False,
    ):
        # Guild-only
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        # Gather options and validate
        options = [o for o in (opt1, opt2, opt3, opt4, opt5, opt6) if o]
        if len(options) < 2:
            return await interaction.response.send_message(
                S("poll.native.err.need_two"), ephemeral=True
            )
        if len(options) > MAX_OPTIONS:
            return await interaction.response.send_message(
                S("poll.native.err.too_many", n=MAX_OPTIONS), ephemeral=True
            )

        # Create & send native poll (must be in initial response)
        try:
            p = discord.Poll(
                question=question[:300],
                duration=timedelta(hours=int(hours)),
                allow_multiselect=multi,
            )
            for text in options:
                p.add_answer(text=text[:300])

            await interaction.response.send_message(
                poll=p,
                ephemeral=ephemeral
            )

            log.info(
                "poll.create",
                extra={
                    "guild_id": interaction.guild_id,
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": interaction.user.id,
                    "hours": hours,
                    "multi": multi,
                    "opts": len(options),
                },
            )

        except Exception as e:
            log.exception("poll.create.failed", exc_info=e)
            msg = S("poll.native.err.create_failed", err=type(e).__name__)
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(PollsCog(bot))
