from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..utils.polls import MAX_HOURS, MAX_OPTIONS, add_answer_compat, create_poll

log = logging.getLogger(__name__)


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
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        options = [opt for opt in (opt1, opt2, opt3, opt4, opt5, opt6) if opt]
        if len(options) < 2:
            return await interaction.response.send_message(S("poll.native.err.need_two"), ephemeral=True)
        if len(options) > MAX_OPTIONS:
            return await interaction.response.send_message(
                S("poll.native.err.too_many", n=MAX_OPTIONS), ephemeral=True
            )

        try:
            poll, multi_honored = create_poll(question[:300], int(hours), multi)
            for text in options:
                add_answer_compat(poll, text[:300])

            await interaction.response.send_message(poll=poll, ephemeral=ephemeral)

            if multi and not multi_honored:
                try:
                    await interaction.followup.send(
                        "This Discord library build doesn't support multi-select polls; created single-choice.",
                        ephemeral=True,
                    )
                except Exception:
                    pass

            log.info(
                "poll.create",
                extra={
                    "guild_id": interaction.guild_id,
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": interaction.user.id,
                    "hours": int(hours),
                    "multi": multi,
                    "multi_honored": multi_honored,
                    "opts": len(options),
                },
            )
        except Exception as exc:
            log.exception("poll.create.failed", exc_info=exc)
            message = S("poll.native.err.create_failed", err=type(exc).__name__)
            if not interaction.response.is_done():
                await interaction.response.send_message(message, ephemeral=True)
            else:
                await interaction.followup.send(message, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(PollsCog(bot))