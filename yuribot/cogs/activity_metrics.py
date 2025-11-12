# yuribot/cogs/activity_metrics.py
from __future__ import annotations

import asyncio
import datetime as dt
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import activity_metrics as am
from ..models import message_archive  # existing raw log model


class ActivityMetricsCog(commands.Cog):
    """Live metrics updater & on-demand rebuild."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        am.ensure_tables()

    async def cog_load(self) -> None:  # for discord.py >= 2.4
        am.ensure_tables()

    @commands.Cog.listener("on_message")
    async def on_message(self, message: discord.Message):
        # update live aggregates for every non-bot guild message
        try:
            am.upsert_from_message(message, include_bots=False)
        except Exception as e:
            # keep bot resilient; log if you have a logger
            print(f"[activity_metrics] upsert_from_message error: {e}")

    @app_commands.command(
        name="activity_rebuild",
        description="Rebuild live aggregates from channel history.",
    )
    @app_commands.describe(
        days="Look back this many days (default 30).",
        channel="Optionally limit to one channel.",
        include_bots="Count bot messages as activity.",
    )
    async def activity_rebuild(
        self,
        inter: discord.Interaction,
        days: Optional[int] = 30,
        channel: Optional[discord.TextChannel] = None,
        include_bots: Optional[bool] = False,
    ):
        if not inter.user.guild_permissions.manage_guild:
            await inter.response.sendMessage(
                "Need Manage Server to run this.", ephemeral=True
            )
            return
        if inter.guild is None:
            await inter.response.send_message("Run in a server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)
        guild = inter.guild
        since = None
        if days and days > 0:
            since = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=days)

        async def scan_channel(ch: discord.TextChannel):
            if not ch.permissions_for(guild.me).read_message_history:
                await inter.followup.send(
                    f"Skipping #{ch.name}: missing Read Message History", ephemeral=True
                )
                return
            count = 0
            async for m in ch.history(limit=None, oldest_first=True, after=since):
                am.upsert_from_message(m, include_bots=bool(include_bots))
                count += 1
                if count % 250 == 0:
                    await asyncio.sleep(0)
            await inter.followup.send(f"Indexed {count} in #{ch.name}", ephemeral=True)

        if channel:
            await scan_channel(channel)
        else:
            # order channels for stable progress display
            channels = [c for c in guild.text_channels]
            channels.sort(
                key=lambda c: (
                    c.category.position if c.category else -1,
                    c.position,
                    c.id,
                )
            )
            for ch in channels:
                await scan_channel(ch)

        await inter.followup.send("✅ Rebuild complete.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityMetricsCog(bot))
