from __future__ import annotations

import logging
import time

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..ui.stats import build_botinfo_embed, build_uptime_embed
from ..utils.stats import ensure_start_metadata, gather_botinfo, uptime_info

log = logging.getLogger(__name__)


class CmdCog(commands.GroupCog, name="cmd", description="System diagnostics"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        ensure_start_metadata(bot)

    @app_commands.command(name="ping", description="Show gateway & round-trip latency.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def ping(self, interaction: discord.Interaction, post: bool = False):
        t0 = time.perf_counter()
        await interaction.response.defer(ephemeral=not post, thinking=False)
        rt_ms = (time.perf_counter() - t0) * 1000.0
        gw_ms = (self.bot.latency or 0.0) * 1000.0

        try:
            msg = S("stats.ping.message", gw_ms=f"{gw_ms:.0f}", rt_ms=f"{rt_ms:.0f}")
            await interaction.followup.send(msg, ephemeral=not post)
            log.info(
                "cmd.ping.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "gw_ms": round(gw_ms, 1),
                    "rt_ms": round(rt_ms, 1),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "cmd.ping.failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="uptime", description="Show how long the bot has been running.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def uptime(self, interaction: discord.Interaction, post: bool = False):
        await interaction.response.defer(ephemeral=not post, thinking=False)
        try:
            uptime_seconds, started_at = uptime_info(self.bot)
            embed = build_uptime_embed(uptime_seconds, started_at)
            await interaction.followup.send(embed=embed, ephemeral=not post)
            log.info(
                "cmd.uptime.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "uptime_s": int(uptime_seconds),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "cmd.uptime.failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="botinfo", description="Show runtime stats about the bot.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def botinfo(self, interaction: discord.Interaction, post: bool = False):
        await interaction.response.defer(ephemeral=not post, thinking=False)
        try:
            metrics = gather_botinfo(self.bot)
            embed = build_botinfo_embed(metrics)
            await interaction.followup.send(embed=embed, ephemeral=not post)
            log.info(
                "cmd.botinfo.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "guilds": metrics["guilds"],
                    "members": metrics["members_cached"],
                    "humans": metrics["humans"],
                    "bots": metrics["bots"],
                    "cmds_total": metrics["commands_total"],
                    "gw_ms": round(metrics["gw_latency_ms"], 1),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "cmd.botinfo.failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CmdCog(bot))
