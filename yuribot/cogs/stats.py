from __future__ import annotations
import platform
import time
import logging
from datetime import datetime
import discord
from discord.ext import commands
from discord import app_commands

try:
    import psutil  # optional
except Exception:
    psutil = None  # type: ignore

from ..strings import S

log = logging.getLogger(__name__)

def _human_delta(seconds: float) -> str:
    # 1d 02h 03m 04s
    s = int(seconds)
    days, s = divmod(s, 86400)
    hrs, s = divmod(s, 3600)
    mins, s = divmod(s, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    parts.append(f"{hrs:02d}h")
    parts.append(f"{mins:02d}m")
    parts.append(f"{s:02d}s")
    return " ".join(parts)

class StatsCog(commands.Cog):
    """Basic bot diagnostics: ping, uptime, stats."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        if not hasattr(bot, "start_time"):
            bot.start_time = time.monotonic()
        if not hasattr(bot, "start_datetime"):
            bot.start_datetime = datetime.utcnow()

    @app_commands.command(name="ping", description="Show gateway & round-trip latency.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def ping(self, interaction: discord.Interaction, post: bool = False):
        # measure end-to-end (ack defer, then compute)
        t0 = time.perf_counter()
        await interaction.response.defer(ephemeral=not post, thinking=False)
        rt_ms = (time.perf_counter() - t0) * 1000.0
        gw_ms = (self.bot.latency or 0.0) * 1000.0  # websocket heartbeat

        try:
            msg = S("stats.ping.message", gw_ms=f"{gw_ms:.0f}", rt_ms=f"{rt_ms:.0f}")
            await interaction.followup.send(msg, ephemeral=not post)
            log.info(
                "stats.ping.used",
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
                "stats.ping.failed",
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
            started: float = getattr(self.bot, "start_time", time.monotonic())
            up = time.monotonic() - started
            started_dt: datetime = getattr(self.bot, "start_datetime", datetime.utcnow())

            embed = discord.Embed(
                title=S("stats.uptime.title"),
                color=discord.Color.green()
            )
            embed.add_field(name=S("stats.uptime.field.uptime"), value=_human_delta(up), inline=True)
            embed.add_field(
                name=S("stats.uptime.field.since"),
                value=f"<t:{int(started_dt.timestamp())}:F>",
                inline=True
            )
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "stats.uptime.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "uptime_s": int(up),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "stats.uptime.failed",
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
            guilds = len(self.bot.guilds)

            # Safer counts; member cache may be partial
            members = sum((g.member_count or 0) for g in self.bot.guilds)
            humans = bots = 0
            for g in self.bot.guilds:
                # Only iterate cached members if available
                if hasattr(g, "members"):
                    for m in g.members:
                        if m.bot:
                            bots += 1
                        else:
                            humans += 1

            mem_txt = "n/a"
            cpu_txt = "n/a"
            if psutil:
                try:
                    p = psutil.Process()
                    with p.oneshot():
                        rss = p.memory_info().rss  # bytes
                        cpu = p.cpu_percent(interval=0.1)
                    mem_txt = f"{rss / (1024**2):.1f} MiB"
                    cpu_txt = f"{cpu:.1f}%"
                except Exception:
                    pass

            py = platform.python_version()
            dpy = discord.__version__

            shard_count = getattr(self.bot, "shard_count", None)
            shard_id = getattr(self.bot, "shard_id", None)
            if isinstance(shard_count, int) and shard_count > 1 and isinstance(shard_id, int):
                shard = f"{shard_id}/{shard_count}"
            else:
                shard = S("stats.botinfo.na")

            gw_latency = (self.bot.latency or 0.0) * 1000.0
            cmds_total = len(self.bot.tree.get_commands())

            embed = discord.Embed(title=S("stats.botinfo.title"), color=discord.Color.blurple())
            embed.add_field(name=S("stats.botinfo.field.guilds"), value=str(guilds), inline=True)
            embed.add_field(name=S("stats.botinfo.field.members_cached"), value=str(members), inline=True)
            embed.add_field(name=S("stats.botinfo.field.humans_bots"), value=f"{humans} / {bots}", inline=True)
            embed.add_field(name=S("stats.botinfo.field.commands"), value=str(cmds_total), inline=True)
            embed.add_field(name=S("stats.botinfo.field.shard"), value=shard, inline=True)
            embed.add_field(name=S("stats.botinfo.field.gateway_ping"), value=f"{gw_latency:.0f} ms", inline=True)
            embed.add_field(name=S("stats.botinfo.field.memory"), value=mem_txt, inline=True)
            embed.add_field(name=S("stats.botinfo.field.cpu"), value=cpu_txt, inline=True)
            embed.add_field(name=S("stats.botinfo.field.runtime"), value=S("stats.botinfo.value.runtime", py=py, dpy=dpy), inline=False)

            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "stats.botinfo.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "guilds": guilds,
                    "members": members,
                    "humans": humans,
                    "bots": bots,
                    "cmds_total": cmds_total,
                    "gw_ms": round(gw_latency, 1),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "stats.botinfo.failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(StatsCog(bot))
