from __future__ import annotations
import platform
import time
from datetime import datetime
import discord
from discord.ext import commands
from discord import app_commands

try:
    import psutil  # optional
except Exception:
    psutil = None  # type: ignore

from ..strings import S


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
    async def ping(self, interaction: discord.Interaction):
        t0 = time.perf_counter()
        await interaction.response.defer(ephemeral=True, thinking=False)
        rt_ms = (time.perf_counter() - t0) * 1000.0
        gw_ms = (self.bot.latency or 0.0) * 1000.0  # websocket heartbeat

        msg = S(
            "stats.ping.message",
            gw_ms=f"{gw_ms:.0f}",
            rt_ms=f"{rt_ms:.0f}",
        )
        await interaction.followup.send(msg, ephemeral=True)

    @app_commands.command(name="uptime", description="Show how long the bot has been running.")
    async def uptime(self, interaction: discord.Interaction):
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
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="botinfo", description="Show runtime stats about the bot.")
    async def botinfo(self, interaction: discord.Interaction):
        # Guilds / members (cached)
        guilds = len(self.bot.guilds)
        members = 0
        humans = 0
        bots = 0
        for g in self.bot.guilds:
            members += g.member_count or 0
            for m in g.members:
                if m.bot:
                    bots += 1
                else:
                    humans += 1

        # Process stats (optional)
        mem_txt = "n/a"
        cpu_txt = "n/a"
        if psutil:
            p = psutil.Process()
            with p.oneshot():
                rss = p.memory_info().rss  # bytes
                cpu = p.cpu_percent(interval=0.1)
            mem_txt = f"{rss / (1024**2):.1f} MiB"
            cpu_txt = f"{cpu:.1f}%"

        py = platform.python_version()
        dpy = discord.__version__

        # ---- SAFE SHARD FORMAT ----
        shard_count = getattr(self.bot, "shard_count", None)
        shard_id = getattr(self.bot, "shard_id", None)
        if isinstance(shard_count, int) and shard_count > 1 and isinstance(shard_id, int):
            shard = f"{shard_id}/{shard_count}"
        else:
            shard = "n/a"

        gw_latency = (self.bot.latency or 0.0) * 1000.0
        cmds_total = len(self.bot.tree.get_commands())

        embed = discord.Embed(title="🤖 Bot Info", color=discord.Color.blurple())
        embed.add_field(name="Guilds", value=str(guilds), inline=True)
        embed.add_field(name="Members (cached)", value=f"{members}", inline=True)
        embed.add_field(name="Humans / Bots", value=f"{humans} / {bots}", inline=True)
        embed.add_field(name="Commands", value=str(cmds_total), inline=True)
        embed.add_field(name="Shard", value=shard, inline=True)
        embed.add_field(name="Gateway Ping", value=f"{gw_latency:.0f} ms", inline=True)
        embed.add_field(name="Memory", value=mem_txt, inline=True)
        embed.add_field(name="CPU", value=cpu_txt, inline=True)
        embed.add_field(name="Runtime", value=f"py {py} · discord.py {dpy}", inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(StatsCog(bot))
