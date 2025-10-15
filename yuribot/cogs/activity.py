from __future__ import annotations
from datetime import datetime, timezone
from io import StringIO
import csv
from typing import Optional, List

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _month_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")


def _fmt_rank(rows: list[tuple[int, int]], guild: discord.Guild, limit: int) -> str:
    lines: List[str] = []
    for i, (uid, cnt) in enumerate(rows[:limit], start=1):
        member = guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(S("activity.leaderboard.row", i=i, name=name, count=cnt))
    return "\n".join(lines) if lines else S("activity.leaderboard.empty")


class ActivityCog(commands.Cog):
    """Message activity tracking: per-member counts, monthly and total."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        models.bump_member_message(
            message.guild.id,
            message.author.id,
            when_iso=_now_iso(),
            inc=1,
        )

    group = app_commands.Group(name="activity", description="Member message activity")

    @group.command(name="top", description="Top active members by messages.")
    @app_commands.describe(scope="Month or all-time", month="YYYY-MM (for scope=month)", limit="Top N (5–50)")
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    async def top(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] = None,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 50] = 20,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        scope_val = (scope.value if scope else "month")
        if scope_val == "month":
            month = month or _month_default()
            rows = models.top_members_month(interaction.guild_id, month, int(limit))
            footer = S("activity.leaderboard.footer_month", limit=int(limit), month=month)
        else:
            rows = models.top_members_total(interaction.guild_id, int(limit))
            footer = S("activity.leaderboard.footer_all", limit=int(limit))

        desc = _fmt_rank(rows, interaction.guild, int(limit))
        embed = discord.Embed(
            title=S("activity.leaderboard.title"),
            description=desc,
            color=discord.Color.blurple()
        )
        embed.set_footer(text=footer)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @group.command(name="me", description="Your message counts (monthly + total).")
    @app_commands.describe(month="Optional YYYY-MM to highlight")
    async def me(self, interaction: discord.Interaction, month: Optional[str] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        total, rows = models.member_stats(interaction.guild_id, interaction.user.id)
        if not rows and total == 0:
            return await interaction.response.send_message(S("activity.none_yet"), ephemeral=True)

        month = month or _month_default()
        month_count = next((c for (m, c) in rows if m == month), 0)
        join = "\n".join([f"• **{m}** — {c}" for (m, c) in rows[:12]])  # last 12 months

        embed = discord.Embed(
            title=S("activity.me.title", user=interaction.user),
            color=discord.Color.green()
        )
        embed.add_field(name=S("activity.me.month", month=month), value=str(month_count), inline=True)
        embed.add_field(name=S("activity.me.total"), value=str(total), inline=True)
        if join:
            embed.add_field(name=S("activity.me.recent"), value=join, inline=False)
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @group.command(name="export", description="Export message activity as CSV.")
    @app_commands.describe(scope="Month or all-time", month="YYYY-MM for scope=month")
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    async def export(self, interaction: discord.Interaction, scope: app_commands.Choice[str] = None, month: Optional[str] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        scope_val = (scope.value if scope else "month")
        buf = StringIO()
        w = csv.writer(buf)

        if scope_val == "month":
            month = month or _month_default()
            rows = models.top_members_month(interaction.guild_id, month, limit=10_000)
            w.writerow(["guild_id", "month", "user_id", "count"])
            for uid, cnt in rows:
                w.writerow([interaction.guild_id, month, uid, cnt])
            filename = f"activity-{interaction.guild_id}-{month}.csv"
        else:
            rows = models.top_members_total(interaction.guild_id, limit=10_000)
            w.writerow(["guild_id", "user_id", "count"])
            for uid, cnt in rows:
                w.writerow([interaction.guild_id, uid, cnt])
            filename = f"activity-{interaction.guild_id}-all.csv"

        data = buf.getvalue().encode("utf-8")
        file = discord.File(fp=discord.BytesIO(data), filename=filename)
        await interaction.response.send_message(file=file, ephemeral=True)

    @group.command(name="reset", description="ADMIN: reset activity stats.")
    @app_commands.describe(scope="month/all", month="YYYY-MM if scope=month")
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    async def reset(self, interaction: discord.Interaction, scope: app_commands.Choice[str], month: Optional[str] = None):
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)
        if scope.value == "month" and not month:
            return await interaction.response.send_message(S("activity.reset.need_month"), ephemeral=True)

        models.reset_member_activity(interaction.guild_id, scope=scope.value, month=month)
        await interaction.response.send_message(S("activity.reset.done"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityCog(bot))
