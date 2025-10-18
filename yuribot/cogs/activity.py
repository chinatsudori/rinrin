from __future__ import annotations
import logging
import re
from datetime import datetime, timezone
from io import StringIO, BytesIO
import csv
from typing import Optional, List

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

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

async def _require_guild(inter: discord.Interaction) -> bool:
    if not inter.guild:
        if not inter.response.is_done():
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
        else:
            await inter.followup.send(S("common.guild_only"), ephemeral=True)
        return False
    return True

class ActivityCog(commands.Cog):
    """Message activity tracking: per-member counts, monthly and total."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        try:
            models.bump_member_message(
                message.guild.id,
                message.author.id,
                when_iso=_now_iso(),
                inc=1,
            )
        except Exception:
            log.exception(
                "activity.bump_failed",
                extra={
                    "guild_id": getattr(message.guild, "id", None),
                    "user_id": getattr(message.author, "id", None),
                },
            )

    group = app_commands.Group(name="activity", description="Member message activity")

    async def _month_autocomplete(self, inter: discord.Interaction, current: str):
        gid = inter.guild_id
        try:
            available = getattr(models, "available_months", lambda _gid: [])(gid) or []
        except Exception:
            log.exception("activity.month_autocomplete_failed", extra={"guild_id": gid})
            available = []

        if not available:
            now = datetime.now(timezone.utc)
            y, m = now.year, now.month
            for _ in range(12):
                available.append(f"{y:04d}-{m:02d}")
                m -= 1
                if m == 0:
                    m = 12
                    y -= 1

        filtered = [c for c in available if c.startswith(current)] if current else available
        return [app_commands.Choice(name=c, value=c) for c in filtered[:25]]

    @group.command(name="top", description="Top active members by messages.")
    @app_commands.describe(
        scope="Month or all-time",
        month="YYYY-MM (for scope=month)",
        limit="Top N (5–50)",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.autocomplete(month=_month_autocomplete)
    async def top(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] | None = None,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 50] = 20,
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val = (scope.value if scope else "month")
            if scope_val == "month":
                month = month or _month_default()
                if not MONTH_RE.match(month):
                    return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)
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
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "activity.top.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "scope": scope_val,
                    "month": month,
                    "limit": int(limit),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "activity.top.failed",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "scope": scope.value if scope else None,
                    "month": month,
                    "limit": int(limit),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="me", description="Your message counts (monthly + total).")
    @app_commands.describe(
        month="Optional YYYY-MM to highlight",
        post="If true, post publicly in this channel"
    )
    @app_commands.autocomplete(month=_month_autocomplete)
    async def me(self, interaction: discord.Interaction, month: Optional[str] = None, post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            total, rows = models.member_stats(interaction.guild_id, interaction.user.id)
            if not rows and total == 0:
                return await interaction.followup.send(S("activity.none_yet"), ephemeral=not post)

            month = month or _month_default()
            if not MONTH_RE.match(month):
                return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)

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
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "activity.me.used",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "month": month, "post": post},
            )
        except Exception:
            log.exception(
                "activity.me.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "month": month},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="export", description="Export message activity as CSV.")
    @app_commands.describe(
        scope="Month or all-time",
        month="YYYY-MM for scope=month",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    async def export(self, interaction: discord.Interaction, scope: app_commands.Choice[str] | None = None, month: Optional[str] = None, post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val = (scope.value if scope else "month")
            buf = StringIO()
            w = csv.writer(buf)

            if scope_val == "month":
                month = month or _month_default()
                if not MONTH_RE.match(month):
                    return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)
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
            file = discord.File(fp=BytesIO(data), filename=filename)
            await interaction.followup.send(file=file, ephemeral=not post)

            log.info(
                "activity.export.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "scope": scope_val,
                    "month": month,
                    "rows": len(rows),
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "activity.export.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value if scope else None, "month": month},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="reset", description="ADMIN: reset activity stats.")
    @app_commands.describe(
        scope="month/all",
        month="YYYY-MM if scope=month",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.default_permissions(manage_guild=True)
    async def reset(self, interaction: discord.Interaction, scope: app_commands.Choice[str], month: Optional[str] = None, post: bool = False):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        if scope.value == "month":
            if not month:
                return await interaction.response.send_message(S("activity.reset.need_month"), ephemeral=not post)
            if not MONTH_RE.match(month):
                return await interaction.response.send_message(S("activity.bad_month_format"), ephemeral=not post)

        try:
            models.reset_member_activity(interaction.guild_id, scope=scope.value, month=month)
            msg = (
                S("activity.reset.public_notice_month", scope=scope.value, month=month)
                if scope.value == "month"
                else S("activity.reset.public_notice_all", scope=scope.value)
            )
            await interaction.response.send_message(msg, ephemeral=not post)
            log.warning(
                "activity.reset.done",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value, "month": month, "post": post},
            )
        except Exception:
            log.exception(
                "activity.reset.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value, "month": month},
            )
            await interaction.response.send_message(S("common.error_generic"), ephemeral=True)

    @commands.Cog.listener()
    async def on_app_command_completion(self, interaction: discord.Interaction, command: app_commands.Command):
        try:
            log.info(
                "slash.completed",
                extra={
                    "guild_id": interaction.guild_id,
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": interaction.user.id,
                    "command": command.qualified_name,
                },
            )
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        log.exception(
            "slash.error",
            extra={
                "guild_id": interaction.guild_id,
                "channel_id": getattr(interaction.channel, "id", None),
                "user_id": getattr(interaction.user, "id", None),
                "error_type": type(error).__name__,
                "command": getattr(interaction.command, "qualified_name", None),
            },
        )
        if not interaction.response.is_done():
            await interaction.response.send_message(S("common.error_generic"), ephemeral=True)
        else:
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityCog(bot))
