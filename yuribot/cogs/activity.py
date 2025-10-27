from __future__ import annotations
import logging
import re
from datetime import datetime, timezone
from io import StringIO, BytesIO
import csv
from typing import Optional, List, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _month_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")

def _count_words(text: str | None) -> int:
    if not text:
        return 0
    return len(WORD_RE.findall(text))

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

def _have_word_models() -> bool:
    """Check if the backing models expose the word-count API."""
    return all(
        hasattr(models, attr) for attr in (
            "bump_member_words",
            "top_members_words_month",
            "top_members_words_total",
            "member_word_stats",
            "reset_member_words",
        )
    )

class ActivityCog(commands.Cog):
    """Message activity tracking: per-member counts, monthly and total (messages & words)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        try:
            # Increment message count (existing)
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

        # Increment word count (best-effort, if models support it)
        if _have_word_models():
            try:
                inc_words = _count_words(message.content)
                if inc_words > 0:
                    models.bump_member_words(
                        message.guild.id,
                        message.author.id,
                        when_iso=_now_iso(),
                        inc=inc_words,
                    )
            except Exception:
                log.exception(
                    "activity.bump_words_failed",
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

    @group.command(name="top", description="Top active members by messages or words.")
    @app_commands.describe(
        scope="Month or all-time",
        month="YYYY-MM (for scope=month)",
        limit="Top N (5–50)",
        metric="messages or words",
        post="If true, post publicly in this channel",
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
    ])
    @app_commands.autocomplete(month=_month_autocomplete)
    async def top(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] | None = None,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 50] = 20,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val = (scope.value if scope else "month")
            metric_val = (metric.value if metric else "messages")

            if metric_val == "words" and not _have_word_models():
                return await interaction.followup.send(
                    "Word-count tracking isn’t enabled in the backing storage.",
                    ephemeral=not post,
                )

            if scope_val == "month":
                month = month or _month_default()
                if not MONTH_RE.match(month):
                    return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)
                if metric_val == "words":
                    rows = models.top_members_words_month(interaction.guild_id, month, int(limit))
                else:
                    rows = models.top_members_month(interaction.guild_id, month, int(limit))
                footer = f"Top {int(limit)} — {month} — {metric_val}"
            else:
                if metric_val == "words":
                    rows = models.top_members_words_total(interaction.guild_id, int(limit))
                else:
                    rows = models.top_members_total(interaction.guild_id, int(limit))
                footer = f"Top {int(limit)} — all-time — {metric_val}"

            desc = _fmt_rank(rows, interaction.guild, int(limit))
            title_base = S("activity.leaderboard.title")
            embed = discord.Embed(
                title=f"{title_base} ({metric_val})",
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
                    "metric": metric_val,
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
                    "metric": metric.value if metric else None,
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="me", description="Your counts (monthly + total) for messages or words.")
    @app_commands.describe(
        month="Optional YYYY-MM to highlight",
        metric="messages or words",
        post="If true, post publicly in this channel",
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
    ])
    @app_commands.autocomplete(month=_month_autocomplete)
    async def me(
        self,
        interaction: discord.Interaction,
        month: Optional[str] = None,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            metric_val = (metric.value if metric else "messages")
            if metric_val == "words" and not _have_word_models():
                return await interaction.followup.send(
                    "Word-count tracking isn’t enabled in the backing storage.",
                    ephemeral=not post,
                )

            if metric_val == "words":
                total, rows = models.member_word_stats(interaction.guild_id, interaction.user.id)
            else:
                total, rows = models.member_stats(interaction.guild_id, interaction.user.id)

            if not rows and total == 0:
                return await interaction.followup.send(S("activity.none_yet"), ephemeral=not post)

            month = month or _month_default()
            if not MONTH_RE.match(month):
                return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)

            month_count = next((c for (m, c) in rows if m == month), 0)
            join = "\n".join([f"• **{m}** — {c}" for (m, c) in rows[:12]])  # last 12 months

            title = f"{S('activity.me.title', user=interaction.user)} ({metric_val})"
            embed = discord.Embed(title=title, color=discord.Color.green())
            embed.add_field(name=S("activity.me.month", month=month), value=str(month_count), inline=True)
            embed.add_field(name=S("activity.me.total"), value=str(total), inline=True)
            if join:
                embed.add_field(name=S("activity.me.recent"), value=join, inline=False)
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "activity.me.used",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "month": month, "metric": metric_val, "post": post},
            )
        except Exception:
            log.exception(
                "activity.me.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "month": month},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="export", description="Export activity as CSV (messages or words).")
    @app_commands.describe(
        scope="Month or all-time",
        month="YYYY-MM for scope=month",
        metric="messages or words",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
    ])
    async def export(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] | None = None,
        month: Optional[str] = None,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val = (scope.value if scope else "month")
            metric_val = (metric.value if metric else "messages")
            if metric_val == "words" and not _have_word_models():
                return await interaction.followup.send(
                    "Word-count tracking isn’t enabled in the backing storage.",
                    ephemeral=not post,
                )

            buf = StringIO()
            w = csv.writer(buf)

            if scope_val == "month":
                month = month or _month_default()
                if not MONTH_RE.match(month):
                    return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)
                if metric_val == "words":
                    rows = models.top_members_words_month(interaction.guild_id, month, limit=10_000)
                    w.writerow(["guild_id", "month", "user_id", "words"])
                else:
                    rows = models.top_members_month(interaction.guild_id, month, limit=10_000)
                    w.writerow(["guild_id", "month", "user_id", "messages"])
                for uid, cnt in rows:
                    w.writerow([interaction.guild_id, month, uid, cnt])
                filename = f"activity-{interaction.guild_id}-{month}-{metric_val}.csv"
            else:
                if metric_val == "words":
                    rows = models.top_members_words_total(interaction.guild_id, limit=10_000)
                    w.writerow(["guild_id", "user_id", "words"])
                else:
                    rows = models.top_members_total(interaction.guild_id, limit=10_000)
                    w.writerow(["guild_id", "user_id", "messages"])
                for uid, cnt in rows:
                    w.writerow([interaction.guild_id, uid, cnt])
                filename = f"activity-{interaction.guild_id}-all-{metric_val}.csv"

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
                    "metric": metric_val,
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "activity.export.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value if scope else None, "month": month},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="reset", description="ADMIN: reset activity stats (messages or words).")
    @app_commands.describe(
        scope="month/all",
        month="YYYY-MM if scope=month",
        metric="messages or words",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
    ])
    @app_commands.default_permissions(manage_guild=True)
    async def reset(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str],
        month: Optional[str] = None,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False
    ):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        metric_val = (metric.value if metric else "messages")
        if metric_val == "words" and not _have_word_models():
            return await interaction.response.send_message(
                "Word-count tracking isn’t enabled in the backing storage.",
                ephemeral=True,
            )

        if scope.value == "month":
            if not month:
                return await interaction.response.send_message(S("activity.reset.need_month"), ephemeral=not post)
            if not MONTH_RE.match(month):
                return await interaction.response.send_message(S("activity.bad_month_format"), ephemeral=not post)

        try:
            if metric_val == "words":
                models.reset_member_words(interaction.guild_id, scope=scope.value, month=month)
            else:
                models.reset_member_activity(interaction.guild_id, scope=scope.value, month=month)

            msg = (
                f"Reset {metric_val} for {month}."
                if scope.value == "month"
                else f"Reset all-time {metric_val}."
            )
            await interaction.response.send_message(msg, ephemeral=not post)
            log.warning(
                "activity.reset.done",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value, "month": month, "metric": metric_val, "post": post},
            )
        except Exception:
            log.exception(
                "activity.reset.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope.value, "month": month, "metric": metric_val},
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
