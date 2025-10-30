from __future__ import annotations

import csv
import io
import logging
import re
from calendar import monthrange
from datetime import datetime, timezone
from io import StringIO, BytesIO
from typing import Optional, List, Tuple

import discord
from discord.ext import commands
from discord import app_commands

try:
    import matplotlib.pyplot as plt  # type: ignore
    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

try:
    from zoneinfo import ZoneInfo  # py>=3.9
except Exception:
    ZoneInfo = None  # type: ignore

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
DAY_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
WEEK_RE = re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$")
WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)

# Custom emoji: <a:name:id> or <:name:id>
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:\d+>")
# Loosely match Unicode emoji ranges
UNICODE_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]")

PT_TZNAME = "America/Los_Angeles"

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _month_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m")

def _week_default() -> str:
    dt = datetime.now(timezone.utc).date()
    iso = dt.isocalendar()
    return f"{iso.year}-W{iso.week:02d}"

def _day_default() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def _count_words(text: str | None) -> int:
    if not text:
        return 0
    return len(WORD_RE.findall(text))

def _count_emojis_text(text: str | None) -> int:
    if not text:
        return 0
    return len(CUSTOM_EMOJI_RE.findall(text)) + len(UNICODE_EMOJI_RE.findall(text))

def _fmt_rank(rows: list[tuple[int, int]], guild: discord.Guild, limit: int) -> str:
    lines: List[str] = []
    for i, (uid, cnt) in enumerate(rows[:limit], start=1):
        member = guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(S("activity.leaderboard.row", i=i, name=name, count=cnt))
    return "\n".join(lines) if lines else S("activity.leaderboard.empty")

def _prime_window_from_hist(hour_counts: List[int], window: int = 1) -> tuple[int, int, int]:
    """Return (start_hour, end_hour, total_in_window) for best contiguous window size."""
    best_sum, best_h = -1, 0
    for h in range(24):
        s = sum(hour_counts[(h + i) % 24] for i in range(window))
        if s > best_sum:
            best_sum, best_h = s, h
    return best_h, (best_h + window) % 24, best_sum

def _fmt_hour_range_local(start: int, end: int, tzlabel: str = "PT") -> str:
    def h12(h: int) -> str:
        ampm = "AM" if h < 12 else "PM"
        hh = h % 12 or 12
        return f"{hh}{ampm}"
    return f"{h12(start)}–{h12(end)} {tzlabel}"

def _fmt_scope_footer(limit: int, scope: str, key: Optional[str], metric: str) -> str:
    return f"Top {limit} — {'all-time' if scope=='all' else f'{scope}={key}'} — {metric}"

def _parse_scope_and_key(
    scope: str | None,
    day: str | None,
    week: str | None,
    month: str | None,
) -> tuple[str, Optional[str]]:
    s = scope or "month"
    if s == "day":
        key = day or _day_default()
        if not DAY_RE.match(key):
            raise ValueError("bad_day_format")
    elif s == "week":
        key = week or _week_default()
        if not WEEK_RE.match(key):
            raise ValueError("bad_week_format")
    elif s == "month":
        key = month or _month_default()
        if not MONTH_RE.match(key):
            raise ValueError("bad_month_format")
    else:
        s = "all"
        key = None
    return s, key

async def _require_guild(inter: discord.Interaction) -> bool:
    if not inter.guild:
        if not inter.response.is_done():
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
        else:
            await inter.followup.send(S("common.guild_only"), ephemeral=True)
        return False
    return True


def _have_word_models() -> bool:
    return all(
        hasattr(models, attr) for attr in (
            "bump_member_words",
            "top_members_words_period",
            "top_members_words_total",
            "member_word_stats",
            "reset_member_words",
        )
    )

def _have_mention_models() -> bool:
    return all(hasattr(models, a) for a in (
        "bump_member_mentioned",
        "top_members_mentions_period",
        "top_members_mentions_total",
    ))

def _have_emoji_models() -> bool:
    return all(hasattr(models, a) for a in (
        "bump_member_emoji_chat",
        "bump_member_emoji_react",
        "top_members_emoji_chat_period",
        "top_members_emoji_react_period",
        "top_members_emoji_chat_total",
        "top_members_emoji_react_total",
    ))

def _have_periodic_models() -> bool:
    return all(hasattr(models, a) for a in (
        "top_members_messages_period",
        "top_members_messages_total",
        "member_daily_counts_month",
        "member_hour_histogram_total",
    ))

class ActivityCog(commands.Cog):
    """Message activity tracking: messages, words, mentions, emoji (chat vs react), with day/week/month/all scopes."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return

        # Messages
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

        # Words
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
                        "guild_id": message.guild.id,
                        "user_id": message.author.id,
                    },
                )

        # Mentions (credit the mentioned members)
        if _have_mention_models():
            try:
                mentioned_ids = {m.id for m in message.mentions if not m.bot}
                for uid in mentioned_ids:
                    models.bump_member_mentioned(message.guild.id, uid, when_iso=_now_iso(), inc=1)
            except Exception:
                log.exception("activity.bump_mentions_failed", extra={"guild_id": message.guild.id})

        # Emoji usage in chat (content)
        if _have_emoji_models():
            try:
                ecount = _count_emojis_text(message.content)
                if ecount > 0:
                    models.bump_member_emoji_chat(message.guild.id, message.author.id, when_iso=_now_iso(), inc=ecount)
            except Exception:
                log.exception("activity.bump_emoji_chat_failed", extra={"guild_id": message.guild.id, "user_id": message.author.id})

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.guild_id is None or payload.user_id is None:
            return
        guild = self.bot.get_guild(payload.guild_id)
        if not guild:
            return
        member = guild.get_member(payload.user_id)
        if not member or member.bot:
            return
        if _have_emoji_models():
            try:
                models.bump_member_emoji_react(payload.guild_id, payload.user_id, when_iso=_now_iso(), inc=1)
            except Exception:
                log.exception("activity.bump_emoji_react_add_failed", extra={"guild_id": payload.guild_id, "user_id": payload.user_id})

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

    @group.command(name="top", description="Top members by messages, words, mentions, or emojis.")
    @app_commands.describe(
        scope="day/week/month/all",
        day="YYYY-MM-DD (for scope=day)",
        week="YYYY-Www (ISO week, e.g. 2025-W41)",
        month="YYYY-MM (for scope=month)",
        limit="Top N (5–50)",
        metric="messages, words, mentions, emoji_chat, or emoji_react",
        post="If true, post publicly in this channel",
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="day", value="day"),
        app_commands.Choice(name="week", value="week"),
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions", value="mentions"),
        app_commands.Choice(name="emoji_chat", value="emoji_chat"),
        app_commands.Choice(name="emoji_react", value="emoji_react"),
    ])
    @app_commands.autocomplete(month=_month_autocomplete)
    async def top(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] | None = None,
        day: Optional[str] = None,
        week: Optional[str] = None,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 50] = 20,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val, key = _parse_scope_and_key(scope.value if scope else None, day, week, month)
            metric_val = (metric.value if metric else "messages")

            # Capability guards
            if metric_val == "words" and not _have_word_models():
                return await interaction.followup.send("Word-count tracking isn’t enabled in the backing storage.", ephemeral=not post)
            if metric_val == "mentions" and not _have_mention_models():
                return await interaction.followup.send("Mention tracking isn’t enabled.", ephemeral=not post)
            if metric_val in {"emoji_chat", "emoji_react"} and not _have_emoji_models():
                return await interaction.followup.send("Emoji tracking isn’t enabled.", ephemeral=not post)

            lim = int(limit)
            # Fetch rows
            if scope_val == "all":
                if metric_val == "words":
                    rows = models.top_members_words_total(interaction.guild_id, lim) if _have_word_models() else []
                elif metric_val == "mentions":
                    rows = models.top_members_mentions_total(interaction.guild_id, lim) if _have_mention_models() else []
                elif metric_val == "emoji_chat":
                    rows = models.top_members_emoji_chat_total(interaction.guild_id, lim) if _have_emoji_models() else []
                elif metric_val == "emoji_react":
                    rows = models.top_members_emoji_react_total(interaction.guild_id, lim) if _have_emoji_models() else []
                else:
                    rows = models.top_members_messages_total(interaction.guild_id, lim)
            else:
                if metric_val == "words":
                    rows = models.top_members_words_period(interaction.guild_id, scope=scope_val, key=key, limit=lim)
                elif metric_val == "mentions":
                    rows = models.top_members_mentions_period(interaction.guild_id, scope=scope_val, key=key, limit=lim)
                elif metric_val == "emoji_chat":
                    rows = models.top_members_emoji_chat_period(interaction.guild_id, scope=scope_val, key=key, limit=lim)
                elif metric_val == "emoji_react":
                    rows = models.top_members_emoji_react_period(interaction.guild_id, scope=scope_val, key=key, limit=lim)
                else:
                    rows = models.top_members_messages_period(interaction.guild_id, scope=scope_val, key=key, limit=lim)

            desc = _fmt_rank(rows, interaction.guild, lim)
            embed = discord.Embed(
                title=f"{S('activity.leaderboard.title')} ({metric_val})",
                description=desc or S("activity.leaderboard.empty"),
                color=discord.Color.blurple()
            )
            embed.set_footer(text=_fmt_scope_footer(lim, scope_val, key, metric_val))
            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info("activity.top.used", extra={
                "guild_id": interaction.guild_id, "user_id": interaction.user.id,
                "scope": scope_val, "key": key, "limit": lim, "metric": metric_val, "post": post
            })
        except ValueError as ve:
            key = str(ve)
            if key == "bad_month_format":
                msg = S("activity.bad_month_format")
            elif key == "bad_day_format":
                msg = "Use YYYY-MM-DD for day."
            elif key == "bad_week_format":
                msg = "Use YYYY-Www (e.g. 2025-W41) for week."
            else:
                msg = S("common.error_generic")
            await interaction.followup.send(msg, ephemeral=not post)
        except Exception:
            log.exception("activity.top.failed", extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="me", description="Your counts and prime chat window (messages or words).")
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

            # Prime window (PT) using hour histogram
            try:
                if _have_periodic_models():
                    hist = models.member_hour_histogram_total(
                        interaction.guild_id, interaction.user.id, tz=PT_TZNAME
                    )
                    if isinstance(hist, (list, tuple)) and len(hist) == 24:
                        s1, e1, _ = _prime_window_from_hist(list(hist), window=1)
                        s2, e2, _ = _prime_window_from_hist(list(hist), window=2)
                        embed.add_field(name="Prime hour", value=_fmt_hour_range_local(s1, e1, "PT"), inline=True)
                        embed.add_field(name="Prime 2-hr", value=_fmt_hour_range_local(s2, e2, "PT"), inline=True)
            except Exception:
                log.exception("activity.me.prime_window_failed", extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id})

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


    @group.command(name="graph", description="Graph message frequency per day over a month.")
    @app_commands.describe(
        month="YYYY-MM",
        member="Optional member to filter",
        post="If true, post publicly in this channel"
    )
    @app_commands.autocomplete(month=_month_autocomplete)
    async def graph(
        self,
        interaction: discord.Interaction,
        month: Optional[str] = None,
        member: Optional[discord.Member] = None,
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            month = month or _month_default()
            if not MONTH_RE.match(month):
                return await interaction.followup.send(S("activity.bad_month_format"), ephemeral=not post)
            if not _have_periodic_models():
                return await interaction.followup.send("Graphing not available (missing periodic models).", ephemeral=not post)

            uid = member.id if member else None
            rows = models.member_daily_counts_month(interaction.guild_id, user_id=uid, month=month)
            y, m = map(int, month.split("-"))
            days_in_month = monthrange(y, m)[1]
            per_day = {d: 0 for d in range(1, days_in_month + 1)}
            for dstr, cnt in rows:
                try:
                    d = int(dstr.split("-")[2])
                    if 1 <= d <= days_in_month:
                        per_day[d] = cnt
                except Exception:
                    continue

            xs = list(range(1, days_in_month + 1))
            ys = [per_day[d] for d in xs]

            title_suffix = f" — {member.display_name}" if member else ""
            if _HAS_MPL:
                try:
                    plt.figure(figsize=(8, 3))
                    plt.plot(xs, ys, marker="o")
                    plt.title(f"Messages per day — {month}{title_suffix}")
                    plt.xlabel("Day")
                    plt.ylabel("Messages")
                    plt.tight_layout()
                    buf = io.BytesIO()
                    plt.savefig(buf, format="png", dpi=200)
                    plt.close()
                    buf.seek(0)
                    filename = f"activity-graph-{interaction.guild_id}-{month}{'-'+str(uid) if uid else ''}.png"
                    file = discord.File(fp=buf, filename=filename)
                    await interaction.followup.send(file=file, ephemeral=not post)
                except Exception:
                    log.exception("activity.graph.image_failed", extra={"guild_id": interaction.guild_id})
                    # Fallback to text chart
                    graph_txt = _mini_text_graph(xs, ys, width=40, height=8)
                    await interaction.followup.send(
                        content=f"**Messages per day — {month}{title_suffix}**\n```\n{graph_txt}\n```",
                        ephemeral=not post
                    )
            else:
                # Text fallback
                graph_txt = _mini_text_graph(xs, ys, width=40, height=8)
                await interaction.followup.send(
                    content=f"**Messages per day — {month}{title_suffix}**\n```\n{graph_txt}\n```",
                    ephemeral=not post
                )

            log.info("activity.graph.used", extra={
                "guild_id": interaction.guild_id, "user_id": interaction.user.id, "month": month, "member": uid
            })
        except Exception:
            log.exception("activity.graph.failed", extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(name="export", description="Export activity as CSV for any scope/metric.")
    @app_commands.describe(
        scope="day/week/month/all",
        day="YYYY-MM-DD (for scope=day)",
        week="YYYY-Www (ISO week, e.g. 2025-W41)",
        month="YYYY-MM (for scope=month)",
        metric="messages, words, mentions, emoji_chat, or emoji_react",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="day", value="day"),
        app_commands.Choice(name="week", value="week"),
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions", value="mentions"),
        app_commands.Choice(name="emoji_chat", value="emoji_chat"),
        app_commands.Choice(name="emoji_react", value="emoji_react"),
    ])
    @app_commands.autocomplete(month=_month_autocomplete)
    async def export(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str] | None = None,
        day: Optional[str] = None,
        week: Optional[str] = None,
        month: Optional[str] = None,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            scope_val, key = _parse_scope_and_key(scope.value if scope else None, day, week, month)
            metric_val = (metric.value if metric else "messages")

            # Capability guards
            if metric_val == "words" and not _have_word_models():
                return await interaction.followup.send("Word-count tracking isn’t enabled in the backing storage.", ephemeral=not post)
            if metric_val == "mentions" and not _have_mention_models():
                return await interaction.followup.send("Mention tracking isn’t enabled.", ephemeral=not post)
            if metric_val in {"emoji_chat", "emoji_react"} and not _have_emoji_models():
                return await interaction.followup.send("Emoji tracking isn’t enabled.", ephemeral=not post)

            buf = StringIO()
            w = csv.writer(buf)

            # Fetch rows by metric/scope
            if scope_val == "all":
                if metric_val == "words":
                    rows = models.top_members_words_total(interaction.guild_id, limit=10_000)
                    header = ["guild_id", "user_id", "words"]
                elif metric_val == "mentions":
                    rows = models.top_members_mentions_total(interaction.guild_id, limit=10_000)
                    header = ["guild_id", "user_id", "mentions"]
                elif metric_val == "emoji_chat":
                    rows = models.top_members_emoji_chat_total(interaction.guild_id, limit=10_000)
                    header = ["guild_id", "user_id", "emoji_chat"]
                elif metric_val == "emoji_react":
                    rows = models.top_members_emoji_react_total(interaction.guild_id, limit=10_000)
                    header = ["guild_id", "user_id", "emoji_react"]
                else:
                    rows = models.top_members_total(interaction.guild_id, limit=10_000)
                    header = ["guild_id", "user_id", "messages"]
                w.writerow(header)
                for uid, cnt in rows:
                    w.writerow([interaction.guild_id, uid, cnt])
                filename = f"activity-{interaction.guild_id}-all-{metric_val}.csv"
            else:
                if metric_val == "words":
                    rows = models.top_members_words_period(interaction.guild_id, scope=scope_val, key=key, limit=10_000)
                    header = ["guild_id", scope_val, "user_id", "words"]
                elif metric_val == "mentions":
                    rows = models.top_members_mentions_period(interaction.guild_id, scope=scope_val, key=key, limit=10_000)
                    header = ["guild_id", scope_val, "user_id", "mentions"]
                elif metric_val == "emoji_chat":
                    rows = models.top_members_emoji_chat_period(interaction.guild_id, scope=scope_val, key=key, limit=10_000)
                    header = ["guild_id", scope_val, "user_id", "emoji_chat"]
                elif metric_val == "emoji_react":
                    rows = models.top_members_emoji_react_period(interaction.guild_id, scope=scope_val, key=key, limit=10_000)
                    header = ["guild_id", scope_val, "user_id", "emoji_react"]
                else:
                    rows = models.top_members_messages_period(interaction.guild_id, scope=scope_val, key=key, limit=10_000)
                    header = ["guild_id", scope_val, "user_id", "messages"]
                w.writerow(header)
                for uid, cnt in rows:
                    w.writerow([interaction.guild_id, key, uid, cnt])
                filename = f"activity-{interaction.guild_id}-{scope_val}-{key}-{metric_val}.csv"

            data = buf.getvalue().encode("utf-8")
            file = discord.File(fp=BytesIO(data), filename=filename)
            await interaction.followup.send(file=file, ephemeral=not post)

            log.info(
                "activity.export.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "scope": scope_val,
                    "key": key,
                    "rows": len(rows),
                    "metric": metric_val,
                    "post": post,
                },
            )
        except ValueError as ve:
            key = str(ve)
            if key == "bad_month_format":
                msg = S("activity.bad_month_format")
            elif key == "bad_day_format":
                msg = "Use YYYY-MM-DD for day."
            elif key == "bad_week_format":
                msg = "Use YYYY-Www (e.g. 2025-W41) for week."
            else:
                msg = S("common.error_generic")
            await interaction.followup.send(msg, ephemeral=not post)
        except Exception:
            log.exception(
                "activity.export.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)


    @group.command(name="reset", description="ADMIN: reset activity stats for a scope/metric.")
    @app_commands.describe(
        scope="day/week/month/all",
        day="YYYY-MM-DD (for scope=day)",
        week="YYYY-Www",
        month="YYYY-MM",
        metric="messages, words, mentions, emoji_chat, or emoji_react",
        post="If true, post publicly in this channel"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="day", value="day"),
        app_commands.Choice(name="week", value="week"),
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="all", value="all"),
    ])
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions", value="mentions"),
        app_commands.Choice(name="emoji_chat", value="emoji_chat"),
        app_commands.Choice(name="emoji_react", value="emoji_react"),
    ])
    @app_commands.default_permissions(manage_guild=True)
    async def reset(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str],
        day: Optional[str] = None,
        week: Optional[str] = None,
        month: Optional[str] = None,
        metric: app_commands.Choice[str] | None = None,
        post: bool = False
    ):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        metric_val = (metric.value if metric else "messages")

        try:
            scope_val, key = _parse_scope_and_key(scope.value, day, week, month)
        except ValueError as ve:
            code = str(ve)
            if code == "bad_month_format":
                return await interaction.response.send_message(S("activity.bad_month_format"), ephemeral=not post)
            if code == "bad_day_format":
                return await interaction.response.send_message("Use YYYY-MM-DD for day.", ephemeral=not post)
            if code == "bad_week_format":
                return await interaction.response.send_message("Use YYYY-Www (e.g. 2025-W41) for week.", ephemeral=not post)
            return await interaction.response.send_message(S("common.error_generic"), ephemeral=not post)

        try:

            if metric_val == "words":
                if hasattr(models, "reset_member_words"):
                    models.reset_member_words(interaction.guild_id, scope=scope_val, key=key)
                else:
                    raise NotImplementedError
            elif metric_val == "mentions":
                if hasattr(models, "reset_member_mentions"):
                    models.reset_member_mentions(interaction.guild_id, scope=scope_val, key=key)
                else:
                    raise NotImplementedError
            elif metric_val == "emoji_chat":
                if hasattr(models, "reset_member_emoji_chat"):
                    models.reset_member_emoji_chat(interaction.guild_id, scope=scope_val, key=key)
                else:
                    raise NotImplementedError
            elif metric_val == "emoji_react":
                if hasattr(models, "reset_member_emoji_react"):
                    models.reset_member_emoji_react(interaction.guild_id, scope=scope_val, key=key)
                else:
                    raise NotImplementedError
            else:
                if hasattr(models, "reset_member_activity"):
                    models.reset_member_activity(interaction.guild_id, scope=scope_val, key=key)
                else:
                    raise NotImplementedError

            msg = f"Reset {metric_val} for {('all-time' if scope_val=='all' else f'{scope_val}={key}')}."
            await interaction.response.send_message(msg, ephemeral=not post)
            log.warning(
                "activity.reset.done",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope_val, "key": key, "metric": metric_val, "post": post},
            )
        except NotImplementedError:
            await interaction.response.send_message("Reset not supported for this metric in storage.", ephemeral=True)
        except Exception:
            log.exception(
                "activity.reset.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "scope": scope_val, "key": key, "metric": metric_val},
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


def _mini_text_graph(xs: List[int], ys: List[int], width: int = 40, height: int = 8) -> str:
    """Renders a tiny ASCII chart where x is day (compressed) and y is count."""
    if not ys:
        return "(no data)"
    max_y = max(ys) or 1
    # downsample xs/ys to width
    n = len(xs)
    if n > width:
        step = n / width
        buckets = []
        for i in range(width):
            start = int(i * step)
            end = int((i + 1) * step)
            end = max(end, start + 1)
            buckets.append(max(ys[start:end]))
        ys_ds = buckets
    else:
        ys_ds = ys + [0] * (width - n)

    # build grid (height rows, width cols)
    grid = [[" " for _ in range(width)] for _ in range(height)]
    for x, val in enumerate(ys_ds[:width]):
        bar_h = int(round((val / max_y) * (height - 1)))
        for y in range(bar_h + 1):
            grid[height - 1 - y][x] = "▇"

    lines = ["".join(row) for row in grid]
    return "\n".join(lines)


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityCog(bot))
