from __future__ import annotations

import io
import logging
import re
from calendar import monthrange
from datetime import datetime, timezone, timedelta
from typing import Optional, List, Tuple, Dict

import discord
from discord import app_commands
from discord.ext import commands, tasks

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

# =========================
# Config: XP Multipliers
# =========================
# You can edit these lists, or wire commands later to set them per-guild.
XP_MULTIPLIERS: Dict[int, float] = {}  # channel_id -> 2.0 / 4.0 / 8.0 etc.
MULTIPLIER_DEFAULT = 1.0

# ex:
# XP_MULTIPLIERS.update({
#     123456789012345678: 2.0,  # lounge
#     234567890123456789: 4.0,  # events
#     345678901234567890: 8.0,  # elite pit
# })

# Regex & constants (same as before) ----------------
WORD_RE = re.compile(r"\b\w+\b", flags=re.UNICODE)
CUSTOM_EMOJI_RE = re.compile(r"<a?:\w+:(\d+)>")
UNICODE_EMOJI_RE = re.compile(r"[\U0001F300-\U0001FAFF\U00002700-\U000027BF\U00002600-\U000026FF]")
MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
DAY_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])$")
WEEK_RE = re.compile(r"^\d{4}-W(0[1-9]|[1-4]\d|5[0-3])$")
PT_TZNAME = "America/Los_Angeles"

try:
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap
    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

LESBIAN_COLORS = ["#D52D00","#EF7627","#FF9A56","#FFFFFF","#D162A4","#B55690","#A30262"]

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
    return 0 if not text else len(WORD_RE.findall(text))

def _count_emojis_text(text: str | None) -> int:
    if not text:
        return 0
    return len(CUSTOM_EMOJI_RE.findall(text)) + len(UNICODE_EMOJI_RE.findall(text))

def _ch_mult(ch: discord.abc.GuildChannel | None) -> float:
    if not ch:
        return MULTIPLIER_DEFAULT
    return XP_MULTIPLIERS.get(getattr(ch, "id", 0), MULTIPLIER_DEFAULT)

async def _require_guild(inter: discord.Interaction) -> bool:
    if not inter.guild:
        if not inter.response.is_done():
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
        else:
            await inter.followup.send(S("common.guild_only"), ephemeral=True)
        return False
    return True

def _fmt_rank(rows: list[tuple[int, int]], guild: discord.Guild, limit: int) -> str:
    lines: List[str] = []
    for i, (uid, cnt) in enumerate(rows[:limit], start=1):
        m = guild.get_member(uid)
        name = m.mention if m else f"<@{uid}>"
        lines.append(f"{i}. {name} — **{cnt}**")
    return "\n".join(lines) if lines else S("activity.leaderboard.empty")

def _prime_window_from_hist(hour_counts: List[int], window: int = 1) -> tuple[int, int, int]:
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

def _parse_scope_and_key(scope: str | None, day: str | None, week: str | None, month: str | None) -> tuple[str, Optional[str]]:
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
        s = "all"; key = None
    return s, key

# =========================
# Activity Cog (+MMO)
# =========================
class ActivityCog(commands.Cog):
    """Activity tracking + RPG progression."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # track voice sessions (join -> leave)
        self._vc_sessions: dict[tuple[int,int], dict] = {}  # (guild_id, user_id) -> {joined: dt, ch_id: int, stream_on: bool}
        self._poll_presence.start()

    def cog_unload(self):
        self._poll_presence.cancel()

    # ---------- Events ----------
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        gid = message.guild.id
        uid = message.author.id
        ch = message.channel
        when = _now_iso()
        mult = _ch_mult(ch)

        # 1) messages
        try:
            models.bump_member_message(gid, uid, when_iso=when, inc=1)
            models.bump_channel_message_total(gid, uid, getattr(ch, "id", 0), 1)
            # XP
            models.award_xp_for_event(gid, uid, models.XP_RULES["messages"], mult)
        except Exception:
            log.exception("bump.messages_failed", extra={"guild_id": gid, "user_id": uid})

        # 2) words
        try:
            wc = _count_words(message.content)
            if wc > 0:
                models.bump_member_words(gid, uid, when_iso=when, inc=wc)
                # XP: +2 per 20 words
                add = (wc // 20) * models.XP_RULES["words_per_20"]
                if add:
                    models.award_xp_for_event(gid, uid, add, mult)
        except Exception:
            log.exception("bump.words_failed", extra={"guild_id": gid, "user_id": uid})

        # 3) mentions: credit RECEIVED and SENT
        try:
            mentioned_ids = {m.id for m in message.mentions if not m.bot}
            for mid in mentioned_ids:
                models.bump_member_mentioned(gid, mid, when_iso=when, inc=1)
                models.award_xp_for_event(gid, mid, models.XP_RULES["mentions_received"], mult)
            if mentioned_ids:
                models.bump_member_mentions_sent(gid, uid, when_iso=when, inc=len(mentioned_ids))
                models.award_xp_for_event(gid, uid, models.XP_RULES["mentions_sent"] * len(mentioned_ids), mult)
        except Exception:
            log.exception("bump.mentions_failed", extra={"guild_id": gid})

        # 4) emoji in chat
        try:
            ec = _count_emojis_text(message.content)
            if ec > 0:
                models.bump_member_emoji_chat(gid, uid, when_iso=when, inc=ec)
                models.award_xp_for_event(gid, uid, models.XP_RULES["emoji_chat"] * ec, mult)
        except Exception:
            log.exception("bump.emoji_chat_failed", extra={"guild_id": gid, "user_id": uid})

        # 5) stickers
        try:
            if message.stickers:
                for st in message.stickers:
                    models.bump_sticker_usage(gid, when, sticker_id=st.id, sticker_name=(st.name or ""), inc=1)
                models.award_xp_for_event(gid, uid, models.XP_RULES["sticker_use"] * len(message.stickers), mult)
        except Exception:
            log.exception("bump.sticker_failed", extra={"guild_id": gid, "user_id": uid})

        # 6) emoji catalog monthly (best-effort, no XP)
        try:
            if message.content:
                for m in CUSTOM_EMOJI_RE.finditer(message.content):
                    models.bump_emoji_usage(gid, when, f"custom:{m.group(1)}", "", True, False, 1)
                for ch_ in UNICODE_EMOJI_RE.findall(message.content):
                    key = "uni:" + "-".join(f"{ord(c):X}" for c in ch_)
                    models.bump_emoji_usage(gid, when, key, "", False, False, 1)
        except Exception:
            pass

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if payload.guild_id is None or payload.user_id is None:
            return
        gid = int(payload.guild_id)
        uid = int(payload.user_id)
        when = _now_iso()
        guild = self.bot.get_guild(gid)
        # ignore bot reactors
        if guild:
            m = guild.get_member(uid)
            if m and m.bot:
                return

        ch = guild.get_channel(payload.channel_id) if guild else None
        mult = _ch_mult(ch)

        # award reactor
        try:
            models.bump_member_emoji_react(gid, uid, when_iso=when, inc=1)
            models.award_xp_for_event(gid, uid, models.XP_RULES["emoji_react"], mult)
        except Exception:
            log.exception("bump.emoji_react_failed", extra={"guild_id": gid, "user_id": uid})

        # credit reaction RECEIVED to the message author
        try:
            if guild and ch and hasattr(ch, "fetch_message"):
                msg = await ch.fetch_message(payload.message_id)
                if msg and msg.author and not msg.author.bot:
                    models.bump_reactions_received(gid, msg.author.id, when, 1)
                    models.award_xp_for_event(gid, msg.author.id, models.XP_RULES["reactions_received"], _ch_mult(ch))
        except Exception:
            # ignore fetch failures (rate limits or perms)
            pass

        # monthly emoji catalog (no XP)
        try:
            em = payload.emoji
            if getattr(em, "id", None):
                models.bump_emoji_usage(gid, when, f"custom:{int(em.id)}", str(em.name or ""), True, True, 1)
            else:
                ch_ = str(em)
                key = "uni:" + "-".join(f"{ord(c):X}" for c in ch_)
                models.bump_emoji_usage(gid, when, key, "", False, True, 1)
        except Exception:
            pass

    # ---- Voice sessions: minutes & streaming ----
    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if member.bot or not member.guild:
            return
        gid, uid = member.guild.id, member.id
        key = (gid, uid)
        now = datetime.now(timezone.utc)

        # join session
        if before.channel is None and after.channel is not None:
            self._vc_sessions[key] = {"joined": now, "ch_id": after.channel.id, "stream_on": bool(after.self_stream)}
            return

        # update streaming flag
        if key in self._vc_sessions and after.channel is not None:
            self._vc_sessions[key]["stream_on"] = bool(after.self_stream)

        # left channel or moved
        if before.channel is not None and (after.channel is None or after.channel.id != before.channel.id):
            info = self._vc_sessions.pop(key, None)
            if info:
                minutes = max(0, int((now - info["joined"]).total_seconds() // 60))
                stream_minutes = minutes if info.get("stream_on") else 0
                when = _now_iso()
                models.bump_voice_minutes(gid, uid, when, minutes, stream_minutes)
                # XP with channel multiplier of the channel they were in
                mult = XP_MULTIPLIERS.get(info["ch_id"], MULTIPLIER_DEFAULT)
                if minutes:
                    models.award_xp_for_event(gid, uid, models.XP_RULES["voice_minutes"] * minutes, mult)
                if stream_minutes:
                    models.award_xp_for_event(gid, uid, models.XP_RULES["voice_stream_minutes"] * stream_minutes, mult)

    # ---- Presence poll: approximate “Activities” minutes ----
    @tasks.loop(minutes=5)
    async def _poll_presence(self):
        # lightweight: walk guilds; for each member’s current activities, if any app-like, add +5 minutes
        now_iso = _now_iso()
        for guild in list(self.bot.guilds):
            try:
                for m in list(guild.members):
                    if m.bot:
                        continue
                    apps = [a for a in m.activities or [] if getattr(a, "name", None)]
                    if not apps:
                        continue
                    # credit each distinct app 5 minutes
                    names = {str(getattr(a, "name", "")[:64]) for a in apps if a}
                    for nm in names:
                        models.bump_activity_minutes(guild.id, m.id, now_iso, nm, minutes=5, launches=0)
                        # XP (no channel multiplier context; use 1.0)
                        models.award_xp_for_event(guild.id, m.id, models.XP_RULES["activity_minutes"] * 5, 1.0)
            except Exception:
                # never bring the bot down
                continue

    @_poll_presence.before_loop
    async def _before_poll_presence(self):
        await self.bot.wait_until_ready()

    # ---------- Slash commands ----------
    group = app_commands.Group(name="activity", description="Member activity + RPG")

    async def _month_autocomplete(self, inter: discord.Interaction, current: str):
        gid = inter.guild_id
        try:
            available = models.available_months(gid) or []
        except Exception:
            available = []
        if not available:
            now = datetime.now(timezone.utc)
            y, m = now.year, now.month
            for _ in range(12):
                available.append(f"{y:04d}-{m:02d}")
                m -= 1
                if m == 0: m = 12; y -= 1
        filtered = [c for c in available if c.startswith(current)] if current else available
        return [app_commands.Choice(name=c, value=c) for c in filtered[:25]]

    # existing /activity top, /activity graph, /activity export, /activity reset
    # (use the versions from the previous cog you pasted — unchanged)

    # ------- New: /activity rank (by level) -------
    @group.command(name="rank", description="Top members by Level.")
    @app_commands.describe(limit="How many to list (5–50)", post="Post publicly?")
    async def rank(self, interaction: discord.Interaction,
                   limit: app_commands.Range[int, 5, 50] = 20,
                   post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        rows = models.top_levels(interaction.guild_id, int(limit))
        if not rows:
            return await interaction.followup.send("No one has started their journey yet.", ephemeral=not post)
        lines = []
        for i, (uid, lvl, xp) in enumerate(rows, start=1):
            m = interaction.guild.get_member(uid)
            name = m.mention if m else f"<@{uid}>"
            lines.append(f"{i}. {name} — **Lv {lvl}** ({xp} XP)")
        embed = discord.Embed(title=S("activity.rank.title"), description="\n".join(lines), color=discord.Color.gold())
        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- Upgraded: /activity me (full profile) -------
    @group.command(name="me_plus", description="Your full profile: level, stats, derived metrics, voice, activities.")
    @app_commands.describe(month="Highlight YYYY-MM (optional)", post="Post publicly?")
    @app_commands.autocomplete(month=_month_autocomplete)
    async def me_plus(self, interaction: discord.Interaction, month: Optional[str] = None, post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        gid, uid = interaction.guild_id, interaction.user.id
        month = month or _month_default()
        if not MONTH_RE.match(month):
            return await interaction.followup.send("Use YYYY-MM for month.", ephemeral=not post)

        # RPG
        rpg = models.get_rpg_progress(gid, uid)
        lvl, cur, need = models.xp_progress(rpg["xp"])

        # Totals for derived metrics
        def tot(metric: str) -> int:
            with models.connect() as con:
                cur_ = con.cursor()
                row = cur_.execute("""
                    SELECT count FROM member_metrics_total
                    WHERE guild_id=? AND user_id=? AND metric=?
                """, (gid, uid, metric)).fetchone()
                return int(row[0]) if row else 0

        messages = tot("messages")
        words = tot("words")
        mentions_recv = tot("mentions")
        mentions_sent = tot("mentions_sent")
        emoji_chat = tot("emoji_chat")
        emoji_react = tot("emoji_react")
        reacts_recv = tot("reactions_received")
        voice_min = tot("voice_minutes")
        stream_min = tot("voice_stream_minutes")
        act_min = tot("activity_minutes")

        # Deriveds (guard zero-div)
        def _safe(a, b): return (a / b) if b > 0 else 0.0
        engagement_ratio = _safe(reacts_recv, messages)
        reply_density = 0.0
        # replies: approximate from mentions_sent as proxy (or implement a replies metric later)
        reply_density = _safe(mentions_sent, messages)
        mention_depth = _safe(mentions_sent, messages)
        media_ratio = 0.0  # add attachments metric later if needed
        burstiness = 0.0   # requires per-hour stddev; can compute if you want using hour hist
        response_latency = "N/A"  # non-trivial without a reply-tracker

        # Prime hour & channel
        try:
            hist = models.member_hour_histogram_total(gid, uid, tz="America/Los_Angeles")
            s1, e1, _ = _prime_window_from_hist(list(hist), window=1)
            prime_hour = _fmt_hour_range_local(s1, e1, "PT")
        except Exception:
            prime_hour = "N/A"
        ch_id = models.prime_channel_total(gid, uid)
        prime_channel = f"<#{ch_id}>" if ch_id else "N/A"

        # Embed
        embed = discord.Embed(
            title=S("activity.profile.title", user=interaction.user),
            color=discord.Color.purple()
        )
        # Level
        pct = int(round((cur / need) * 100)) if need > 0 else 100
        embed.add_field(name=S("activity.profile.level"), value=f"**Lv {lvl}** — {rpg['xp']} XP\nProgress: {cur}/{need} ({pct}%)", inline=False)
        # Stats
        stats = f"**STR** {rpg['str']}  **DEX** {rpg['dex']}  **INT** {rpg['int']}  **WIS** {rpg['wis']}  **CHA** {rpg['cha']}  **VIT** {rpg['vit']}"
        embed.add_field(name=S("activity.profile.stats"), value=stats, inline=False)
        # Derived
        derived = (
            f"Engagement ratio: **{engagement_ratio:.2f}**\n"
            f"Reply density: **{reply_density:.2f}**\n"
            f"Mention depth: **{mention_depth:.2f}**\n"
            f"Media ratio: **{media_ratio:.2f}**\n"
            f"Burstiness: **{burstiness:.2f}**\n"
            f"Prime hour: **{prime_hour}**\n"
            f"Prime channel: {prime_channel}"
        )
        embed.add_field(name=S("activity.profile.derived"), value=derived, inline=False)
        # Voice / Activities
        embed.add_field(name=S("activity.profile.voice"), value=f"Voice: **{voice_min}** min · Streaming: **{stream_min}** min", inline=True)
        embed.add_field(name=S("activity.profile.apps"), value=f"Activities: **{act_min}** min", inline=True)

        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- Master export (everything) -------
    @group.command(name="export_master", description="Export a master report: totals, RPG, derived.")
    @app_commands.describe(post="Post publicly?")
    async def export_master(self, interaction: discord.Interaction, post: bool = False):
        import csv
        from io import StringIO, BytesIO

        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        gid = interaction.guild_id
        # build union of users seen in RPG or totals
        users: set[int] = set()
        with models.connect() as con:
            cur = con.cursor()
            for row in cur.execute("SELECT DISTINCT user_id FROM member_metrics_total WHERE guild_id=?", (gid,)):
                users.add(int(row[0]))
            for row in cur.execute("SELECT DISTINCT user_id FROM member_rpg_progress WHERE guild_id=?", (gid,)):
                users.add(int(row[0]))

        head = [
            "guild_id","user_id","level","xp",
            "str","dex","int","wis","cha","vit",
            "messages","words","mentions_recv","mentions_sent",
            "emoji_chat","emoji_react","reactions_recv",
            "voice_minutes","voice_stream_minutes","activity_minutes",
            "prime_channel",
        ]
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(head)

        def _tot(uid: int, metric: str) -> int:
            with models.connect() as con:
                cur = con.cursor()
                row = cur.execute("""
                    SELECT count FROM member_metrics_total
                    WHERE guild_id=? AND user_id=? AND metric=?
                """, (gid, uid, metric)).fetchone()
                return int(row[0]) if row else 0

        for uid in sorted(users):
            rpg = models.get_rpg_progress(gid, uid)
            ch_id = models.prime_channel_total(gid, uid) or 0
            w.writerow([
                gid, uid, rpg["level"], rpg["xp"],
                rpg["str"], rpg["dex"], rpg["int"], rpg["wis"], rpg["cha"], rpg["vit"],
                _tot(uid,"messages"), _tot(uid,"words"), _tot(uid,"mentions"), _tot(uid,"mentions_sent"),
                _tot(uid,"emoji_chat"), _tot(uid,"emoji_react"), _tot(uid,"reactions_received"),
                _tot(uid,"voice_minutes"), _tot(uid,"voice_stream_minutes"), _tot(uid,"activity_minutes"),
                ch_id,
            ])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(fp=BytesIO(data), filename=f"activity-master-{gid}.csv")
        await interaction.followup.send(file=file, ephemeral=not post)

    # ------- /activity top -------
    @group.command(name="top", description="Leaderboard for a metric.")
    @app_commands.describe(
        metric="Which metric to rank by",
        scope="day/week/month/all",
        day="YYYY-MM-DD (for scope=day)",
        week="YYYY-Www (for scope=week)",
        month="YYYY-MM (for scope=month)",
        limit="How many to show (5–50)",
        post="Post publicly?"
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions (received)", value="mentions"),
        app_commands.Choice(name="mentions_sent", value="mentions_sent"),
        app_commands.Choice(name="emoji_chat (in text)", value="emoji_chat"),
        app_commands.Choice(name="emoji_react (you reacted)", value="emoji_react"),
        app_commands.Choice(name="reactions_received", value="reactions_received"),
        app_commands.Choice(name="voice_minutes", value="voice_minutes"),
        app_commands.Choice(name="voice_stream_minutes", value="voice_stream_minutes"),
        app_commands.Choice(name="activity_minutes", value="activity_minutes"),
    ])
    async def top(self,
                  interaction: discord.Interaction,
                  metric: app_commands.Choice[str],
                  scope: Optional[str] = "month",
                  day: Optional[str] = None,
                  week: Optional[str] = None,
                  month: Optional[str] = None,
                  limit: app_commands.Range[int, 5, 50] = 20,
                  post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        gid = interaction.guild_id
        metric_name = metric.value

        # parse scope/key
        try:
            s, key = _parse_scope_and_key(scope, day, week, month)
        except ValueError as e:
            return await interaction.followup.send(f"Bad {str(e).replace('_',' ')}.", ephemeral=not post)

        rows: List[Tuple[int, int]] = []

        # Use unified tables directly to support any metric
        with models.connect() as con:
            cur = con.cursor()
            if s == "all":
                rows = cur.execute(
                    """
                    SELECT user_id, count
                    FROM member_metrics_total
                    WHERE guild_id=? AND metric=?
                    ORDER BY count DESC
                    LIMIT ?
                    """, (gid, metric_name, int(limit))
                ).fetchall()
            else:
                where_col = {"day": "day", "week": "week", "month": "month"}[s]
                rows = cur.execute(
                    f"""
                    SELECT user_id, SUM(count) AS c
                    FROM member_metrics_daily
                    WHERE guild_id=? AND metric=? AND {where_col}=?
                    GROUP BY user_id
                    ORDER BY c DESC
                    LIMIT ?
                    """, (gid, metric_name, key, int(limit))
                ).fetchall()

        if not rows:
            return await interaction.followup.send(S("activity.leaderboard.empty"), ephemeral=not post)

        # format
        lines = []
        for i, (uid, cnt) in enumerate(rows, start=1):
            m = interaction.guild.get_member(int(uid))
            name = m.mention if m else f"<@{int(uid)}>"
            lines.append(f"{i}. {name} — **{int(cnt)}** {metric_name}")

        title_scope = s if s == "all" else f"{s}:{key}"
        embed = discord.Embed(
            title=f"Top {metric_name} — {title_scope}",
            description="\n".join(lines),
            color=discord.Color.blurple()
        )
        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- /activity graph -------
    @group.command(name="graph", description="Plot daily messages for a month (guild or a user).")
    @app_commands.describe(
        month="YYYY-MM (default: current month)",
        user="Optional: pick a member to graph",
        post="Post publicly?"
    )
    @app_commands.autocomplete(month=_month_autocomplete)
    async def graph(self,
                    interaction: discord.Interaction,
                    month: Optional[str] = None,
                    user: Optional[discord.Member] = None,
                    post: bool = False):
        if not await _require_guild(interaction):
            return
        if not _HAS_MPL:
            return await interaction.response.send_message("Matplotlib not available on this runtime.", ephemeral=True)

        await interaction.response.defer(ephemeral=not post)
        gid = interaction.guild_id
        month = month or _month_default()
        if not MONTH_RE.match(month):
            return await interaction.followup.send("Use YYYY-MM for month.", ephemeral=not post)

        uid = user.id if user else None
        rows = models.member_daily_counts_month(gid, uid, month)
        if not rows:
            return await interaction.followup.send("No data for that month.", ephemeral=not post)

        # Build X/Y
        xs = [d for (d, _) in rows]
        ys = [int(c) for (_, c) in rows]

        # Pre-size figure
        import matplotlib.pyplot as plt
        from matplotlib.colors import LinearSegmentedColormap
        fig = plt.figure(figsize=(max(8, len(xs)*0.25), 4.5), dpi=160)

        # Gradient bar color
        try:
            cmap = LinearSegmentedColormap.from_list("lesbian", LESBIAN_COLORS)
        except Exception:
            cmap = None

        ax = fig.add_subplot(111)
        ax.bar(range(len(xs)), ys, align="center",
               color=[cmap(i/len(xs)) if cmap else None for i in range(len(xs))])
        ax.set_title(f"Messages per day — {month}" + (f" — {user.display_name}" if user else " — Server"))
        ax.set_ylabel("Messages")
        ax.set_xticks(range(len(xs)))
        ax.set_xticklabels([x.split("-")[-1] for x in xs], rotation=0)
        ax.grid(axis="y", linestyle="--", alpha=0.3)

        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png")
        plt.close(fig)
        buf.seek(0)

        file = discord.File(buf, filename=f"activity-{month}" + (f"-{uid}" if uid else "") + ".png")
        await interaction.followup.send(file=file, ephemeral=not post)

    # ------- /activity export -------
    @group.command(name="export", description="Export a CSV for a metric and month.")
    @app_commands.describe(
        metric="Metric to export",
        month="YYYY-MM (default: current month)",
        post="Post publicly?"
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions", value="mentions"),
        app_commands.Choice(name="mentions_sent", value="mentions_sent"),
        app_commands.Choice(name="emoji_chat", value="emoji_chat"),
        app_commands.Choice(name="emoji_react", value="emoji_react"),
        app_commands.Choice(name="reactions_received", value="reactions_received"),
        app_commands.Choice(name="voice_minutes", value="voice_minutes"),
        app_commands.Choice(name="voice_stream_minutes", value="voice_stream_minutes"),
        app_commands.Choice(name="activity_minutes", value="activity_minutes"),
    ])
    async def export(self,
                     interaction: discord.Interaction,
                     metric: app_commands.Choice[str],
                     month: Optional[str] = None,
                     post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        gid = interaction.guild_id
        month = month or _month_default()
        if not MONTH_RE.match(month):
            return await interaction.followup.send("Use YYYY-MM for month.", ephemeral=not post)

        # Pull all per-day counts for the month
        with models.connect() as con:
            cur = con.cursor()
            rows = cur.execute("""
                SELECT user_id, day, count
                FROM member_metrics_daily
                WHERE guild_id=? AND metric=? AND month=?
                ORDER BY user_id ASC, day ASC
            """, (gid, metric.value, month)).fetchall()

        import csv
        from io import StringIO, BytesIO
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(["guild_id", "metric", "user_id", "day", "count"])
        for uid, day, cnt in rows:
            w.writerow([gid, metric.value, int(uid), str(day), int(cnt)])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(fp=BytesIO(data), filename=f"activity-{metric.value}-{month}-{gid}.csv")
        await interaction.followup.send(file=file, ephemeral=not post)

    # ------- /activity reset -------
    @group.command(name="reset", description="Admin: reset metrics (careful).")
    @app_commands.describe(
        metric="Which metric to reset (messages has special legacy handling)",
        scope="day/week/month/all",
        key="Key for day/week/month (YYYY-MM-DD / YYYY-Www / YYYY-MM). Ignored for all.",
        post="Post publicly?"
    )
    @app_commands.choices(metric=[
        app_commands.Choice(name="messages", value="messages"),
        app_commands.Choice(name="words", value="words"),
        app_commands.Choice(name="mentions", value="mentions"),
        app_commands.Choice(name="mentions_sent", value="mentions_sent"),
        app_commands.Choice(name="emoji_chat", value="emoji_chat"),
        app_commands.Choice(name="emoji_react", value="emoji_react"),
        app_commands.Choice(name="reactions_received", value="reactions_received"),
        app_commands.Choice(name="voice_minutes", value="voice_minutes"),
        app_commands.Choice(name="voice_stream_minutes", value="voice_stream_minutes"),
        app_commands.Choice(name="activity_minutes", value="activity_minutes"),
    ])
    @app_commands.checks.has_permissions(manage_guild=True)
    async def reset(self,
                    interaction: discord.Interaction,
                    metric: app_commands.Choice[str],
                    scope: str,
                    key: Optional[str] = None,
                    post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        gid = interaction.guild_id
        m = metric.value
        s = scope
        k = key

        # Validate key formats
        try:
            if s == "day" and (not k or not DAY_RE.match(k)):
                raise ValueError("Use YYYY-MM-DD for day")
            if s == "week" and (not k or not WEEK_RE.match(k)):
                raise ValueError("Use YYYY-Www for week")
            if s == "month" and (not k or not MONTH_RE.match(k)):
                raise ValueError("Use YYYY-MM for month")
            if s == "all":
                k = None
        except ValueError as e:
            return await interaction.followup.send(str(e), ephemeral=not post)

        # For messages, use special extended routine to keep legacy mirrors in sync
        try:
            if m == "messages":
                models.reset_member_activity(gid, scope=s, key=k)
            else:
                models._reset_metric(gid, metric=m, scope=s, key=k)
        except Exception:
            log.exception("reset.failed", extra={"guild_id": gid, "metric": m, "scope": s, "key": k})
            return await interaction.followup.send("Reset failed. Check logs.", ephemeral=not post)

        await interaction.followup.send(f"Reset **{m}** for **{s}{f'={k}' if k else ''}**.", ephemeral=not post)

    # ---- permissions failure for reset ----
    @reset.error
    async def _reset_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        else:
            raise error
