from __future__ import annotations

import asyncio
import io
import logging
import re
from datetime import datetime, timezone
from typing import Awaitable, Callable, Dict, Iterable, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import commands, tasks

from ..db import connect
from ..models import activity, emoji_stats, rpg, message_archive
from ..strings import S
from ..ui.activity import (
    LESBIAN_COLORS,
    build_metric_leaderboard_embed as _build_metric_leaderboard_embed,
    build_profile_embed as _build_profile_embed,
    build_rank_embed as _build_rank_embed,
    area_with_vertical_gradient as _area_with_vertical_gradient,
    format_hour_range_local as _fmt_hour_range_local,
    require_guild as _require_guild,
)
from ..utils.activity import (
    CUSTOM_EMOJI_RE,
    DAY_RE,
    GIF_DOMAINS,
    PIN_FALLBACK_XP,
    PIN_MULTIPLIER,
    ROLE_XP_MULTIPLIERS,
    UNICODE_EMOJI_RE,
    XP_MULTIPLIERS,
    MULTIPLIER_DEFAULT,
    channel_multiplier as _ch_mult,
    count_emojis_text as _count_emojis_text,
    count_words as _count_words,
    day_default as _day_default,
    ensure_matplotlib_environment,
    gif_source_from_url as _gif_source_from_url,
    is_emoji_only as _is_emoji_only,
    month_default as _month_default,
    now_iso as _now_iso,
    parse_scope_and_key as _parse_scope_and_key,
    prime_window_from_hist as _prime_window_from_hist,
    role_multiplier as _role_mult,
    day_key as _day_key,
    MONTH_RE,
    WEEK_RE,
    week_default as _week_default,
    xp_multiplier as _xp_mult,
)

ensure_matplotlib_environment()

MENTION_RE = re.compile(r"<@!?(\d+)>")

try:
    import matplotlib.pyplot as plt
    from matplotlib.colors import LinearSegmentedColormap

    _HAS_MPL = True
except Exception:
    _HAS_MPL = False

log = logging.getLogger(__name__)

# =========================
# Activity Cog (+MMO)
# =========================
class ActivityCog(commands.GroupCog, name="activity", description="Member activity + RPG"):
    """Activity tracking + RPG progression (roles & pins multipliers, GIFs)."""

    def __init__(self, bot: commands.Bot):
        super().__init__()
        self.bot = bot
        # track voice sessions (join -> leave)
        self._vc_sessions: dict[tuple[int,int], dict] = {}  # (guild_id, user_id) -> {joined: dt, ch_id: int, stream_on: bool}

        # Cache per-message XP at creation for pin-bonus
        self._msg_xp: Dict[tuple[int, int], int] = {}          # (guild_id, message_id) -> xp_awarded_for_that_message
        self._pin_awarded: Set[tuple[int, int]] = set()        # ensure bonus is applied once

        # Joins de-dup: (guild_id, user_id, app_name, YYYY-MM-DD)
        self._join_seen: Set[tuple[int, int, str, str]] = set()

        self._poll_presence.start()

    def cog_unload(self):
        self._poll_presence.cancel()

    # ---------- internals ----------
    def _maybe_count_join(self, guild_id: int, user_id: int, app_name: str, when_dt: Optional[datetime] = None) -> None:
        """Count one 'activity_joins' per user/app/day. Idempotent per day."""
        day = _day_key(when_dt)
        key = (int(guild_id), int(user_id), str(app_name)[:80], day)
        if key in self._join_seen:
            return
        self._join_seen.add(key)
        try:
            activity.bump_activity_join(guild_id, user_id, when_iso=_now_iso(), app_name=app_name, joins=1)
        except Exception:
            log.exception("bump.activity_join_failed", extra={"guild_id": guild_id, "user_id": user_id, "app": app_name})

    # ---------- Events ----------
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return
        gid = message.guild.id
        uid = message.author.id
        ch = message.channel
        member = message.author if isinstance(message.author, discord.Member) else message.guild.get_member(uid)

        when = _now_iso()
        mult = _xp_mult(member, ch)

        # Track how much XP we award for THIS message (for potential pin-bonus)
        total_xp_for_msg = 0

        # 1) messages
        try:
            activity.bump_member_message(gid, uid, when_iso=when, inc=1)
            activity.bump_channel_message_total(gid, uid, getattr(ch, "id", 0), 1)
            base = rpg.XP_RULES["messages"]
            rpg.award_xp_for_event(gid, uid, base, mult)
            total_xp_for_msg += int(base * mult)
        except Exception:
            log.exception("bump.messages_failed", extra={"guild_id": gid, "user_id": uid})

        # 2) words
        try:
            wc = _count_words(message.content)
            if wc > 0:
                activity.bump_member_words(gid, uid, when_iso=when, inc=wc)
                add = (wc // 20) * rpg.XP_RULES["words_per_20"]
                if add:
                    rpg.award_xp_for_event(gid, uid, add, mult)
                    total_xp_for_msg += int(add * mult)
        except Exception:
            log.exception("bump.words_failed", extra={"guild_id": gid, "user_id": uid})

        # 3) mentions: credit RECEIVED and SENT
        try:
            mentioned_ids = {m.id for m in message.mentions if not m.bot}
            for mid in mentioned_ids:
                activity.bump_member_mentioned(gid, mid, when_iso=when, inc=1)
                rec_member = message.guild.get_member(mid)
                rec_mult = _xp_mult(rec_member, ch)
                rpg.award_xp_for_event(gid, mid, rpg.XP_RULES["mentions_received"], rec_mult)
            if mentioned_ids:
                activity.bump_member_mentions_sent(gid, uid, when_iso=when, inc=len(mentioned_ids))
                base_sent = rpg.XP_RULES["mentions_sent"] * len(mentioned_ids)
                rpg.award_xp_for_event(gid, uid, base_sent, mult)
                total_xp_for_msg += int(base_sent * mult)
        except Exception:
            log.exception("bump.mentions_failed", extra={"guild_id": gid})

        # 4) emoji in chat
        try:
            ec = _count_emojis_text(message.content)
            if ec > 0:
                activity.bump_member_emoji_chat(gid, uid, when_iso=when, inc=ec)
                base_emoji = rpg.XP_RULES["emoji_chat"] * ec
                rpg.award_xp_for_event(gid, uid, base_emoji, mult)
                total_xp_for_msg += int(base_emoji * mult)
        except Exception:
            log.exception("bump.emoji_chat_failed", extra={"guild_id": gid, "user_id": uid})
        # 4b) emoji-only message => DEX-flavored signal (no extra XP)
        try:
            if message.content and _is_emoji_only(message.content):
                activity.bump_member_emoji_only(gid, uid, when_iso=when, inc=1)
        except Exception:
            log.exception("bump.emoji_only_failed", extra={"guild_id": gid, "user_id": uid})

        # 5) stickers
        try:
            if message.stickers:
                for st in message.stickers:
                    emoji_stats.bump_sticker_usage(gid, when, sticker_id=st.id, sticker_name=(st.name or ""), inc=1)
                base_st = rpg.XP_RULES["sticker_use"] * len(message.stickers)
                rpg.award_xp_for_event(gid, uid, base_st, mult)
                total_xp_for_msg += int(base_st * mult)
        except Exception:
            log.exception("bump.sticker_failed", extra={"guild_id": gid, "user_id": uid})

        # 5b) GIFs (attachments, embeds, URLs)
        try:
            gif_count = 0

            # Attachments marked as GIF
            for att in message.attachments or []:
                filename = (att.filename or "").lower()
                ctype = (att.content_type or "").lower() if hasattr(att, "content_type") else ""
                if filename.endswith(".gif") or "gif" in ctype:
                    gif_count += 1
                    emoji_stats.bump_gif_usage(gid, when, att.url, "discord", 1)

            # Embeds that look like GIFs
            for em in message.embeds or []:
                try:
                    u = (getattr(em, "url", None) or getattr(getattr(em, "thumbnail", None), "url", None) or "")
                    if isinstance(u, str) and u:
                        if u.lower().endswith(".gif") or any(d in u for d in GIF_DOMAINS):
                            gif_count += 1
                            emoji_stats.bump_gif_usage(gid, when, u, _gif_source_from_url(u), 1)
                except Exception:
                    pass

            # Raw URLs in content
            if message.content:
                for tok in message.content.split():
                    if tok.startswith("http://") or tok.startswith("https://"):
                        u = tok.strip("<>")
                        if u.lower().endswith(".gif") or any(d in u for d in GIF_DOMAINS):
                            gif_count += 1
                            emoji_stats.bump_gif_usage(gid, when, u, _gif_source_from_url(u), 1)

            if gif_count > 0:
                activity.bump_member_gifs(gid, uid, when_iso=when, inc=gif_count)
                base_gif = rpg.XP_RULES["gif_use"] * gif_count
                rpg.award_xp_for_event(gid, uid, base_gif, mult)
                total_xp_for_msg += int(base_gif * mult)
        except Exception:
            log.exception("bump.gif_failed", extra={"guild_id": gid, "user_id": uid})

        # 6) emoji catalog monthly (best-effort, no XP)
        try:
            if message.content:
                for m in CUSTOM_EMOJI_RE.finditer(message.content):
                    emoji_stats.bump_emoji_usage(gid, when, f"custom:{m.group(1)}", "", True, False, 1)
                for ch_ in UNICODE_EMOJI_RE.findall(message.content):
                    key = "uni:" + "-".join(f"{ord(c):X}" for c in ch_)
                    emoji_stats.bump_emoji_usage(gid, when, key, "", False, False, 1)
        except Exception:
            pass

        # store XP for potential pin bonus
        try:
            self._msg_xp[(gid, message.id)] = total_xp_for_msg
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
        member = None
        if guild:
            member = guild.get_member(uid)
            if member and member.bot:
                return

        ch = guild.get_channel(payload.channel_id) if guild else None
        mult = _xp_mult(member, ch)

        # award reactor
        try:
            activity.bump_member_emoji_react(gid, uid, when_iso=when, inc=1)
            rpg.award_xp_for_event(gid, uid, rpg.XP_RULES["emoji_react"], mult)
        except Exception:
            log.exception("bump.emoji_react_failed", extra={"guild_id": gid, "user_id": uid})

        # credit reaction RECEIVED to the message author
        try:
            if guild and ch and hasattr(ch, "fetch_message"):
                msg = await ch.fetch_message(payload.message_id)
                if msg and msg.author and not msg.author.bot:
                    activity.bump_reactions_received(gid, msg.author.id, when, 1)
                    author_member = guild.get_member(msg.author.id)
                    recv_mult = _xp_mult(author_member, ch)
                    rpg.award_xp_for_event(gid, msg.author.id, rpg.XP_RULES["reactions_received"], recv_mult)
        except Exception:
            pass

        # monthly emoji catalog (no XP)
        try:
            em = payload.emoji
            if getattr(em, "id", None):
                emoji_stats.bump_emoji_usage(gid, when, f"custom:{int(em.id)}", str(em.name or ""), True, True, 1)
            else:
                ch_ = str(em)
                key = "uni:" + "-".join(f"{ord(c):X}" for c in ch_)
                emoji_stats.bump_emoji_usage(gid, when, key, "", False, True, 1)
        except Exception:
            pass

    @commands.Cog.listener("on_backread_archive_batch")
    async def _on_backread_archive_batch(self, rows: List[message_archive.ArchivedMessage]):
        for row in rows:
            try:
                self._process_archived_message(row)
            except Exception:
                log.exception(
                    "activity.backread.process_failed",
                    extra={"guild_id": getattr(row, "guild_id", None), "message_id": getattr(row, "message_id", None)},
                )

    async def replay_archived_messages(
        self,
        rows: Iterable[message_archive.ArchivedMessage],
        *,
        yield_every: int = 500,
        progress_cb: Callable[[int], Awaitable[None]] | None = None,
    ) -> int:
        processed = 0
        for row in rows:
            try:
                self._process_archived_message(row)
            except Exception:
                log.exception(
                    "activity.backread.process_failed",
                    extra={"guild_id": getattr(row, "guild_id", None), "message_id": getattr(row, "message_id", None)},
                )
            else:
                processed += 1
                if yield_every and processed % yield_every == 0:
                    if progress_cb is not None:
                        try:
                            await progress_cb(processed)
                        except Exception:
                            log.debug("activity.replay.progress_cb_failed", exc_info=True)
                    await asyncio.sleep(0)

        if progress_cb is not None:
            try:
                await progress_cb(processed)
            except Exception:
                log.debug("activity.replay.progress_cb_failed", exc_info=True)
        return processed

    def _process_archived_message(self, row: message_archive.ArchivedMessage) -> None:
        gid = int(row.guild_id)
        uid = int(row.author_id)
        when = row.created_at or _now_iso()
        channel_id = int(row.channel_id)

        guild = self.bot.get_guild(gid)
        channel: Optional[discord.abc.GuildChannel] = None
        member: Optional[discord.Member] = None
        if guild:
            channel = guild.get_channel(channel_id)
            if channel is None:
                try:
                    channel = guild.get_thread(channel_id)  # type: ignore[attr-defined]
                except Exception:
                    channel = None
            member = guild.get_member(uid)

        mult = _xp_mult(member, channel)

        try:
            activity.bump_member_message(gid, uid, when_iso=when, inc=1)
            activity.bump_channel_message_total(gid, uid, channel_id, 1)
            base = rpg.XP_RULES["messages"]
            rpg.award_xp_for_event(gid, uid, base, mult)
        except Exception:
            log.exception("activity.backread.bump_message_failed", extra={"guild_id": gid, "user_id": uid})

        content = row.content or ""

        try:
            wc = _count_words(content)
            if wc > 0:
                activity.bump_member_words(gid, uid, when_iso=when, inc=wc)
                add = (wc // 20) * rpg.XP_RULES["words_per_20"]
                if add:
                    rpg.award_xp_for_event(gid, uid, add, mult)
        except Exception:
            log.exception("activity.backread.words_failed", extra={"guild_id": gid, "user_id": uid})

        try:
            mention_ids = self._extract_mentions(content)
            filtered_mentions: List[int] = []
            for mid in mention_ids:
                rec_member = guild.get_member(mid) if guild else None
                if rec_member and rec_member.bot:
                    continue
                activity.bump_member_mentioned(gid, mid, when_iso=when, inc=1)
                rec_mult = _xp_mult(rec_member, channel)
                rpg.award_xp_for_event(gid, mid, rpg.XP_RULES["mentions_received"], rec_mult)
                filtered_mentions.append(mid)
            if filtered_mentions:
                base_sent = rpg.XP_RULES["mentions_sent"] * len(filtered_mentions)
                activity.bump_member_mentions_sent(gid, uid, when_iso=when, inc=len(filtered_mentions))
                rpg.award_xp_for_event(gid, uid, base_sent, mult)
        except Exception:
            log.exception("activity.backread.mentions_failed", extra={"guild_id": gid, "user_id": uid})

        try:
            ec = _count_emojis_text(content)
            if ec > 0:
                activity.bump_member_emoji_chat(gid, uid, when_iso=when, inc=ec)
                base_emoji = rpg.XP_RULES["emoji_chat"] * ec
                rpg.award_xp_for_event(gid, uid, base_emoji, mult)
            if content and _is_emoji_only(content):
                activity.bump_member_emoji_only(gid, uid, when_iso=when, inc=1)
        except Exception:
            log.exception("activity.backread.emoji_failed", extra={"guild_id": gid, "user_id": uid})

    def _extract_mentions(self, content: str) -> Set[int]:
        ids: Set[int] = set()
        for match in MENTION_RE.findall(content or ""):
            try:
                ids.add(int(match))
            except ValueError:
                continue
        return ids

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
            # Count a **join** once (used by WIS). App name "Voice".
            self._maybe_count_join(gid, uid, app_name="Voice", when_dt=now)
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
                activity.bump_voice_minutes(gid, uid, when, minutes, stream_minutes)
                voice_mult = max(MULTIPLIER_DEFAULT, float(XP_MULTIPLIERS.get(info["ch_id"], MULTIPLIER_DEFAULT)))
                voice_mult *= _role_mult(member)
                if minutes:
                    rpg.award_xp_for_event(gid, uid, rpg.XP_RULES["voice_minutes"] * minutes, voice_mult)
                if stream_minutes:
                    rpg.award_xp_for_event(gid, uid, rpg.XP_RULES["voice_stream_minutes"] * stream_minutes, voice_mult)

    # ---- Presence poll: approximate “Activities” minutes + joins ----
    @tasks.loop(minutes=5)
    async def _poll_presence(self):
        # lightweight: walk guilds; for each member’s current activities, if any app-like, add +5 minutes
        now_dt = datetime.now(timezone.utc)
        now_iso = now_dt.isoformat(timespec="seconds")
        for guild in list(self.bot.guilds):
            try:
                for m in list(guild.members):
                    if m.bot:
                        continue
                    apps = [a for a in (m.activities or []) if getattr(a, "name", None)]
                    if not apps:
                        continue
                    # credit each distinct app 5 minutes, and count **one join per app per day**
                    names = {str(getattr(a, "name", "")[:64]) for a in apps if a}
                    for nm in names:
                        # minutes
                        activity.bump_activity_minutes(guild.id, m.id, now_iso, nm, minutes=5, launches=0)
                        rpg.award_xp_for_event(guild.id, m.id, rpg.XP_RULES["activity_minutes"] * 5, _role_mult(m))
                        # join (once/day/app)
                        self._maybe_count_join(guild.id, m.id, app_name=nm, when_dt=now_dt)
            except Exception:
                continue

    @_poll_presence.before_loop
    async def _before_poll_presence(self):
        await self.bot.wait_until_ready()

    # ---- Pin bonus: detect pin status toggle ----
    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        """
        When a message becomes pinned, grant an XP bonus to its author:
        bonus = (PIN_MULTIPLIER - 1) * xp_awarded_for_that_message
        Fallback to PIN_FALLBACK_XP if we didn't record the message’s XP.
        """
        try:
            if not payload.guild_id or not isinstance(payload.data, dict):
                return
            data = payload.data
            if not data.get("pinned"):
                return

            gid = int(payload.guild_id)
            mid = int(data.get("id") or 0)
            if not mid:
                return
            key = (gid, mid)
            if key in self._pin_awarded:
                return

            guild = self.bot.get_guild(gid)
            if not guild:
                return

            ch_id = int(data.get("channel_id") or 0)
            ch = guild.get_channel(ch_id) if ch_id else None

            msg = None
            if ch and hasattr(ch, "fetch_message"):
                try:
                    msg = await ch.fetch_message(mid)
                except Exception:
                    msg = None

            base_recorded = self._msg_xp.pop(key, None)
            if base_recorded is not None:
                bonus = int((max(PIN_MULTIPLIER, 1.0) - 1.0) * base_recorded)
                if msg and msg.author and not msg.author.bot and bonus > 0:
                    rpg.award_xp_for_event(gid, msg.author.id, bonus, 1.0)
                self._pin_awarded.add(key)
                return

            if msg and msg.author and not msg.author.bot:
                mult = _xp_mult(msg.author if isinstance(msg.author, discord.Member) else guild.get_member(msg.author.id), ch)
                rpg.award_xp_for_event(gid, msg.author.id, PIN_FALLBACK_XP, mult)
                self._pin_awarded.add(key)
                return

            self._pin_awarded.add(key)
        except Exception:
            log.exception("pin_bonus.failed", extra={"payload": str(getattr(payload, "data", None))})

    # ---------- Slash commands ----------

    async def _month_autocomplete(self, inter: discord.Interaction, current: str):
        gid = inter.guild_id
        try:
            available = activity.available_months(gid) or []
        except Exception:
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

    # ------- /activity rank (by level) -------
    @app_commands.command(name="rank", description="Top members by Level.")
    @app_commands.describe(limit="How many to list (5–50)", post="Post publicly?")
    async def rank(self, interaction: discord.Interaction,
                   limit: app_commands.Range[int, 5, 50] = 20,
                   post: bool = False):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        rows = rpg.top_levels(interaction.guild_id, int(limit))
        if not rows:
            return await interaction.followup.send(S("activity.leaderboard.empty"), ephemeral=not post)

        embed = _build_metric_leaderboard_embed(
            guild=interaction.guild,
            metric_name="Levels",
            scope_label="all time",
            rows=[(int(uid), int(level)) for uid, level, _ in rows],
        )
        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- /activity me (profile) -------
    @app_commands.command(
        name="me",
        description="Profile: level, stats, derived metrics, voice, activities. You can inspect someone else."
    )
    @app_commands.describe(
        user="Whose profile to view (default: you)",
        month="Highlight YYYY-MM (optional)",
        post="Post publicly?"
    )
    @app_commands.autocomplete(month=_month_autocomplete)
    async def me(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        month: Optional[str] = None,
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        target: discord.Member = user or interaction.user
        gid, uid = interaction.guild_id, target.id
        month = month or _month_default()
        if not MONTH_RE.match(month):
            return await interaction.followup.send("Use YYYY-MM for month.", ephemeral=not post)

        # RPG
        progress = rpg.get_rpg_progress(gid, uid)
        lvl, cur, need = rpg.xp_progress(progress["xp"])

        # Totals helper
        def tot(metric: str) -> int:
            with connect() as con:
                cur_ = con.cursor()
                row = cur_.execute("""
                    SELECT count FROM member_metrics_total
                    WHERE guild_id=? AND user_id=? AND metric=?
                """, (gid, uid, metric)).fetchone()
                return int(row[0]) if row else 0

        messages        = tot("messages")
        words           = tot("words")
        mentions_recv   = tot("mentions")
        mentions_sent   = tot("mentions_sent")
        emoji_chat      = tot("emoji_chat")
        emoji_react     = tot("emoji_react")
        reacts_recv     = tot("reactions_received")
        voice_min       = tot("voice_minutes")
        stream_min      = tot("voice_stream_minutes")
        act_min         = tot("activity_minutes")
        act_joins       = tot("activity_joins")

        # Deriveds
        _safe = lambda a, b: (a / b) if b > 0 else 0.0
        engagement_ratio = _safe(reacts_recv, messages)
        reply_density    = _safe(mentions_sent, messages)
        mention_depth    = _safe(mentions_recv, messages)
        media_ratio      = 0.0
        burstiness       = 0.0

        # Prime hour & channel
        try:
            hist = activity.member_hour_histogram_total(gid, uid, tz="America/Los_Angeles")
            s1, e1, _ = _prime_window_from_hist(list(hist), window=1)
            prime_hour = _fmt_hour_range_local(s1, e1, "PT")
        except Exception:
            prime_hour = "N/A"
        ch_id = activity.prime_channel_total(gid, uid)
        prime_channel = f"<#{ch_id}>" if ch_id else "N/A"

        embed = _build_profile_embed(
            target=target,
            level=lvl,
            total_xp=progress["xp"],
            progress_current=cur,
            progress_needed=need,
            stats={
                "str": progress["str"],
                "dex": progress["dex"],
                "int": progress["int"],
                "wis": progress["wis"],
                "cha": progress["cha"],
                "vit": progress["vit"],
            },
            engagement_ratio=engagement_ratio,
            reply_density=reply_density,
            mention_depth=mention_depth,
            media_ratio=media_ratio,
            burstiness=burstiness,
            prime_hour=prime_hour,
            prime_channel=prime_channel,
            voice_minutes=voice_min,
            stream_minutes=stream_min,
            activity_minutes=act_min,
            activity_joins=act_joins,
        )

        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- Master export (everything) -------
    @app_commands.command(name="export_master", description="Export a master report: totals, RPG, derived.")
    @app_commands.describe(post="Post publicly?")
    async def export_master(self, interaction: discord.Interaction, post: bool = False):
        import csv
        from io import StringIO, BytesIO

        if not await _require_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        gid = interaction.guild_id
        users: set[int] = set()
        with connect() as con:
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
            "voice_minutes","voice_stream_minutes","activity_minutes","activity_joins",
            "prime_channel",
        ]
        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(head)

        def _tot(uid: int, metric: str) -> int:
            with connect() as con:
                cur = con.cursor()
                row = cur.execute("""
                    SELECT count FROM member_metrics_total
                    WHERE guild_id=? AND user_id=? AND metric=?
                """, (gid, uid, metric)).fetchone()
                return int(row[0]) if row else 0

        for uid in sorted(users):
            progress = rpg.get_rpg_progress(gid, uid)
            ch_id = activity.prime_channel_total(gid, uid) or 0
            w.writerow([
                gid, uid, progress["level"], progress["xp"],
                progress["str"], progress["dex"], progress["int"], progress["wis"], progress["cha"], progress["vit"],
                _tot(uid,"messages"), _tot(uid,"words"), _tot(uid,"mentions"), _tot(uid,"mentions_sent"),
                _tot(uid,"emoji_chat"), _tot(uid,"emoji_react"), _tot(uid,"reactions_received"),
                _tot(uid,"voice_minutes"), _tot(uid,"voice_stream_minutes"), _tot(uid,"activity_minutes"), _tot(uid,"activity_joins"),
                ch_id,
            ])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(fp=BytesIO(data), filename=f"activity-master-{gid}.csv")
        await interaction.followup.send(file=file, ephemeral=not post)

    # ------- /activity top -------
    @app_commands.command(name="top", description="Leaderboard for a metric.")
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
        app_commands.Choice(name="activity_joins", value="activity_joins"),
        app_commands.Choice(name="gifs", value="gifs"),
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

        # archive-aware leaderboards for messages/words; fall back for others
        rows: List[Tuple[int, int]] = []
        if s == "all":
            rows = activity.top_members_total_merged(gid, metric_name, int(limit))
        else:
            rows = activity.top_members_period_merged(gid, metric_name, s, key, int(limit))

        if not rows:
            return await interaction.followup.send(S("activity.leaderboard.empty"), ephemeral=not post)

        title_scope = s if s == "all" else f"{s}:{key}"
        embed = _build_metric_leaderboard_embed(
            guild=interaction.guild,
            metric_name=metric_name,
            scope_label=title_scope,
            rows=[(int(uid), int(cnt)) for uid, cnt in rows],
        )
        await interaction.followup.send(embed=embed, ephemeral=not post)

    # ------- /activity graph -------
    @app_commands.command(name="graph", description="Plot daily messages for a month (guild or a user).")
    @app_commands.describe(
        month="YYYY-MM (default: current month)",
        user="Optional: pick a member to graph",
        pretty="Use gradient area style (lesbian colors)",
        post="Post publicly?"
    )
    @app_commands.autocomplete(month=_month_autocomplete)
    async def graph(self,
                interaction: discord.Interaction,
                month: Optional[str] = None,
                user: Optional[discord.Member] = None,
                pretty: bool = True,
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
        rows = activity.member_daily_counts_month(gid, uid, month)
        if not rows:
            return await interaction.followup.send("No data for that month.", ephemeral=not post)

        xs = [d for (d, _) in rows]
        ys = [int(c) for (_, c) in rows]
        x_idx = list(range(len(xs)))

        import matplotlib.pyplot as plt
        from matplotlib.colors import LinearSegmentedColormap

        fig = plt.figure(figsize=(max(8, len(xs)*0.25), 4.5), dpi=160)
        ax = fig.add_subplot(111)
        fig.patch.set_facecolor("#111214")
        ax.set_facecolor("#111214")
        ax.tick_params(colors="#C9CDD2")
        ax.yaxis.label.set_color("#C9CDD2")
        ax.xaxis.label.set_color("#C9CDD2")
        for spine in ax.spines.values():
            spine.set_color("#2A2D31")

        try:
            cmap = LinearSegmentedColormap.from_list("lesbian", LESBIAN_COLORS)
        except Exception:
            cmap = None

        title_suffix = f" — {user.display_name}" if user else " — Server"
        ax.set_title(f"Messages per day — {month}{title_suffix}", color="#E7E9EC", pad=10)
        ax.set_ylabel("Messages", color="#C9CDD2")

        if pretty and cmap is not None:
            smoothed = ys
            _area_with_vertical_gradient(ax, x_idx, smoothed, cmap)
            ax.set_ylim(bottom=0)
            ax.grid(axis="y", linestyle="--", alpha=0.15, color="#60646C")
        else:
            colors = [cmap(i/len(x_idx)) if cmap else None for i in range(len(x_idx))]
            ax.bar(x_idx, ys, align="center", color=colors, zorder=2)
            ax.grid(axis="y", linestyle="--", alpha=0.3, color="#60646C")

        ax.set_xticks(x_idx)
        ax.set_xticklabels([x.split("-")[-1] for x in xs], rotation=0, color="#C9CDD2")

        buf = io.BytesIO()
        fig.tight_layout()
        fig.savefig(buf, format="png", facecolor=fig.get_facecolor(), edgecolor="none")
        plt.close(fig)
        buf.seek(0)

        file = discord.File(buf, filename=f"activity-{month}" + (f"-{user.id}" if user else "") + ( "-pretty" if pretty else "" ) + ".png")
        await interaction.followup.send(file=file, ephemeral=not post)

    # ------- /activity export -------
    @app_commands.command(name="export", description="Export a CSV for a metric and month.")
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
        app_commands.Choice(name="activity_joins", value="activity_joins"),
        app_commands.Choice(name="gifs", value="gifs"),
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

        with connect() as con:
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
    @app_commands.command(name="reset", description="Admin: reset metrics (careful).")
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
        app_commands.Choice(name="activity_joins", value="activity_joins"),
        app_commands.Choice(name="gifs", value="gifs"),
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

        try:
            if m == "messages":
                activity.reset_member_activity(gid, scope=s, key=k)
            else:
                activity._reset_metric(gid, metric=m, scope=s, key=k)
        except Exception:
            log.exception("reset.failed", extra={"guild_id": gid, "metric": m, "scope": s, "key": k})
            return await interaction.followup.send("Reset failed. Check logs.", ephemeral=not post)

        await interaction.followup.send(f"Reset **{m}** for **{s}{f'={k}' if k else ''}**.", ephemeral=not post)

    @reset.error
    async def _reset_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("You need **Manage Server** to do that.", ephemeral=True)
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityCog(bot))
