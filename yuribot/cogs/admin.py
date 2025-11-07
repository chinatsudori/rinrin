# /app/yuribot/cogs/admin.py
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import time
import re
import zipfile
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Set
from datetime import datetime, timezone
import discord
from discord import app_commands
from discord.ext import commands
from ..db import connect   # <-- needed for voice_stats to query voice_minutes_day

from ..models import voice as voice_model
from ..models import activity, activity_report, message_archive, rpg
from ..models.message_archive import ArchivedMessage  # type: ignore
from ..strings import S
from ..ui.admin import build_club_config_embed
from ..utils.admin import ensure_guild, validate_image_filename
from ..utils.cleanup import (
    DEFAULT_BOT_AUTHOR_ID,
    DEFAULT_FORUM_ID,
    collect_threads,
    has_purge_permissions,
    purge_messages_from_threads,
    resolve_forum_channel,
)
from ..utils.maintact import month_from_day

log = logging.getLogger(__name__)

# ---------- helpers / checks ----------


_JOIN_TITLES  = {"voice join",  "voice_join",  "join",  "voice connected"}
_LEAVE_TITLES = {"voice leave", "voice_leave", "leave", "voice disconnected"}
_ID_RE = re.compile(r"\((\d{10,})\)")

def _first_id(text: str | None) -> int | None:
    if not text: return None
    m = _ID_RE.search(text)
    return int(m.group(1)) if m else None

def _parse_voice_embed(msg: discord.Message) -> tuple[str|None,int|None,int|None,datetime|None]:
    """
    Returns (kind, user_id, channel_id, timestamp) OR (None, None, None, None) if not a voice log embed.
    We read: title, 'User' field, 'Channel' field, and use message.created_at as the time.
    """
    if not msg.embeds: return (None, None, None, None)
    emb = msg.embeds[0]
    title = (emb.title or "").strip().lower()
    is_join  = title in _JOIN_TITLES
    is_leave = title in _LEAVE_TITLES
    if not (is_join or is_leave): return (None, None, None, None)

    user_id = None
    chan_id = None
    # Prefer fields
    for f in emb.fields:
        name = (f.name or "").lower()
        val  = f.value or ""
        if "user" in name and user_id is None:
            user_id = _first_id(val)
        if "channel" in name and chan_id is None:
            chan_id = _first_id(val)
    # Fallback: description
    if user_id is None:
        user_id = _first_id(emb.description or "")
    if chan_id is None:
        chan_id = _first_id(emb.description or "")
    kind = "join" if is_join else "leave"
    ts = msg.created_at if msg.created_at.tzinfo else msg.created_at.replace(tzinfo=timezone.utc)
    return (kind, user_id, chan_id, ts)
def require_manage_guild() -> app_commands.Check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
            return False
        if not interaction.user.guild_permissions.manage_guild:  # type: ignore[attr-defined]
            await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)

BATCH_SIZE = 100
DELAY_BETWEEN_CHANNELS = 0.5

@dataclass(slots=True)
class BackreadStats:
    channels_scanned: int = 0
    threads_scanned: int = 0
    messages_archived: int = 0
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)

# ---------- unified /admin GroupCog ----------

class AdminCog(commands.GroupCog, name="admin", description="Admin tools"):
    group = app_commands.Group(name="admin", description="Admin tools")

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self) -> None:
        log.info("Loaded AdminCog with flattened admin commands")

    # ===== Core admin =====

    @app_commands.command(name="club_config", description="Show configured club IDs and assets.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def club_config(self, interaction: discord.Interaction, post: bool = False):
        from ..models import guilds
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            cfg = guilds.get_club_map(interaction.guild_id)
        except Exception:
            log.exception("admin.club_config.lookup_failed", extra={"guild_id": interaction.guild_id})
            return await interaction.followup.send(S("admin.club_config.error"), ephemeral=not post)

        pairs = [(club, str(info.get("club_id", "-"))) for club, info in cfg.items()]
        embed = build_club_config_embed(guild=interaction.guild, club_pairs=pairs)
        await interaction.followup.send(embed=embed, ephemeral=not post)

    @app_commands.command(name="set_image", description="Upload an image asset for a club.")
    @app_commands.describe(
        club_slug="Club slug (e.g. movie)",
        image="PNG/JPG file",
        filename="Optional filename (defaults to uploaded name)",
        post="If true, post publicly in this channel",
    )
    async def set_image(
        self,
        interaction: discord.Interaction,
        club_slug: str,
        image: discord.Attachment,
        filename: Optional[str] = None,
        post: bool = False,
    ):
        from ..models import guilds
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        name = filename or image.filename
        valid_name = validate_image_filename(name)
        if not valid_name:
            return await interaction.followup.send(S("admin.set_image.invalid_name"), ephemeral=not post)
        try:
            data = await image.read()
            guilds.store_club_image(interaction.guild_id, club_slug, valid_name, data)
        except Exception:
            log.exception("admin.set_image.store_failed", extra={"gid": interaction.guild_id, "club": club_slug})
            return await interaction.followup.send(S("admin.set_image.error"), ephemeral=not post)
        await interaction.followup.send(S("admin.set_image.ok"), ephemeral=not post)

    @app_commands.command(name="set_link", description="Set an external link for a club.")
    @app_commands.describe(
        club_slug="Club slug (e.g. movie)",
        url="URL to store",
        post="If true, post publicly in this channel",
    )
    async def set_link(self, interaction: discord.Interaction, club_slug: str, url: str, post: bool = False):
        from ..models import guilds
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)
        try:
            guilds.store_club_link(interaction.guild_id, club_slug, url)
        except Exception:
            log.exception("admin.set_link.store_failed", extra={"gid": interaction.guild_id, "club": club_slug})
            return await interaction.followup.send(S("admin.set_link.error"), ephemeral=not post)
        await interaction.followup.send(S("admin.set_link.ok"), ephemeral=not post)

    # ===== Sync helpers =====

    @app_commands.command(name="sync_guild", description="Force-sync slash commands to THIS guild (instant).")
    async def sync_guild(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in a guild.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            self.bot.tree.clear_commands(guild=interaction.guild)
            cmds = await self.bot.tree.sync(guild=interaction.guild)
            await interaction.followup.send(
                f"Synced **{len(cmds)}** command(s) for **{interaction.guild.name}**.", ephemeral=True
            )
        except Exception:
            log.exception("admin.sync_guild.failed", extra={"gid": interaction.guild_id})
            await interaction.followup.send("Guild sync failed. Check logs.", ephemeral=True)

    @app_commands.command(name="sync_global", description="Force-sync slash commands globally.")
    async def sync_global(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            cmds = await self.bot.tree.sync()
            await interaction.followup.send(
                f"Synced **{len(cmds)}** global command(s). Propagation may take up to ~1h.", ephemeral=True
            )
        except Exception:
            log.exception("admin.sync_global.failed")
            await interaction.followup.send("Global sync failed. Check logs.", ephemeral=True)

    @app_commands.command(name="show_tree", description="Show the locally-registered slash command paths (debug).")
    async def show_tree(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        lines: List[str] = []
        for cmd in self.bot.tree.get_commands():
            lines.append(f"/{cmd.name}")
            if hasattr(cmd, "commands"):
                for sub in cmd.commands:
                    lines.append(f"/{cmd.name} {sub.name}")
                    if hasattr(sub, "commands"):
                        for sub2 in sub.commands:
                            lines.append(f"/{cmd.name} {sub.name} {sub2.name}")
        await interaction.followup.send("\n".join(lines[:200]) or "(no commands registered locally)", ephemeral=True)

    # ===== Backread (flattened under /admin) =====

    def _br_label(self, ch: discord.abc.GuildChannel) -> str:
        name = getattr(ch, "name", str(ch.id))
        if isinstance(ch, discord.Thread) and ch.parent:
            return f"#{ch.parent.name} › #{name}"
        return f"#{name}"

    def _br_can(self, ch: discord.abc.GuildChannel, me: discord.Member) -> tuple[bool, str]:
        try:
            perms = ch.permissions_for(me)
        except Exception:
            return False, "unable to resolve permissions"
        if not perms.view_channel:
            return False, "missing View Channel"
        if not perms.read_message_history:
            return False, "missing Read Message History"
        return True, ""

    async def _br_threads(
        self, parent: discord.abc.GuildChannel, include_private: bool, me: discord.Member
    ) -> List[discord.Thread]:
        threads: List[discord.Thread] = [t for t in getattr(parent, "threads", []) if isinstance(t, discord.Thread)]
        seen = {t.id for t in threads}
        archived_iter = getattr(parent, "archived_threads", None)
        if callable(archived_iter):
            try:
                async for t in archived_iter(limit=None):
                    if isinstance(t, discord.Thread) and t.id not in seen:
                        threads.append(t)
                        seen.add(t.id)
            except discord.Forbidden:
                log.info("backread.threads.forbidden", extra={"gid": parent.guild.id, "cid": parent.id})
            except discord.HTTPException:
                log.exception("backread.threads.error", extra={"gid": parent.guild.id, "cid": parent.id})
        if include_private and isinstance(parent, discord.TextChannel) and callable(archived_iter):
            if not parent.permissions_for(me).manage_threads:
                return threads
            try:
                async for t in archived_iter(limit=None, private=True):  # type: ignore[arg-type]
                    if isinstance(t, discord.Thread) and t.id not in seen:
                        threads.append(t)
                        seen.add(t.id)
            except (discord.Forbidden, discord.HTTPException, TypeError):
                pass
        return threads

    async def _br_archive(
        self, ch: discord.abc.GuildChannel, stats: BackreadStats, *, is_thread: bool
    ) -> None:
        label = self._br_label(ch)
        gid = ch.guild.id  # type: ignore[assignment]
        last_id = message_archive.max_message_id(gid, ch.id)  # type: ignore[arg-type]
        history_kwargs = {"limit": None, "oldest_first": True}
        if last_id:
            history_kwargs["after"] = discord.Object(id=last_id)

        batch: List[ArchivedMessage] = []
        stored = 0
        latest_seen = last_id
        seen: set[int] = set()
        try:
            async for msg in ch.history(**history_kwargs):  # type: ignore[attr-defined]
                if latest_seen is not None and msg.id <= latest_seen:
                    continue
                try:
                    row = message_archive.from_discord_message(msg)
                except Exception as exc:
                    stats.errors.append(f"{label}: {exc}")
                    continue
                if row.message_id in seen:
                    continue
                seen.add(row.message_id)
                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    stored += message_archive.upsert_many(batch)
                    latest_seen = max(latest_seen or 0, batch[-1].message_id)
                    batch.clear()
        except discord.Forbidden:
            stats.skipped.append(f"{label} (forbidden)"); return
        except discord.HTTPException as exc:
            stats.errors.append(f"{label}: HTTP {getattr(exc,'status','?')}")
        finally:
            if batch:
                stored += message_archive.upsert_many(batch)
                latest_seen = max(latest_seen or 0, batch[-1].message_id)

        stats.messages_archived += stored
        if is_thread: stats.threads_scanned += 1
        else:         stats.channels_scanned += 1

    @app_commands.command(name="backread_start", description="Backread text channels into the message archive.")
    @app_commands.describe(
        channel="Limit to a specific text channel.",
        include_archived_threads="Also scan archived public threads.",
        include_private_threads="Scan private archived threads (TextChannels; requires Manage Threads).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def backread_start(
        self,
        interaction: discord.Interaction,
        channel: Optional[discord.TextChannel] = None,
        include_archived_threads: bool = True,
        include_private_threads: bool = False,
    ):
        if not await ensure_guild(interaction): return
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None: return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        me = guild.me
        if not isinstance(me, discord.Member):
            try: me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
            except Exception:
                log.exception("backread.fetch_member_failed", extra={"gid": guild.id})
                return await interaction.followup.send("Unable to resolve my member.", ephemeral=True)

        targets: List[discord.abc.GuildChannel] = []
        if channel: targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            try:
                targets.extend(list(guild.forum_channels))  # type: ignore[attr-defined]
            except AttributeError:
                targets.extend(ch for ch in guild.channels if isinstance(ch, discord.ForumChannel))

        stats = BackreadStats()
        for parent in targets:
            ok, why = self._br_can(parent, me)
            if not ok:
                stats.skipped.append(f"{self._br_label(parent)} ({why})"); continue
            counted = False
            if isinstance(parent, discord.TextChannel):
                await self._br_archive(parent, stats, is_thread=False); counted = True
            threads = await self._br_threads(parent, include_private_threads, me) if (include_archived_threads or include_private_threads) else [t for t in getattr(parent,"threads",[]) if isinstance(t, discord.Thread)]
            for t in threads:
                ok, why = self._br_can(t, me)
                if not ok: stats.skipped.append(f"{self._br_label(t)} ({why})"); continue
                await self._br_archive(t, stats, is_thread=True)
                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)
            if not counted: stats.channels_scanned += 1
            await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

        lines = [
            f"Archived **{stats.messages_archived:,}** messages.",
            f"Scanned **{stats.channels_scanned:,}** channels and **{stats.threads_scanned:,}** threads.",
            "New messages will be archived automatically.",
        ]
        if stats.skipped:
            prev = ", ".join(stats.skipped[:5]) + ("..." if len(stats.skipped) > 5 else "")
            lines.append(f"Skipped (perm): {prev}")
        if stats.errors:
            prev = "; ".join(stats.errors[:3]) + ("..." if len(stats.errors) > 3 else "")
            lines.append(f"Errors: {prev}")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(name="backread_stats", description="Show archive stats for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def backread_stats(self, interaction: discord.Interaction):
        if not await ensure_guild(interaction): return
        await interaction.response.defer(ephemeral=True)
        gid = interaction.guild_id
        try: summary = message_archive.stats_summary(gid)
        except Exception:
            log.exception("backread.stats.failed", extra={"gid": gid})
            return await interaction.followup.send("Archive stats unavailable.", ephemeral=True)
        lines = [
            f"Archive stats for **{interaction.guild.name}**",
            f"• Messages: **{int(summary.get('messages',0)):,}**",
            f"• Channels: **{int(summary.get('channels',0)):,}**",
            f"• Unique users: **{int(summary.get('users',0)):,}**",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(name="backread_audit", description="Audit readable channels/threads and what is skipped.")
    @app_commands.describe(
        channel="Limit to a specific text or forum channel.",
        include_archived_threads="Scan archived threads for each channel (public).",
        include_private_threads="Try private archived threads (TextChannels only; forums unsupported).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def backread_audit(
        self,
        interaction: discord.Interaction,
        channel: Optional[discord.abc.GuildChannel] = None,
        include_archived_threads: bool = True,
        include_private_threads: bool = True,
    ):
        if not await ensure_guild(interaction): return
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None: return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        me = guild.me
        if not isinstance(me, discord.Member):
            me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]

        targets: List[discord.abc.GuildChannel] = []
        if channel: targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            try: targets.extend(list(guild.forum_channels))  # type: ignore[attr-defined]
            except AttributeError:
                targets.extend(ch for ch in guild.channels if isinstance(ch, discord.ForumChannel))

        readable_channels = 0; skipped_channels: List[str]=[]
        total_threads=0; readable_threads=0; public_threads=0; private_threads=0; skipped_threads: List[str]=[]

        for parent in targets:
            ok, why = self._br_can(parent, me)
            if not ok: skipped_channels.append(f"{self._br_label(parent)} ({why})"); continue
            readable_channels += 1
            threads = await self._br_threads(parent, include_private_threads, me) if (include_archived_threads or include_private_threads) else [t for t in getattr(parent,"threads",[]) if isinstance(t, discord.Thread)]
            for t in threads:
                total_threads += 1
                ttype = getattr(t, "type", None)
                name = str(getattr(ttype, "name", ttype)).lower() if ttype else ""
                (private_threads if "private" in name else public_threads).__iadd__(1)
                ok, why = self._br_can(t, me)
                if ok: readable_threads += 1
                else:  skipped_threads.append(f"{self._br_label(t)} ({why})")

        lines = [
            f"**Audit for `{guild.name}`**",
            f"• Channels readable: **{readable_channels:,}**",
        ]
        if skipped_channels:
            prev = ", ".join(skipped_channels[:8]) + ("..." if len(skipped_channels)>8 else "")
            lines += [f"• Channels skipped (perm): **{len(skipped_channels):,}**", f"  ↳ {prev}"]
        lines += [
            f"• Threads discovered: **{total_threads:,}** (public: **{public_threads:,}**, private: **{private_threads:,}**)",
            f"• Threads readable now: **{readable_threads:,}**",
            "• Private threads (TextChannels): require **Manage Threads** to fetch archived ones.",
            "• Private forum threads are not retrievable on this discord.py version.",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="voice_import_log",
        description="Parse Voice Join/Leave embeds in a log channel and store historical minutes per user."
    )
    @app_commands.describe(
        log_channel="The bot-log channel that contains the voice join/leave embeds.",
        since_message_id="Optional: start AFTER this message id.",
        dry_run="If true, only report what would be inserted."
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def voice_import_log(
        self,
        interaction: discord.Interaction,
        log_channel: discord.TextChannel,
        since_message_id: Optional[int] = None,
        dry_run: bool = False,
    ):
        if not await ensure_guild(interaction): return
        await interaction.response.defer(ephemeral=True, thinking=True)

        voice_model.ensure_schema()

        guild = interaction.guild
        gid = guild.id  # type: ignore[assignment]
        after_obj = discord.Object(id=since_message_id) if since_message_id else None

        await interaction.edit_original_response(content="Scanning log channel history…")

        # Active sessions per user: user_id -> (channel_id, joined_at)
        active: dict[int, tuple[int, datetime]] = {}
        sessions: list[tuple[int,int,datetime,datetime]] = []  # (user_id, channel_id, join, leave)
        scanned = 0
        matched = 0

        try:
            async for m in log_channel.history(limit=None, oldest_first=True, after=after_obj):
                scanned += 1
                kind, user_id, channel_id, ts = _parse_voice_embed(m)
                if kind is None:
                    continue
                if user_id is None or channel_id is None or ts is None:
                    continue
                matched += 1
                if kind == "join":
                    # If user already has an open session, close it at the new join time (switch case)
                    if user_id in active:
                        prev_ch, prev_ts = active.pop(user_id)
                        if ts > prev_ts:
                            sessions.append((user_id, prev_ch, prev_ts, ts))
                    active[user_id] = (channel_id, ts)
                else:  # leave
                    if user_id in active:
                        prev_ch, prev_ts = active.pop(user_id)
                        if ts > prev_ts:
                            sessions.append((user_id, prev_ch, prev_ts, ts))
                    # else: stray leave; ignore
        except discord.Forbidden:
            return await interaction.edit_original_response(
                content="Forbidden: I need **Read Message History** in that channel."
            )

        # Close any dangling sessions at "now" (optional). Safer to drop.
        # If you prefer to close them, set end = discord.utils.utcnow()
        dangling = len(active)
        active.clear()

        # Persist
        total_sessions = len(sessions)
        total_minutes  = 0
        per_day_rollup: dict[tuple[int,int,str], int] = {}  # (gid, uid, day) -> minutes

        if not dry_run:
            for uid, cid, start, end in sessions:
                dur = voice_model.add_session(gid, uid, cid, start, end)
                mins = max(1, round(dur/60)) if dur else 0
                total_minutes += mins
                # explode across days
                # cheap split: credit all to the start day if < 24h; for precision use explode_minutes_per_day
                for day, m in voice_model.explode_minutes_per_day(gid, uid, start, end).items():
                    per_day_rollup[(gid, uid, day)] = per_day_rollup.get((gid, uid, day), 0) + int(m)
            # bulk upsert
            items = [(g,u,d,m) for (g,u,d),m in per_day_rollup.items()]
            voice_model.upsert_minutes_bulk(items)

        summary = [
            f"Scanned **{scanned:,}** messages.",
            f"Matched **{matched:,}** voice embeds.",
            f"Built **{total_sessions:,}** session(s).",
            f"Dangling sessions ignored: **{dangling}**.",
        ]
        if not dry_run:
            summary.append(f"Upserted **{len(per_day_rollup):,}** day-rows; ~**{total_minutes:,}** minutes total.")
        else:
            summary.append("DRY RUN — nothing written.")
        await interaction.edit_original_response(content="\n".join(summary))
    @app_commands.command(
        name="voice_stats",
        description="Show a user's total imported voice minutes (UTC days)."
    )
    @app_commands.describe(
        user="Target user (default: you).",
        limit="How many days to preview (default 10)."
    )
    async def voice_stats(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        limit: int = 10,
    ):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        gid = interaction.guild_id
        uid = (user or interaction.user).id

        with connect() as con:
            cur = con.cursor()
            cur.execute(
                """
                SELECT day, minutes
                FROM voice_minutes_day
                WHERE guild_id=? AND user_id=?
                ORDER BY day DESC
                LIMIT ?
                """,
                (gid, uid, limit),
            )
            rows = cur.fetchall()

            cur.execute(
                """
                SELECT COALESCE(SUM(minutes), 0)
                FROM voice_minutes_day
                WHERE guild_id=? AND user_id=?
                """,
                (gid, uid),
            )
            total = int(cur.fetchone()[0] or 0)
        lines = [f"Voice minutes for <@{uid}> (total **{total:,}**):"]
        for day, minutes in rows:
            lines.append(f"• {day}: **{int(minutes):,}**")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @app_commands.command(
        name="backread_export",
        description="Export a channel/thread to CSV. Fills missing messages first unless disabled.",
    )
    @app_commands.describe(
        channel="A TextChannel or Thread to export.",
        fill_missing="Fetch any messages not in the DB first (default: true).",
        compress="Zip the CSV if large (auto if >7.5MB).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def backread_export(
        self,
        interaction: discord.Interaction,
        channel: Optional[discord.abc.GuildChannel] = None,
        fill_missing: bool = True,
        compress: bool = False,
    ):
        if not await ensure_guild(interaction): return
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if guild is None: return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        target = channel
        if target is None: return await interaction.followup.send("Pick a specific text channel or thread.", ephemeral=True)
        if isinstance(target, discord.ForumChannel):
            return await interaction.followup.send("Export a **specific forum thread**, not the forum parent.", ephemeral=True)

        me = guild.me
        if not isinstance(me, discord.Member):
            me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
        ok, why = self._br_can(target, me)
        if not ok: return await interaction.followup.send(f"I can’t read {self._br_label(target)}: {why}.", ephemeral=True)

        label = self._br_label(target); gid=guild.id; cid=target.id
        progress = await interaction.followup.send(f"**Exporting {label}…**\nPreparing…", ephemeral=True)
        deadline = time.monotonic() + 12*60; last_edit=0.0; editable=True
        async def edit(stage:str,*,found:int=0,scanned:int=0,exported:int=0):
            nonlocal last_edit,editable
            if not editable or time.monotonic()>deadline: editable=False; return
            now=time.monotonic()
            if now-last_edit<1.0: return
            parts=[f"**Exporting {label}…**", f"• {stage}"]
            if fill_missing: parts.append(f"• Missing found this run: **{found:,}**")
            if scanned:      parts.append(f"• Messages scanned: **{scanned:,}**")
            if exported:     parts.append(f"• Rows written: **{exported:,}**")
            try: await progress.edit(content="\n".join(parts)); last_edit=now
            except discord.HTTPException as e:
                if getattr(e,"code",None)==50027 or e.status==401: editable=False

        # load existing ids
        await edit("Loading existing IDs from DB…"); existing:set[int]=set()
        try:
            con = message_archive.get_connection()
            with con:
                cur=con.cursor()
                cur.execute("SELECT message_id FROM message_archive WHERE guild_id=? AND channel_id=?", (gid, cid))
                for (mid,) in cur.fetchall(): existing.add(int(mid))
        except Exception:
            log.exception("backread.export.load_existing_failed", extra={"gid":gid,"cid":cid})
            try: await progress.edit(content=f"**Exporting {label}…**\nDB read failed while loading existing rows.")
            except Exception: pass
            return

        # fill gaps
        missing=0; scanned=0
        if fill_missing and hasattr(target,"history"):
            await edit("Scanning Discord history for gaps…", found=missing, scanned=scanned)
            batch:list[ArchivedMessage]=[]
            try:
                async for msg in target.history(limit=None, oldest_first=True):  # type: ignore[attr-defined]
                    scanned+=1
                    if msg.id in existing:
                        if scanned%250==0: await edit("Scanning Discord history for gaps…", found=missing, scanned=scanned)
                        continue
                    try: row = message_archive.from_discord_message(msg)
                    except Exception:
                        if scanned%250==0: await edit("Scanning Discord history for gaps…", found=missing, scanned=scanned)
                        continue
                    batch.append(row); existing.add(msg.id); missing+=1
                    if len(batch)>=200:
                        message_archive.upsert_many(batch); batch.clear()
                        await edit("Filling gaps (writing batch)…", found=missing, scanned=scanned)
                if batch: message_archive.upsert_many(batch); batch.clear(); await edit("Filling gaps (finalize)…", found=missing, scanned=scanned)
            except discord.Forbidden:
                try: await progress.edit(content=f"**Exporting {label}…**\nForbidden: need **Read Message History**.")
                except Exception: pass
                return
            except discord.HTTPException as e:
                log.exception("backread.export.history_http", extra={"status":getattr(e,'status','?')})
                try: await progress.edit(content=f"**Exporting {label}…**\nDiscord API error (HTTP {getattr(e,'status','?')}).")
                except Exception: pass
                return

        # dump CSV
        await edit("Dumping rows from DB to CSV…", found=missing, scanned=scanned)
        sbuf = io.StringIO(); w = csv.writer(sbuf, lineterminator="\n")
        w.writerow(["message_id","guild_id","channel_id","author_id","message_type","created_at","content","edited_at","attachments","embeds","reactions_json","reply_to_id"])
        exported=0
        try:
            con = message_archive.get_connection()
            with con:
                cur=con.cursor()
                cur.execute("""
                    SELECT message_id,guild_id,channel_id,author_id,message_type,created_at,content,edited_at,attachments,embeds,reactions,reply_to_id
                    FROM message_archive WHERE guild_id=? AND channel_id=? ORDER BY created_at ASC, message_id ASC
                """,(gid,cid))
                rows=cur.fetchmany(1000)
                while rows:
                    for row in rows: w.writerow(row); exported+=1
                    await edit("Dumping rows from DB to CSV…", found=missing, scanned=scanned, exported=exported)
                    rows=cur.fetchmany(1000)
        except Exception:
            log.exception("backread.export.dump_failed", extra={"gid":gid,"cid":cid})
            try: await progress.edit(content=f"**Exporting {label}…**\nDB read failed during export.")
            except Exception: pass
            return

        csv_bytes=sbuf.getvalue().encode("utf-8-sig")
        base=f"{guild.name}-{getattr(target,'name',target.id)}".replace("/","_"); csv_name=f"{base}.csv"
        if not compress and len(csv_bytes)<=int(7.5*1024*1024):
            out = discord.File(io.BytesIO(csv_bytes), filename=csv_name)
        else:
            zbuf=io.BytesIO()
            with zipfile.ZipFile(zbuf, "w", zipfile.ZIP_DEFLATED) as z: z.writestr(csv_name, csv_bytes)
            out = discord.File(io.BytesIO(zbuf.getvalue()), filename=f"{base}.zip")

        text = "\n".join([
            f"**Export for {label}**",
            f"• Rows exported: **{exported:,}**",
            f"• Missing messages filled this run: **{missing:,}**" if fill_missing else "• Missing fill: skipped",
        ])
        try: await interaction.followup.send(text, file=out, ephemeral=True)
        except discord.HTTPException as e:
            if getattr(e,"code",None)==50027 or e.status==401:
                try: await interaction.user.send(text, file=out)
                except Exception: log.exception("backread.export.dm_failed")
                try: await progress.edit(content=text+"\n_(Interaction expired; sent via DM.)_")
                except Exception: pass
            else:
                log.exception("backread.export.final_send_failed")
                try: await progress.edit(content=text+"\n_(Failed to send file.)_")
                except Exception: pass

    # ===== Maint (flattened) =====

    @app_commands.command(name="maint_activity_report", description="Generate archive analytics report.")
    @app_commands.describe(
        timezone="IANA timezone name for heatmaps (default America/Los_Angeles)",
        apply_rpg_stats="If enabled, recompute RPG stats from the generated report.",
    )
    @require_manage_guild()
    async def maint_activity_report(
        self,
        interaction: discord.Interaction,
        timezone: str = activity_report.DEFAULT_TIMEZONE,
        apply_rpg_stats: bool = False,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        gid = interaction.guild_id
        guild = interaction.guild
        if gid is None: return await interaction.followup.send("Guild not resolved.", ephemeral=True)
        bot_ids = {m.id for m in (guild.members if guild else []) if m.bot}
        member_count = getattr(guild,"member_count",None)
        try:
            report = activity_report.generate_activity_report(gid, timezone_name=timezone or activity_report.DEFAULT_TIMEZONE, member_count=member_count, bot_user_ids=bot_ids)
        except Exception:
            log.exception("maint.activity_report.failed", extra={"gid":gid})
            return await interaction.followup.send(S("common.error_generic"), ephemeral=True)
        payload = json.dumps(report.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")
        buf=io.BytesIO(payload); fname=f"activity_report_{gid}.json"
        if apply_rpg_stats:
            try: updated = rpg.apply_stat_snapshot(gid, activity_report.compute_rpg_stats_from_report(report))
            except Exception: log.exception("maint.activity_report.apply_stats_failed", extra={"gid":gid}); updated=0
            buf.seek(0)
            return await interaction.followup.send(f"Generated report and updated stats for **{updated}** member(s).", files=[discord.File(buf, filename=fname)], ephemeral=True)
        buf.seek(0)
        await interaction.followup.send("Generated archive analytics report.", files=[discord.File(buf, filename=fname)], ephemeral=True)

    @app_commands.command(name="maint_import_day_csv", description="Import day-scope CSV and rebuild months.")
    @app_commands.describe(file="CSV exported via /activity export scope=day", month="Optional YYYY-MM filter")
    @require_manage_guild()
    async def maint_import_day_csv(self, interaction: discord.Interaction, file: discord.Attachment, month: Optional[str]=None):
        await interaction.response.defer(ephemeral=True, thinking=True)
        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
        header = next(reader, None) or []
        try:
            idx_g=header.index("guild_id"); idx_d=header.index("day"); idx_u=header.index("user_id"); idx_c=header.index("messages")
        except ValueError:
            return await interaction.followup.send("Bad CSV header. Expected: guild_id, day, user_id, messages.", ephemeral=True)
        touched:Set[str]=set(); rows_imported=0
        for row in reader:
            try:
                gid=int(row[idx_g]); 
                if gid!=interaction.guild_id: continue
                day=row[idx_d]
                if month and not day.startswith(month): continue
                uid=int(row[idx_u]); cnt=int(row[idx_c])
                if cnt<=0: continue
                activity.upsert_member_messages_day(interaction.guild_id, uid, day, cnt)
                touched.add(month_from_day(day)); rows_imported+=1
            except Exception:
                log.exception("maint.import_day_csv.row_failed", extra={"gid": interaction.guild_id, "row": row})
        rebuilt=0
        for m in sorted(touched):
            try: activity.rebuild_month_from_days(interaction.guild_id, m); rebuilt+=1
            except Exception: log.exception("maint.rebuild_month.failed", extra={"gid": interaction.guild_id, "month": m})
        await interaction.followup.send(f"Imported **{rows_imported}** day rows. Rebuilt **{rebuilt}** month aggregates.", ephemeral=True)

    @app_commands.command(name="maint_import_month_csv", description="Import month-scope CSV (direct month upserts).")
    @app_commands.describe(file="CSV exported via /activity export scope=month", month="Optional YYYY-MM filter")
    @require_manage_guild()
    async def maint_import_month_csv(self, interaction: discord.Interaction, file: discord.Attachment, month: Optional[str]=None):
        await interaction.response.defer(ephemeral=True, thinking=True)
        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
        header = next(reader, None) or []
        try:
            idx_g=header.index("guild_id"); idx_m=header.index("month"); idx_u=header.index("user_id"); idx_c=header.index("messages")
        except ValueError:
            return await interaction.followup.send("Bad CSV header. Expected: guild_id, month, user_id, messages.", ephemeral=True)
        rows_imported=0; months:Set[str]=set()
        for row in reader:
            try:
                gid=int(row[idx_g]); 
                if gid!=interaction.guild_id: continue
                mon=row[idx_m]
                if month and mon!=month: continue
                uid=int(row[idx_u]); cnt=int(row[idx_c])
                if cnt<=0: continue
                activity.upsert_member_messages_month(interaction.guild_id, uid, mon, cnt)
                months.add(mon); rows_imported+=1
            except Exception:
                log.exception("maint.import_month_csv.row_failed", extra={"gid": interaction.guild_id, "row": row})
        await interaction.followup.send(f"Imported **{rows_imported}** month rows into {len(months)} month(s).", ephemeral=True)

    @app_commands.command(name="maint_rebuild_month", description="Rebuild a month aggregate from day table.")
    @app_commands.describe(month="YYYY-MM")
    @require_manage_guild()
    async def maint_rebuild_month(self, interaction: discord.Interaction, month: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            activity.rebuild_month_from_days(interaction.guild_id, month)
            await interaction.followup.send(f"Rebuilt aggregates for **{month}**.", ephemeral=True)
        except Exception:
            log.exception("maint.rebuild_month.failed", extra={"gid": interaction.guild_id, "month": month})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="maint_replay_archive", description="Replay archived messages into metrics and RPG XP.")
    @app_commands.describe(
        reset_metrics="Delete existing message/word/emoji/mention metrics before replaying.",
        reset_xp="Delete RPG progress rows before replaying.",
        respec_stats="Redistribute stat points using the current formula after replay.",
        chunk_size="Messages per progress update (default 1000).",
    )
    @require_manage_guild()
    async def maint_replay_archive(
        self,
        interaction: discord.Interaction,
        reset_metrics: bool = True,
        reset_xp: bool = True,
        respec_stats: bool = True,
        chunk_size: int = 1000,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        gid = interaction.guild_id
        if gid is None: return await interaction.edit_original_response(content="Guild not resolved.")

        activity_cog = self.bot.get_cog("ActivityCog")
        if activity_cog is None or not hasattr(activity_cog, "replay_archived_messages"):
            return await interaction.edit_original_response(content="Activity cog is not loaded; cannot replay archive.")

        if chunk_size <= 0: chunk_size = 1000
        await interaction.edit_original_response(content="Preparing archive replay…")

        if reset_metrics:
            try:
                activity.reset_member_activity(gid,"all"); activity.reset_member_words(gid,"all")
                activity.reset_member_mentions(gid,"all"); activity.reset_member_mentions_sent(gid,"all")
                activity.reset_member_emoji_chat(gid,"all"); activity.reset_member_emoji_only(gid,"all")
                activity.reset_member_emoji_react(gid,"all"); activity.reset_member_reactions_received(gid,"all")
                activity.reset_member_channel_totals(gid)
            except Exception:
                log.exception("maint.replay_archive.reset_metrics_failed", extra={"gid":gid})
                return await interaction.edit_original_response(content="Failed to reset metrics. Check logs.")

        cleared=0
        if reset_xp:
            try: cleared = rpg.reset_progress(gid)
            except Exception:
                log.exception("maint.replay_archive.reset_xp_failed", extra={"gid":gid})
                return await interaction.edit_original_response(content="Failed to reset RPG progress. Check logs.")

        summary = message_archive.stats_summary(gid); total=int(summary.get("messages",0))
        base = f"Replaying **{total:,}** archived messages…" if total else "Replaying archived messages…"
        last=0.0
        async def progress(n:int):
            nonlocal last
            now=time.monotonic()
            if n and now-last<1.5 and n<total: return
            last=now
            await interaction.edit_original_response(content="\n".join([base, f"Processed **{n:,}** message(s)…"]))

        await progress(0)
        processed = await activity_cog.replay_archived_messages(
            message_archive.iter_guild_messages(gid, chunk_size=chunk_size),
            yield_every=chunk_size,
            progress_cb=progress,
        )

        redistributed=0
        if respec_stats:
            try: redistributed = rpg.respec_stats_to_formula(gid)
            except Exception:
                log.exception("maint.replay_archive.respec_failed", extra={"gid":gid})
                return await interaction.edit_original_response(
                    content="Archive replay completed, but redistributing stat points failed. XP/metrics were updated."
                )

        lines=[f"Archive replay complete. Processed **{processed:,}** message(s)."]
        if reset_metrics: lines.append("Activity metrics were reset before replaying.")
        if reset_xp:      lines.append(f"Cleared **{cleared:,}** RPG progress row(s) before replay.")
        if respec_stats:  lines.append(f"Redistributed stat points for **{redistributed:,}** member(s).")
        await interaction.edit_original_response(content="\n".join(lines))

    # ===== Cleanup (flattened) =====

    @app_commands.command(name="cleanup_mupurge", description="Purge messages by a bot from a Forum and its threads.")
    @app_commands.describe(
        forum_id="Forum channel ID (defaults to 1428158868843921429).",
        bot_author_id="Author ID to purge (defaults to 1266545197077102633).",
        include_private_archived="Also scan private archived threads (requires permissions).",
        dry_run="If true, only report what would be deleted.",
    )
    @app_commands.checks.has_permissions(manage_messages=True)
    async def cleanup_mupurge(
        self,
        interaction: discord.Interaction,
        forum_id: Optional[int] = None,
        bot_author_id: Optional[int] = None,
        include_private_archived: bool = True,
        dry_run: bool = False,
    ):
        await interaction.response.defer(ephemeral=True)
        forum_id = forum_id or DEFAULT_FORUM_ID
        bot_author_id = bot_author_id or DEFAULT_BOT_AUTHOR_ID
        forum = await resolve_forum_channel(self.bot, interaction.guild, forum_id)
        if forum is None:
            return await interaction.followup.send(f"Forum channel `{forum_id}` not found or not accessible.", ephemeral=True)
        me = forum.guild.me  # type: ignore[assignment]
        if not isinstance(me, discord.Member) or not has_purge_permissions(me, forum):
            return await interaction.followup.send(
                "I need **View Channel**, **Read Message History**, and **Manage Messages** in that forum.", ephemeral=True
            )
        scanned_threads, scanned_messages, matches, deleted = await purge_messages_from_threads(
            await collect_threads(forum, include_private_archived=include_private_archived),
            author_id=bot_author_id,
            dry_run=dry_run,
        )
        dry = "DRY RUN - " if dry_run else ""
        msg = (
            f"{dry}Scanned **{scanned_threads}** threads and **{scanned_messages}** messages in <#{forum.id}>.\n"
            f"Found **{matches}** messages by `<@{bot_author_id}>`."
            f"{'' if dry_run else f' Deleted **{deleted}**.'}"
        )
        await interaction.followup.send(msg, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
    log.info("Loaded AdminCog (flattened /admin commands)")
