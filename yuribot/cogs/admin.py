# /app/yuribot/cogs/admin.py
from __future__ import annotations

import asyncio
import logging
import re
from typing import List, Optional
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from ..models import voice as voice_model
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

log = logging.getLogger(__name__)

# ---------- helpers / checks ----------

_JOIN_TITLES = {"voice join", "voice_join", "join", "voice connected"}
_LEAVE_TITLES = {"voice leave", "voice_leave", "leave", "voice disconnected"}
_ID_IN_PARENS = re.compile(r"\((?:\s*)(\d{15,25})(?:\s*)\)\s*$", re.S | re.M)


# replace the old patterns + _extract_last_id with this

_PARENS_ANY = re.compile(r"\(([^()]*)\)\s*$", re.S | re.M)
_DIGITS_10P = re.compile(r"(\d{10,})")

def _extract_last_id(text: str | None) -> Optional[int]:
    """Get the last snowflake-like number from the LAST (...) group; be tolerant of whitespace/newlines."""
    if not text:
        return None

    # Try the last (...) group first
    m = _PARENS_ANY.search(text)
    if m:
        inside = re.sub(r"\D+", "", m.group(1))  # keep only digits (handles splits / spaces / newlines)
        if len(inside) >= 10:
            try:
                return int(inside)
            except ValueError:
                pass

    # Fallback: last 10+ digit run anywhere in the string
    m2 = None
    for m2 in _DIGITS_10P.finditer(text):
        pass
    if m2:
        try:
            return int(m2.group(1))
        except ValueError:
            return None
    return None



def require_manage_guild() -> app_commands.Check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message("Run this in a server.", ephemeral=True)
            return False
        perms = getattr(interaction.user, "guild_permissions", None)
        if not perms or not perms.manage_guild:
            await interaction.response.send_message("You need **Manage Server** to run this.", ephemeral=True)
            return False
        return True

    return app_commands.check(predicate)


def _parse_voice_embed(msg: discord.Message) -> tuple[Optional[str], Optional[int], Optional[int], Optional[datetime]]:
    if not msg.embeds:
        return (None, None, None, None)

    for emb in msg.embeds:
        title = (emb.title or "").strip().lower()

        kind: Optional[str] = None
        if title in _JOIN_TITLES:
            kind = "join"
        elif title in _LEAVE_TITLES:
            kind = "leave"
        else:
            continue

        ts = emb.timestamp or msg.created_at  # both are aware UTC

        user_id: Optional[int] = None
        channel_id: Optional[int] = None
        for f in emb.fields or []:
            name = (f.name or "").strip().lower()
            val = f.value or ""
            if name == "user":
                user_id = _extract_last_id(val)
            elif name == "channel":
                channel_id = _extract_last_id(val)

        if kind and user_id and channel_id and ts:
            return (kind, user_id, channel_id, ts)

    return (None, None, None, None)
# ---------- unified /admin GroupCog ----------

class AdminCog(commands.GroupCog, name="admin", description="Admin tools"):
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

    # ===== Voice log import / stats =====

    @app_commands.command(
        name="voice_import_log",
        description="Parse Voice Join/Leave embeds in a log channel and store historical minutes per user.",
    )
    @app_commands.describe(
        log_channel="The bot-log channel that contains the voice join/leave embeds.",
        since_message_id="Optional: start AFTER this message id.",
        dry_run="If true, only report what would be inserted.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def voice_import_log(
        self,
        interaction: discord.Interaction,
        log_channel: discord.TextChannel,
        since_message_id: Optional[int] = None,
        dry_run: bool = False,
    ):
        if not await ensure_guild(interaction):
            return

        await interaction.response.defer(ephemeral=True, thinking=True)
        voice_model.ensure_schema()

        guild = interaction.guild
        gid = guild.id  # type: ignore[assignment]
        after_obj = discord.Object(id=since_message_id) if since_message_id else None

        # Progress message (we edit it as we go, but rate-limit edits)
        progress = await interaction.edit_original_response(content="Scanning log channel history…")
        last_edit = 0.0

        def _maybe_edit(stage: str, scanned: int, matched: int, sessions: int) -> None:
            nonlocal last_edit
            now = discord.utils.utcnow().timestamp()
            if now - last_edit < 1.0:
                return
            last_edit = now
            content = (
                f"Scanning **#{log_channel.name}**…\n"
                f"• {stage}\n"
                f"• Messages scanned: **{scanned:,}**\n"
                f"• Voice embeds matched: **{matched:,}**\n"
                f"• Sessions built: **{sessions:,}**"
            )
            try:
                asyncio.create_task(progress.edit(content=content))
            except Exception:
                pass

        # Active sessions per user_id -> (channel_id, joined_at)
        active: dict[int, tuple[int, datetime]] = {}
        built: list[voice_model.Session] = []
        scanned = 0
        matched = 0

        try:
            async for m in log_channel.history(limit=None, oldest_first=True, after=after_obj):
                scanned += 1
                kind, user_id, channel_id, ts = _parse_voice_embed(m)
                if kind is None or user_id is None or channel_id is None or ts is None:
                    continue

                matched += 1

                if kind == "join":
                    # Close an existing session (switch) at this join time.
                    if user_id in active:
                        prev_cid, prev_ts = active.pop(user_id)
                        if ts > prev_ts:
                            built.append(voice_model.Session(user_id, prev_cid, prev_ts, ts))
                    active[user_id] = (channel_id, ts)
                else:  # leave
                    if user_id in active:
                        prev_cid, prev_ts = active.pop(user_id)
                        if ts > prev_ts:
                            built.append(voice_model.Session(user_id, prev_cid, prev_ts, ts))
                    # else: stray leave → ignore

                if scanned % 500 == 0:
                    _maybe_edit("Parsing…", scanned, matched, len(built))

        except discord.Forbidden:
            return await interaction.edit_original_response(
                content="Forbidden: I need **Read Message History** in that channel."
            )

        # Historical import: drop dangling sessions
        dangling = len(active)
        active.clear()

        total_sessions = len(built)
        total_minutes = 0
        upsert_rows = 0

        if not dry_run and built:
            rows, minutes_added = voice_model.upsert_sessions_minutes(gid, built)
            upsert_rows += rows
            total_minutes += minutes_added

        lines = [
            f"Scanned **{scanned:,}** messages.",
            f"Matched **{matched:,}** voice embeds.",
            f"Built **{total_sessions:,}** session(s).",
            f"Dangling sessions ignored: **{dangling}**.",
        ]
        if dry_run:
            lines.append("DRY RUN — nothing written.")
        else:
            lines.append(f"Upserted **{upsert_rows:,}** day-row(s); ~**{total_minutes:,}** minutes total.")

        await interaction.edit_original_response(content="\n".join(lines))

    @app_commands.command(
        name="voice_stats",
        description="Show a user's total imported voice minutes (UTC days).",
    )
    @app_commands.describe(user="Target user (default: you).", limit="How many days to preview (default 10).")
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
            return await interaction.followup.send(
                f"Forum channel `{forum_id}` not found or not accessible.", ephemeral=True
            )
        me = forum.guild.me  # type: ignore[assignment]
        if not isinstance(me, discord.Member) or not has_purge_permissions(me, forum):
            return await interaction.followup.send(
                "I need **View Channel**, **Read Message History**, and **Manage Messages** in that forum.",
                ephemeral=True,
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
