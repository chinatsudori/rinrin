from __future__ import annotations
# /app/yuribot/cogs/admin.py

import asyncio
import logging
import re
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from ..strings import S
from ..ui.admin import build_club_config_embed
from ..ui.movebot import format_move_summary, format_pin_summary
from ..utils.admin import ensure_guild, validate_image_filename
from ..utils.cleanup import (
    DEFAULT_BOT_AUTHOR_ID,
    DEFAULT_FORUM_ID,
    collect_threads,
    has_purge_permissions,
    purge_messages_from_threads,
    resolve_forum_channel,
)
from ..utils.movebot import (
    attach_signature,
    fuzzy_ratio,
    get_or_create_webhook,
    normalize_content,
    parent_for_destination,
    parse_jump_or_id,
    resolve_messageable_from_id,
    send_copy,
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

    move = app_commands.Group(name="move", description="Move/pin utilities")

    async def _collect_messages_between(
        self,
        channel: discord.TextChannel | discord.Thread,
        start_id: int,
        end_id: int,
    ) -> List[discord.Message]:
        messages: List[discord.Message] = []
        async for message in channel.history(
            limit=None, oldest_first=True, after=discord.Object(id=start_id - 1)
        ):
            messages.append(message)
            if message.id == end_id:
                break
        return messages

    @move.command(
        name="any",
        description="Copy messages between channels/threads, optionally deleting originals.",
    )
    @app_commands.describe(
        source_id="ID or jump URL of the source message to start from.",
        destination_id="ID or jump URL of the destination channel/thread.",
        end_id="Optional ID or jump URL of the last message to include.",
        delete_original="Delete original messages after copying.",
        include_header="Include author/timestamp/jump header.",
        use_webhook="Relay via webhook to preserve author appearance.",
        post="Post summary publicly in this channel.",
    )
    async def move_any(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        end_id: Optional[str] = None,
        delete_original: bool = False,
        include_header: bool = True,
        use_webhook: bool = True,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        await interaction.response.defer(ephemeral=not post, thinking=True)

        gid = interaction.guild_id
        src_ident = parse_jump_or_id(source_id)
        dst_ident = parse_jump_or_id(destination_id)
        end_ident = parse_jump_or_id(end_id) if end_id else None
        if not src_ident or not dst_ident:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=not post)

        source = await resolve_messageable_from_id(self.bot, gid, src_ident)
        destination = await resolve_messageable_from_id(self.bot, gid, dst_ident)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=not post)
        if not isinstance(destination, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type_text_or_thread"), ephemeral=not post)

        try:
            start_msg = await source.fetch_message(src_ident)
        except discord.NotFound:
            return await interaction.followup.send(S("move_any.error.start_not_found"), ephemeral=not post)

        end_msg: Optional[discord.Message] = None
        if end_ident:
            try:
                end_msg = await source.fetch_message(end_ident)
            except discord.NotFound:
                return await interaction.followup.send(S("move_any.error.end_not_found"), ephemeral=not post)
            if end_msg.created_at < start_msg.created_at:
                start_msg, end_msg = end_msg, start_msg

        to_copy = await self._collect_messages_between(
            source, start_msg.id, end_msg.id if end_msg else start_msg.id
        )
        if not to_copy:
            return await interaction.followup.send(S("move_any.info.no_messages"), ephemeral=not post)

        me = interaction.guild.me
        if not source.permissions_for(me).read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=not post)
        if not destination.permissions_for(me).send_messages:
            return await interaction.followup.send(S("move_any.error.need_send_dest"), ephemeral=not post)

        webhook = None
        parent = parent_for_destination(destination)
        if use_webhook and isinstance(parent, (discord.TextChannel, discord.ForumChannel)):
            webhook = await get_or_create_webhook(parent, me)
            if webhook is None:
                use_webhook = False

        copied = 0
        failed = 0
        for message in to_copy:
            ok, _ = await send_copy(
                destination,
                message,
                use_webhook=use_webhook,
                webhook=webhook,
                include_header=include_header,
            )
            if ok:
                copied += 1
            else:
                failed += 1
            await asyncio.sleep(0.2)

        deleted = 0
        if delete_original:
            for message in to_copy:
                try:
                    await message.delete()
                    deleted += 1
                except Exception as exc:
                    log.info(
                        "movebot.delete.failed",
                        extra={"message_id": message.id, "error": str(exc)},
                    )
                await asyncio.sleep(0.1)

        summary = format_move_summary(
            copied=copied,
            total=len(to_copy),
            failed=failed,
            deleted=deleted,
            post_publicly=post,
        )
        await interaction.followup.send(summary, ephemeral=not post)

    @move.command(
        name="pinmatch",
        description="Mirror pins from a source channel/thread into a destination by content matching.",
    )
    @app_commands.describe(
        source_id="ID (or jump URL) of the source TextChannel/Thread with pins.",
        destination_id="ID (or jump URL) of the destination TextChannel/Thread to pin in.",
        search_depth="How many destination messages to scan (default 3000).",
        allow_header="Set true if copies may include a header/backlink you want ignored.",
        ignore_case="Case-insensitive content match (default true).",
        collapse_ws="Collapse whitespace for matching (default true).",
        min_fuzzy="Minimum fuzzy ratio (0.0-1.0) if exact match fails (default 0.88).",
        ts_slack_seconds="Timestamp slack for candidate selection (default 240s).",
        post="Post summary publicly in this channel.",
    )
    async def pinmatch(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        search_depth: int = 3000,
        allow_header: bool = True,
        ignore_case: bool = True,
        collapse_ws: bool = True,
        min_fuzzy: float = 0.88,
        ts_slack_seconds: int = 240,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        await interaction.response.defer(ephemeral=not post, thinking=True)

        gid = interaction.guild_id
        src_ident = parse_jump_or_id(source_id)
        dst_ident = parse_jump_or_id(destination_id)
        if not src_ident or not dst_ident:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=not post)

        source = await resolve_messageable_from_id(self.bot, gid, src_ident)
        destination = await resolve_messageable_from_id(self.bot, gid, dst_ident)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=not post)
        if not isinstance(destination, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type_text_or_thread"), ephemeral=not post)

        me = interaction.guild.me
        if not source.permissions_for(me).read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=not post)
        if not destination.permissions_for(me).manage_messages:
            return await interaction.followup.send(S("move_any.error.need_read_and_manage_dest"), ephemeral=not post)

        try:
            src_pins = await source.pins()
        except discord.Forbidden:
            return await interaction.followup.send(S("move_any.error.forbidden_read_pins"), ephemeral=not post)

        if not src_pins:
            return await interaction.followup.send(S("move_any.info.no_pins_source"), ephemeral=not post)

        dest_msgs: List[discord.Message] = [
            message
            async for message in destination.history(limit=search_depth, oldest_first=False)
            if message.type == discord.MessageType.default
        ]

        by_content: Dict[str, List[discord.Message]] = {}
        by_attach: Dict[str, List[discord.Message]] = {}
        for dest_msg in dest_msgs:
            key = normalize_content(
                dest_msg.content or "",
                allow_header=allow_header,
                ignore_case=ignore_case,
                collapse_ws=collapse_ws,
            )
            by_content.setdefault(key, []).append(dest_msg)
            sig = attach_signature(dest_msg)
            if sig:
                by_attach.setdefault(sig, []).append(dest_msg)

        pinned = 0
        misses: List[int] = []
        for src_pin in src_pins:
            if src_pin.type != discord.MessageType.default:
                misses.append(src_pin.id)
                continue

            src_key = normalize_content(
                src_pin.content or "",
                allow_header=False,
                ignore_case=ignore_case,
                collapse_ws=collapse_ws,
            )
            src_sig = attach_signature(src_pin)
            timestamp = int(src_pin.created_at.timestamp())

            candidates: List[discord.Message] = []
            if src_key:
                candidates = list(by_content.get(src_key, []))
            if src_sig:
                with_attach = by_attach.get(src_sig, [])
                if candidates:
                    ids = {msg.id for msg in candidates}
                    both = [msg for msg in with_attach if msg.id in ids]
                    candidates = both or candidates
                else:
                    candidates = list(with_attach)

            if not candidates and src_key:
                best: Optional[Tuple[discord.Message, float]] = None
                for dest_msg in dest_msgs:
                    dkey = normalize_content(
                        dest_msg.content or "",
                        allow_header=allow_header,
                        ignore_case=ignore_case,
                        collapse_ws=collapse_ws,
                    )
                    if not dkey:
                        continue
                    ratio = fuzzy_ratio(src_key, dkey)
                    if ratio >= min_fuzzy and (best is None or ratio > best[1]):
                        best = (dest_msg, ratio)
                if best:
                    candidates = [best[0]]

            if not candidates:
                misses.append(src_pin.id)
                continue

            candidates.sort(key=lambda msg: abs(int(msg.created_at.timestamp()) - timestamp))
            target = candidates[0]
            try:
                await target.pin()
                pinned += 1
                await asyncio.sleep(0.3)
            except Exception:
                misses.append(src_pin.id)

        summary = format_pin_summary(
            pinned=pinned,
            total=len(src_pins),
            destination=destination,
            misses=misses,
        )
        await interaction.followup.send(summary, ephemeral=not post)

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
