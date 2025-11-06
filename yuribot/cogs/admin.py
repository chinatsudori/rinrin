# /app/yuribot/cogs/admin.py
from __future__ import annotations

import asyncio
import csv
import io
import json
import logging
import time
import zipfile
from dataclasses import dataclass, field
from typing import Iterable, List, Optional, Sequence, Set

import discord
from discord import app_commands
from discord.app_commands import errors as app_command_errors
from discord.ext import commands

from ..models import activity, activity_report, message_archive, rpg
from ..models.message_archive import ArchivedMessage  # type: ignore
from ..strings import S
from ..ui.admin import build_club_config_embed
from ..utils.admin import ensure_guild, validate_image_filename
from ..utils.maintact import month_from_day
from ..utils.cleanup import (
    DEFAULT_BOT_AUTHOR_ID,
    DEFAULT_FORUM_ID,
    collect_threads,
    has_purge_permissions,
    purge_messages_from_threads,
    resolve_forum_channel,
)

log = logging.getLogger(__name__)

# ----------------------------- helpers / checks -----------------------------


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


# --------------------------------- Cog --------------------------------------


class AdminCog(commands.GroupCog, name="admin", description="Admin tools"):
    """
    Unified Admin cog with nested sub-groups:

      /admin
      /admin backread  (archive, stats, audit, export)
      /admin maint     (activity import/report/replay)
      /admin cleanup   (thread/bot purge)

    This implementation keeps nesting to one level to satisfy discord.py's
    "groups can only be nested at most one level" rule.
    """

    # parent group (required on your discord.py build)
    group = app_commands.Group(name="admin", description="Admin tools")

    # child groups (declared on the class so they can be used as decorators)
    backread = app_commands.Group(name="backread", description="Archive server message history")
    maint = app_commands.Group(name="maint", description="Admin: activity maintenance")
    cleanup = app_commands.Group(name="cleanup", description="Mod cleanup utilities")

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    async def cog_load(self) -> None:
        """Attach child groups under /admin (idempotent)."""
        parent = self.group
        for g in (self.backread, self.maint, self.cleanup):
            try:
                parent.add_command(g)
            except app_commands.CommandAlreadyRegistered:
                try:
                    parent.remove_command(g.name)
                except (KeyError, AttributeError):
                    pass
                parent.add_command(g)
        log.info("Loaded AdminCog with nested groups: /admin backread|maint|cleanup")

    # ------------------------------- /admin ----------------------------------

    @app_commands.command(name="club_config", description="Show configured club IDs and assets.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def club_config(self, interaction: discord.Interaction, post: bool = False):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        try:
            cfg = guilds := __import__("..models.guilds", fromlist=["guilds"]).guilds  # late import safety
            data = cfg.get_club_map(interaction.guild_id)
        except Exception:
            log.exception("admin.club_config.lookup_failed", extra={"guild_id": interaction.guild_id})
            return await interaction.followup.send(S("admin.club_config.error"), ephemeral=not post)

        pairs = [(club, str(info.get("club_id", "-"))) for club, info in data.items()]
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
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        name = filename or image.filename
        valid_name = validate_image_filename(name)
        if not valid_name:
            return await interaction.followup.send(S("admin.set_image.invalid_name"), ephemeral=not post)

        try:
            data = await image.read()
            __import__("..models.guilds", fromlist=["guilds"]).guilds.store_club_image(
                interaction.guild_id, club_slug, valid_name, data
            )
        except Exception:
            log.exception(
                "admin.set_image.store_failed",
                extra={"guild_id": interaction.guild_id, "club": club_slug},
            )
            return await interaction.followup.send(S("admin.set_image.error"), ephemeral=not post)

        await interaction.followup.send(S("admin.set_image.ok"), ephemeral=not post)

    @app_commands.command(name="set_link", description="Set an external link for a club.")
    @app_commands.describe(
        club_slug="Club slug (e.g. movie)",
        url="URL to store",
        post="If true, post publicly in this channel",
    )
    async def set_link(
        self,
        interaction: discord.Interaction,
        club_slug: str,
        url: str,
        post: bool = False,
    ):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        try:
            __import__("..models.guilds", fromlist=["guilds"]).guilds.store_club_link(
                interaction.guild_id, club_slug, url
            )
        except Exception:
            log.exception(
                "admin.set_link.store_failed",
                extra={"guild_id": interaction.guild_id, "club": club_slug},
            )
            return await interaction.followup.send(S("admin.set_link.error"), ephemeral=not post)

        await interaction.followup.send(S("admin.set_link.ok"), ephemeral=not post)

    # --- sync helpers ---

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
            log.exception("admin.sync_guild.failed", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send("Guild sync failed. Check logs.", ephemeral=True)

    @app_commands.command(
        name="sync_global",
        description="Force-sync slash commands globally (propagation may take up to ~1 hour).",
    )
    async def sync_global(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            cmds = await self.bot.tree.sync()
            await interaction.followup.send(
                f"Synced **{len(cmds)}** global command(s). Allow time for Discord to propagate.",
                ephemeral=True,
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

    # ---------------------------- backread helpers ---------------------------

    @staticmethod
    def _br_label(ch: discord.abc.GuildChannel) -> str:
        name = getattr(ch, "name", str(ch.id))
        if isinstance(ch, discord.Thread) and ch.parent:
            return f"#{ch.parent.name} › #{name}"
        return f"#{name}"

    @staticmethod
    def _br_can_backread(channel: discord.abc.GuildChannel, me: discord.Member) -> tuple[bool, str]:
        try:
            perms = channel.permissions_for(me)
        except Exception:
            return False, "unable to resolve permissions"
        if not perms.view_channel:
            return False, "missing View Channel"
        if not perms.read_message_history:
            return False, "missing Read Message History"
        return True, ""

    async def _br_gather_threads(
        self,
        channel: discord.abc.GuildChannel,
        include_private: bool,
        me: discord.Member,
    ) -> List[discord.Thread]:
        threads: List[discord.Thread] = [t for t in getattr(channel, "threads", []) if isinstance(t, discord.Thread)]
        seen = {t.id for t in threads}

        archived_iter = getattr(channel, "archived_threads", None)
        if callable(archived_iter):
            try:
                async for t in archived_iter(limit=None):
                    if isinstance(t, discord.Thread) and t.id not in seen:
                        threads.append(t)
                        seen.add(t.id)
            except discord.Forbidden:
                log.info(
                    "backread.threads.forbidden",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id, "archived": "public"},
                )
            except discord.HTTPException:
                log.exception(
                    "backread.threads.error",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id, "archived": "public"},
                )

        if include_private and isinstance(channel, discord.TextChannel) and callable(archived_iter):
            perms = channel.permissions_for(me)
            if not perms.manage_threads:
                log.warning(
                    "backread.threads.private_missing_perm",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
                return threads
            try:
                async for t in archived_iter(limit=None, private=True):  # type: ignore[arg-type]
                    if isinstance(t, discord.Thread) and t.id not in seen:
                        threads.append(t)
                        seen.add(t.id)
            except discord.Forbidden:
                log.info(
                    "backread.threads.private_forbidden",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
            except TypeError:
                log.info("backread.threads.private_unsupported", extra={"guild_id": channel.guild.id})
            except discord.HTTPException:
                log.exception("backread.threads.private_error", extra={"guild_id": channel.guild.id})

        return threads

    async def _br_archive_history(
        self,
        channel: discord.abc.GuildChannel,
        stats: BackreadStats,
        *,
        is_thread: bool,
    ) -> None:
        label = self._br_label(channel)
        guild_id = channel.guild.id  # type: ignore[assignment]
        last_id = message_archive.max_message_id(guild_id, channel.id)  # type: ignore[arg-type]

        history_kwargs = {"limit": None, "oldest_first": True}
        if last_id:
            history_kwargs["after"] = discord.Object(id=last_id)

        batch: List[ArchivedMessage] = []
        stored = 0
        latest_seen_id = last_id
        seen_ids: set[int] = set()
        try:
            async for msg in channel.history(**history_kwargs):  # type: ignore[attr-defined]
                if latest_seen_id is not None and msg.id <= latest_seen_id:
                    continue
                try:
                    row = message_archive.from_discord_message(msg)
                except Exception as exc:
                    stats.errors.append(f"{label}: {exc}")
                    continue

                if row.message_id in seen_ids:
                    continue
                seen_ids.add(row.message_id)

                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    stored_now = message_archive.upsert_many(batch)
                    stored += stored_now
                    latest_seen_id = max(latest_seen_id or 0, batch[-1].message_id)
                    batch.clear()
        except discord.Forbidden:
            stats.skipped.append(f"{label} (forbidden)")
            return
        except discord.HTTPException as exc:
            stats.errors.append(f"{label}: HTTP {getattr(exc, 'status', '?')}")
        except Exception:
            log.exception("backread.history.error", extra={"guild_id": guild_id, "channel_id": channel.id})
            stats.errors.append(f"{label}: error")
        finally:
            if batch:
                stored_now = message_archive.upsert_many(batch)
                stored += stored_now
                latest_seen_id = max(latest_seen_id or 0, batch[-1].message_id)

        stats.messages_archived += stored
        if is_thread:
            stats.threads_scanned += 1
        else:
            stats.channels_scanned += 1

    # ------------------------------- /admin backread -------------------------

    @backread.command(name="start", description="Backread text channels into the message archive.")
    @app_commands.describe(
        channel="Limit to a specific text channel.",
        include_archived_threads="Also scan archived public threads for each text channel.",
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
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        me = guild.me
        if not isinstance(me, discord.Member):
            try:
                me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
            except Exception:
                log.exception("backread.fetch_member_failed", extra={"guild_id": guild.id})
                return await interaction.followup.send(
                    "Unable to resolve my member object for permission checks.", ephemeral=True
                )

        targets: List[discord.abc.GuildChannel] = []
        if channel:
            targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            forum_channels = []
            try:
                forum_channels = list(guild.forum_channels)  # type ignore[attr-defined]
            except AttributeError:
                pass
            if not forum_channels:
                forum_channels = [ch for ch in guild.channels if isinstance(ch, discord.ForumChannel)]
            targets.extend(forum_channels)

        stats = BackreadStats()
        for parent in targets:
            can_read, reason = self._br_can_backread(parent, me)
            label = self._br_label(parent)
            if not can_read:
                stats.skipped.append(f"{label} ({reason})")
                continue

            counted_by_history = False
            if isinstance(parent, discord.TextChannel):
                await self._br_archive_history(parent, stats, is_thread=False)
                counted_by_history = True

            if include_archived_threads or include_private_threads:
                threads = await self._br_gather_threads(parent, include_private_threads, me)
            else:
                threads = [t for t in getattr(parent, "threads", []) if isinstance(t, discord.Thread)]

            for t in threads:
                t_can_read, t_reason = self._br_can_backread(t, me)
                if not t_can_read:
                    stats.skipped.append(f"{self._br_label(t)} ({t_reason})")
                    continue
                await self._br_archive_history(t, stats, is_thread=True)
                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

            if not counted_by_history:
                stats.channels_scanned += 1
            await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

        lines = [
            f"Archived **{stats.messages_archived:,}** messages.",
            f"Scanned **{stats.channels_scanned:,}** channels and **{stats.threads_scanned:,}** threads.",
            "New messages will be archived automatically.",
        ]
        if stats.skipped:
            preview = ", ".join(stats.skipped[:5])
            if len(stats.skipped) > 5:
                preview += ", ..."
            lines.append(f"Skipped (permissions): {preview}")
        if stats.errors:
            preview = "; ".join(stats.errors[:3])
            if len(stats.errors) > 3:
                preview += "; ..."
            lines.append(f"Errors: {preview}")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @backread_start.error
    async def _backread_start_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("You need **Manage Server** to run this.", ephemeral=True)
        else:
            raise error

    @backread.command(name="stats", description="Show archive stats for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def backread_stats(self, interaction: discord.Interaction):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)
        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        try:
            summary = message_archive.stats_summary(guild.id)
        except Exception:
            log.exception("backread.stats.failed", extra={"guild_id": guild.id})
            return await interaction.followup.send("Archive stats unavailable.", ephemeral=True)

        lines = [
            f"Archive stats for **{guild.name}**",
            f"• Messages: **{int(summary.get('messages', 0)):,}**",
            f"• Channels: **{int(summary.get('channels', 0)):,}**",
            f"• Unique users: **{int(summary.get('users', 0)):,}**",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @backread.command(name="audit", description="Audit readable channels/threads and what is skipped.")
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
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        me = guild.me
        if not isinstance(me, discord.Member):
            me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]

        targets: List[discord.abc.GuildChannel] = []
        if channel:
            targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            forum_channels = []
            try:
                forum_channels = list(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                pass
            if not forum_channels:
                forum_channels = [ch for ch in guild.channels if isinstance(ch, discord.ForumChannel)]
            targets.extend(forum_channels)

        readable_channels = 0
        skipped_channels: list[str] = []
        total_threads = 0
        readable_threads = 0
        public_threads = 0
        private_threads = 0
        skipped_threads: list[str] = []

        for parent in targets:
            can_read, reason = self._br_can_backread(parent, me)
            label = self._br_label(parent)
            if not can_read:
                skipped_channels.append(f"{label} ({reason})")
                continue

            readable_channels += 1

            if include_archived_threads or include_private_threads:
                threads = await self._br_gather_threads(parent, include_private_threads, me)
            else:
                threads = [t for t in getattr(parent, "threads", []) if isinstance(t, discord.Thread)]

            for t in threads:
                total_threads += 1
                ttype = getattr(t, "type", None)
                ttype_name = str(getattr(ttype, "name", ttype)).lower() if ttype is not None else ""
                is_private = "private" in ttype_name
                if is_private:
                    private_threads += 1
                else:
                    public_threads += 1

                t_can_read, t_reason = self._br_can_backread(t, me)
                if t_can_read:
                    readable_threads += 1
                else:
                    skipped_threads.append(f"{self._br_label(t)} ({t_reason})")

        lines = [
            f"**Audit for `{guild.name}`**",
            f"• Channels readable: **{readable_channels:,}**",
        ]
        if skipped_channels:
            preview = ", ".join(skipped_channels[:8])
            if len(skipped_channels) > 8:
                preview += ", ..."
            lines.append(f"• Channels skipped (perm): **{len(skipped_channels):,}**")
            lines.append(f"  ↳ {preview}")

        lines.extend(
            [
                f"• Threads discovered: **{total_threads:,}** (public: **{public_threads:,}**, private: **{private_threads:,}**)",
                f"• Threads readable now: **{readable_threads:,}**",
                "• Private threads (TextChannels): require **Manage Threads** to fetch archived ones.",
                "• Private forum threads are not retrievable when archived in this discord.py version.",
            ]
        )

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @backread.command(
        name="export",
        description="Export a channel or thread to CSV. Fills missing messages first (unless disabled).",
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
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        target = channel
        if target is None:
            return await interaction.followup.send("Pick a specific text channel or thread.", ephemeral=True)
        if isinstance(target, discord.ForumChannel):
            return await interaction.followup.send(
                "Export a **specific forum thread**, not the forum parent.", ephemeral=True
            )

        me = guild.me
        if not isinstance(me, discord.Member):
            me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
        can_read, reason = self._br_can_backread(target, me)
        if not can_read:
            return await interaction.followup.send(f"I can’t read {self._br_label(target)}: {reason}.", ephemeral=True)

        label = self._br_label(target)
        guild_id = guild.id
        channel_id = target.id

        progress_msg = await interaction.followup.send(f"**Exporting {label}…**\nPreparing…", ephemeral=True)

        deadline = time.monotonic() + 12 * 60  # stop editing before token expiry
        last_edit = 0.0
        edit_enabled = True

        async def _edit(stage: str, *, found: int = 0, scanned: int = 0, exported: int = 0):
            nonlocal last_edit, edit_enabled
            if not edit_enabled or time.monotonic() > deadline:
                edit_enabled = False
                return
            now = time.monotonic()
            if now - last_edit < 1.0:
                return
            parts = [f"**Exporting {label}…**", f"• {stage}"]
            if fill_missing:
                parts.append(f"• Missing found this run: **{found:,}**")
            if scanned:
                parts.append(f"• Messages scanned: **{scanned:,}**")
            if exported:
                parts.append(f"• Rows written: **{exported:,}**")
            try:
                await progress_msg.edit(content="\n".join(parts))
                last_edit = now
            except discord.HTTPException as e:
                if getattr(e, "code", None) == 50027 or e.status == 401:
                    edit_enabled = False

        # 1) Load existing IDs
        await _edit("Loading existing IDs from DB…")
        existing_ids: set[int] = set()
        try:
            con = message_archive.get_connection()
            with con:
                cur = con.cursor()
                cur.execute(
                    "SELECT message_id FROM message_archive WHERE guild_id=? AND channel_id=?",
                    (guild_id, channel_id),
                )
                for (mid,) in cur.fetchall():
                    existing_ids.add(int(mid))
        except Exception:
            log.exception("backread.export.load_existing_failed", extra={"guild_id": guild_id, "channel_id": channel_id})
            try:
                await progress_msg.edit(content=f"**Exporting {label}…**\nDB read failed while loading existing rows.")
            except Exception:
                pass
            return

        # 2) Fill missing via Discord history
        missing_found = 0
        scanned = 0
        if fill_missing and hasattr(target, "history"):
            await _edit("Scanning Discord history for gaps…", found=missing_found, scanned=scanned)
            batch_rows: list[ArchivedMessage] = []
            try:
                async for msg in target.history(limit=None, oldest_first=True):  # type: ignore[attr-defined]
                    scanned += 1
                    if msg.id in existing_ids:
                        if scanned % 250 == 0:
                            await _edit("Scanning Discord history for gaps…", found=missing_found, scanned=scanned)
                        continue

                    try:
                        row = message_archive.from_discord_message(msg)
                    except Exception as exc:
                        log.debug(
                            "backread.export.from_message_failed",
                            extra={"channel_id": channel_id, "msg_id": msg.id, "err": str(exc)},
                        )
                        if scanned % 250 == 0:
                            await _edit("Scanning Discord history for gaps…", found=missing_found, scanned=scanned)
                        continue

                    batch_rows.append(row)
                    existing_ids.add(msg.id)
                    missing_found += 1

                    if len(batch_rows) >= 200:
                        message_archive.upsert_many(batch_rows)
                        batch_rows.clear()
                        await _edit("Filling gaps (writing batch)…", found=missing_found, scanned=scanned)

                if batch_rows:
                    message_archive.upsert_many(batch_rows)
                    batch_rows.clear()
                    await _edit("Filling gaps (finalize)…", found=missing_found, scanned=scanned)

            except discord.Forbidden:
                try:
                    await progress_msg.edit(content=f"**Exporting {label}…**\nForbidden: need **Read Message History**.")
                except Exception:
                    pass
                return
            except discord.HTTPException as exc:
                log.exception("backread.export.history_http", extra={"status": getattr(exc, 'status', '?')})
                try:
                    await progress_msg.edit(
                        content=f"**Exporting {label}…**\nDiscord API error while reading history (HTTP {getattr(exc, 'status', '?')})."
                    )
                except Exception:
                    pass
                return

        # 3) Dump DB rows → CSV
        await _edit("Dumping rows from DB to CSV…", found=missing_found, scanned=scanned)

        buf = io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow(
            [
                "message_id",
                "guild_id",
                "channel_id",
                "author_id",
                "message_type",
                "created_at",
                "content",
                "edited_at",
                "attachments",
                "embeds",
                "reactions_json",
                "reply_to_id",
            ]
        )

        exported = 0
        try:
            con = message_archive.get_connection()
            with con:
                cur = con.cursor()
                cur.execute(
                    """
                    SELECT message_id, guild_id, channel_id, author_id, message_type,
                           created_at, content, edited_at, attachments, embeds, reactions, reply_to_id
                    FROM message_archive
                    WHERE guild_id=? AND channel_id=?
                    ORDER BY created_at ASC, message_id ASC
                    """,
                    (guild_id, channel_id),
                )
                rows = cur.fetchmany(1000)
                while rows:
                    for row in rows:
                        writer.writerow(row)
                        exported += 1
                    await _edit("Dumping rows from DB to CSV…", found=missing_found, scanned=scanned, exported=exported)
                    rows = cur.fetchmany(1000)
        except Exception:
            log.exception("backread.export.dump_failed", extra={"guild_id": guild_id, "channel_id": channel_id})
            try:
                await progress_msg.edit(content=f"**Exporting {label}…**\nDB read failed during export.")
            except Exception:
                pass
            return

        csv_bytes = buf.getvalue().encode("utf-8-sig")
        filename_base = f"{guild.name}-{getattr(target, 'name', target.id)}".replace("/", "_")
        csv_name = f"{filename_base}.csv"

        if compress or len(csv_bytes) > int(7.5 * 1024 * 1024):
            zbuf = io.BytesIO()
            with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                zf.writestr(csv_name, csv_bytes)
            outgoing = discord.File(io.BytesIO(zbuf.getvalue()), filename=f"{filename_base}.zip")
        else:
            outgoing = discord.File(io.BytesIO(csv_bytes), filename=csv_name)

        summary = [
            f"**Export for {label}**",
            f"• Rows exported: **{exported:,}**",
            f"• Missing messages filled this run: **{missing_found:,}**" if fill_missing else "• Missing fill: skipped",
        ]
        text = "\n".join(summary)

        try:
            await interaction.followup.send(text, file=outgoing, ephemeral=True)
        except discord.HTTPException as e:
            # interaction token probably expired → DM fallback
            if getattr(e, "code", None) == 50027 or e.status == 401:
                try:
                    await interaction.user.send(text, file=outgoing)
                except Exception:
                    log.exception("backread.export.dm_failed")
                try:
                    await progress_msg.edit(content=text + "\n_(Interaction expired; sent via DM.)_")
                except Exception:
                    pass
            else:
                log.exception("backread.export.final_send_failed")
                try:
                    await progress_msg.edit(content=text + "\n_(Failed to send file.)_")
                except Exception:
                    pass

    # ------------------------------- /admin maint ----------------------------

    @maint.command(name="activity_report", description="Generate a full activity analytics report from the archive.")
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
        guild = interaction.guild
        gid = interaction.guild_id
        if gid is None:
            await interaction.followup.send("Guild not resolved.", ephemeral=True)
            return

        bot_ids = {m.id for m in (guild.members if guild else []) if m.bot}
        member_count = getattr(guild, "member_count", None)

        try:
            report = activity_report.generate_activity_report(
                gid, timezone_name=timezone or activity_report.DEFAULT_TIMEZONE, member_count=member_count, bot_user_ids=bot_ids
            )
        except Exception:
            log.exception("maint.activity_report.failed", extra={"guild_id": gid})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)
            return

        payload = json.dumps(report.to_dict(), indent=2, ensure_ascii=False).encode("utf-8")
        buffer = io.BytesIO(payload)
        filename = f"activity_report_{gid}.json"

        if apply_rpg_stats:
            try:
                stat_map = activity_report.compute_rpg_stats_from_report(report)
                updated = rpg.apply_stat_snapshot(gid, stat_map)
            except Exception:
                log.exception("maint.activity_report.apply_stats_failed", extra={"guild_id": gid})
                updated = 0
            buffer.seek(0)
            await interaction.followup.send(
                content=f"Generated report and updated stats for **{updated}** member(s).",
                files=[discord.File(buffer, filename=filename)],
                ephemeral=True,
            )
        else:
            buffer.seek(0)
            await interaction.followup.send(
                content="Generated archive analytics report.",
                files=[discord.File(buffer, filename=filename)],
                ephemeral=True,
            )

    @maint.command(name="import_day_csv", description="Import day-scope CSV and rebuild months.")
    @app_commands.describe(
        file="CSV exported via /activity export scope=day",
        month="Optional YYYY-MM filter; if set, only rows for this month are imported",
    )
    @require_manage_guild()
    async def maint_import_day_csv(
        self, interaction: discord.Interaction, file: discord.Attachment, month: Optional[str] = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
        header = next(reader, None) or []
        try:
            idx_g = header.index("guild_id")
            idx_d = header.index("day")
            idx_u = header.index("user_id")
            idx_c = header.index("messages")
        except ValueError:
            return await interaction.followup.send(
                "Bad CSV header. Expected columns: guild_id, day, user_id, messages.", ephemeral=True
            )

        touched: Set[str] = set()
        rows_imported = 0
        for row in reader:
            try:
                gid = int(row[idx_g])
                if gid != interaction.guild_id:
                    continue
                day = row[idx_d]
                if month and not day.startswith(month):
                    continue
                uid = int(row[idx_u])
                cnt = int(row[idx_c])
                if cnt <= 0:
                    continue
                activity.upsert_member_messages_day(interaction.guild_id, uid, day, cnt)
                touched.add(month_from_day(day))
                rows_imported += 1
            except Exception:
                log.exception("maint.import_day_csv.row_failed", extra={"guild_id": interaction.guild_id, "row": row})

        rebuilt = 0
        for m in sorted(touched):
            try:
                activity.rebuild_month_from_days(interaction.guild_id, m)
                rebuilt += 1
            except Exception:
                log.exception("maint.rebuild_month.failed", extra={"guild_id": interaction.guild_id, "month": m})

        await interaction.followup.send(
            f"Imported **{rows_imported}** day rows. Rebuilt **{rebuilt}** month aggregates.", ephemeral=True
        )

    @maint.command(name="import_month_csv", description="Import month-scope CSV (direct month upserts).")
    @app_commands.describe(
        file="CSV exported via /activity export scope=month",
        month="Optional YYYY-MM filter; if set, only rows for this month are imported",
    )
    @require_manage_guild()
    async def maint_import_month_csv(
        self, interaction: discord.Interaction, file: discord.Attachment, month: Optional[str] = None
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
        header = next(reader, None) or []
        try:
            idx_g = header.index("guild_id")
            idx_m = header.index("month")
            idx_u = header.index("user_id")
            idx_c = header.index("messages")
        except ValueError:
            return await interaction.followup.send(
                "Bad CSV header. Expected columns: guild_id, month, user_id, messages.", ephemeral=True
            )

        rows_imported = 0
        months_touched: Set[str] = set()
        for row in reader:
            try:
                gid = int(row[idx_g])
                if gid != interaction.guild_id:
                    continue
                mon = row[idx_m]
                if month and mon != month:
                    continue
                uid = int(row[idx_u])
                cnt = int(row[idx_c])
                if cnt <= 0:
                    continue
                activity.upsert_member_messages_month(interaction.guild_id, uid, mon, cnt)
                months_touched.add(mon)
                rows_imported += 1
            except Exception:
                log.exception("maint.import_month_csv.row_failed", extra={"guild_id": interaction.guild_id, "row": row})

        await interaction.followup.send(
            f"Imported **{rows_imported}** month rows into {len(months_touched)} month(s).", ephemeral=True
        )

    @maint.command(name="rebuild_month", description="Rebuild a month aggregate from day table.")
    @app_commands.describe(month="YYYY-MM")
    @require_manage_guild()
    async def maint_rebuild_month(self, interaction: discord.Interaction, month: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            activity.rebuild_month_from_days(interaction.guild_id, month)
            await interaction.followup.send(f"Rebuilt aggregates for **{month}**.", ephemeral=True)
        except Exception:
            log.exception("maint.rebuild_month.failed", extra={"guild_id": interaction.guild_id, "month": month})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @maint.command(
        name="replay_archive",
        description="Replay archived messages into activity metrics and RPG XP.",
    )
    @app_commands.describe(
        reset_metrics="Delete existing message/word/emoji/mention metrics before replaying.",
        reset_xp="Delete RPG progress rows before replaying.",
        respec_stats="Redistribute stat points using the current formula after replay.",
        chunk_size="Messages to process between progress updates (default 1000).",
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
        if gid is None:
            await interaction.edit_original_response(content="Guild not resolved.")
            return

        activity_cog = self.bot.get_cog("ActivityCog")
        if activity_cog is None or not hasattr(activity_cog, "replay_archived_messages"):
            await interaction.edit_original_response(content="Activity cog is not loaded; cannot replay archive.")
            return

        if chunk_size <= 0:
            chunk_size = 1000

        await interaction.edit_original_response(content="Preparing archive replay…")

        if reset_metrics:
            try:
                activity.reset_member_activity(gid, "all")
                activity.reset_member_words(gid, "all")
                activity.reset_member_mentions(gid, "all")
                activity.reset_member_mentions_sent(gid, "all")
                activity.reset_member_emoji_chat(gid, "all")
                activity.reset_member_emoji_only(gid, "all")
                activity.reset_member_emoji_react(gid, "all")
                activity.reset_member_reactions_received(gid, "all")
                activity.reset_member_channel_totals(gid)
            except Exception:
                log.exception("maint.replay_archive.reset_metrics_failed", extra={"guild_id": gid})
                await interaction.edit_original_response(content="Failed to reset metrics. Check logs for details.")
                return

        cleared_rows = 0
        if reset_xp:
            try:
                cleared_rows = rpg.reset_progress(gid)
            except Exception:
                log.exception("maint.replay_archive.reset_xp_failed", extra={"guild_id": gid})
                await interaction.edit_original_response(content="Failed to reset RPG progress. Check logs.")
                return

        summary = message_archive.stats_summary(gid)
        total_messages = int(summary.get("messages", 0))
        base_line = (
            f"Replaying **{total_messages:,}** archived messages…" if total_messages else "Replaying archived messages…"
        )

        last_update = 0.0

        async def progress_cb(processed: int) -> None:
            nonlocal last_update
            now = time.monotonic()
            if processed and now - last_update < 1.5 and processed < total_messages:
                return
            last_update = now
            lines = [base_line, f"Processed **{processed:,}** message(s)…"]
            await interaction.edit_original_response(content="\n".join(lines))

        await progress_cb(0)

        processed = await activity_cog.replay_archived_messages(
            message_archive.iter_guild_messages(gid, chunk_size=chunk_size), yield_every=chunk_size, progress_cb=progress_cb
        )

        redistributed = 0
        if respec_stats:
            try:
                redistributed = rpg.respec_stats_to_formula(gid)
            except Exception:
                log.exception("maint.replay_archive.respec_failed", extra={"guild_id": gid})
                await interaction.edit_original_response(
                    content="Archive replay completed, but redistributing stat points failed. XP/metrics were updated."
                )
                return

        lines = [f"Archive replay complete. Processed **{processed:,}** message(s)."]
        if reset_metrics:
            lines.append("Activity metrics were reset before replaying.")
        if reset_xp:
            lines.append(f"Cleared **{cleared_rows:,}** RPG progress row(s) before replay.")
        if respec_stats:
            lines.append(f"Redistributed stat points for **{redistributed:,}** member(s).")

        await interaction.edit_original_response(content="\n".join(lines))

    # ------------------------------- /admin cleanup --------------------------

    @cleanup.command(
        name="mupurge",
        description="Purge messages posted by a bot from a Forum and its threads.",
    )
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

        threads = await collect_threads(forum, include_private_archived=include_private_archived)
        scanned_threads, scanned_messages, matches, deleted = await purge_messages_from_threads(
            threads, author_id=bot_author_id, dry_run=dry_run
        )

        dry_prefix = "DRY RUN - " if dry_run else ""
        summary = (
            f"{dry_prefix}Scanned **{scanned_threads}** threads and **{scanned_messages}** messages in <#{forum.id}>.\n"
            f"Found **{matches}** messages authored by `<@{bot_author_id}>`."
            f"{'' if dry_run else f' Deleted **{deleted}**.'}"
        )
        await interaction.followup.send(summary, ephemeral=True)

    @cleanup_mupurge.error
    async def _cleanup_mupurge_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message("You need **Manage Messages** to run this.", ephemeral=True)
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
    log.info("Loaded AdminCog")
