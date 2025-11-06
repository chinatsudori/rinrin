from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass, field
from typing import List, Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import message_archive
from ..utils.admin import ensure_guild
from .admin import AdminCog

log = logging.getLogger(__name__)

BATCH_SIZE = 100
DELAY_BETWEEN_CHANNELS = 0.5

# -------------------------
# Placeholder group for decorators (NOT auto-registered)
# -------------------------
_BACKREAD_GROUP = app_commands.Group(
    name="backread",
    description="Archive server message history",
)


@dataclass
class BackreadStats:
    channels_scanned: int = 0
    threads_scanned: int = 0
    messages_archived: int = 0
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class BackreadCog(commands.Cog):
    """Archive historic guild messages into SQLite for analytics."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._parent_group: app_commands.Group | None = None

        # Real runtime group (we control registration manually)
        self.group = app_commands.Group(
            name="backread",
            description="Archive server message history",
        )
        for cmd in list(_BACKREAD_GROUP.commands):
            self.group.add_command(cmd)

    # ----------- registration / nesting under /admin -----------
    async def cog_load(self) -> None:
        admin_cog = self.bot.get_cog("AdminCog")
        if isinstance(admin_cog, AdminCog):
            try:
                admin_cog.group.remove_command(self.group.name)
            except (KeyError, AttributeError):
                pass
            admin_cog.group.add_command(self.group)
            self._parent_group = admin_cog.group
        else:
            try:
                self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError):
                pass
            self.bot.tree.add_command(self.group)
            self._parent_group = None

    async def cog_unload(self) -> None:
        if self._parent_group is not None:
            try:
                self._parent_group.remove_command(self.group.name)
            except (KeyError, AttributeError):
                pass
        else:
            try:
                self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError):
                pass

    # ------------------------ helpers ------------------------

    async def _gather_threads(
        self,
        channel: discord.abc.GuildChannel,
        include_private: bool,
        me: discord.Member,
    ) -> List[discord.Thread]:
        """Collect active + archived threads, with safe handling for forums."""
        threads: List[discord.Thread] = [
            t for t in getattr(channel, "threads", []) if isinstance(t, discord.Thread)
        ]
        seen = {t.id for t in threads}

        archived_iter = getattr(channel, "archived_threads", None)
        if callable(archived_iter):
            # Public archived (works for TextChannel & ForumChannel)
            try:
                async for thread in archived_iter(limit=None):
                    if isinstance(thread, discord.Thread) and thread.id not in seen:
                        threads.append(thread)
                        seen.add(thread.id)
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

        # Private archived only supported for TextChannel on many lib versions.
        if include_private and isinstance(channel, discord.TextChannel) and callable(archived_iter):
            perms = channel.permissions_for(me)
            if not perms.manage_threads:
                log.warning(
                    "backread.threads.private_missing_perm",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
                return threads
            try:
                async for thread in archived_iter(limit=None, private=True):  # type: ignore[arg-type]
                    if isinstance(thread, discord.Thread) and thread.id not in seen:
                        threads.append(thread)
                        seen.add(thread.id)
            except discord.Forbidden:
                log.info(
                    "backread.threads.private_forbidden",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
            except TypeError:
                # ForumChannel/private not supported (or signature doesn't accept private=)
                log.info(
                    "backread.threads.private_unsupported",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
            except discord.HTTPException:
                log.exception(
                    "backread.threads.private_error",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )

        return threads

    def _can_backread(
        self,
        channel: discord.abc.GuildChannel,
        me: discord.Member,
    ) -> tuple[bool, str]:
        try:
            perms = channel.permissions_for(me)
        except Exception:
            return False, "unable to resolve permissions"

        if not perms.view_channel:
            return False, "missing View Channel"
        if not perms.read_message_history:
            return False, "missing Read Message History"
        return True, ""

    async def _archive_history(
        self,
        channel: discord.abc.GuildChannel,
        stats: BackreadStats,
        *,
        is_thread: bool,
    ) -> None:
        label = self._label(channel)
        guild_id = channel.guild.id  # type: ignore[assignment]
        last_id = message_archive.max_message_id(guild_id, channel.id)  # type: ignore[arg-type]

        log.debug(
            "backread.history.begin",
            extra={
                "guild_id": guild_id,
                "channel_id": channel.id,
                "label": label,
                "last_id": last_id,
                "is_thread": is_thread,
            },
        )

        history_kwargs = {"limit": None, "oldest_first": True}
        if last_id:
            history_kwargs["after"] = discord.Object(id=last_id)

        batch: List[message_archive.ArchivedMessage] = []
        stored = 0
        latest_seen_id = last_id
        seen_ids: set[int] = set()
        try:
            async for message in channel.history(**history_kwargs):  # type: ignore[attr-defined]
                if latest_seen_id is not None and message.id <= latest_seen_id:
                    continue
                try:
                    row = message_archive.from_discord_message(message)
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
            log.exception(
                "backread.history.error",
                extra={"guild_id": guild_id, "channel_id": channel.id, "label": label},
            )
            stats.errors.append(f"{label}: error")
        finally:
            if batch:
                stored_now = message_archive.upsert_many(batch)
                stored += stored_now
                latest_seen_id = max(latest_seen_id or 0, batch[-1].message_id)
                batch.clear()

        stats.messages_archived += stored
        if is_thread:
            stats.threads_scanned += 1
        else:
            stats.channels_scanned += 1

        log.debug(
            "backread.history.complete",
            extra={
                "guild_id": guild_id,
                "channel_id": channel.id,
                "label": label,
                "stored": stored,
                "latest_seen_id": latest_seen_id,
                "is_thread": is_thread,
            },
        )

    @staticmethod
    def _label(channel: discord.abc.GuildChannel) -> str:
        name = getattr(channel, "name", str(channel.id))
        if isinstance(channel, discord.Thread):
            parent = channel.parent
            if parent is not None:
                return f"#{parent.name} › #{name}"
        return f"#{name}"

    # ------------------------ listeners ------------------------

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return
        try:
            row = message_archive.from_discord_message(message)
        except Exception:
            return
        if message_archive.has_message(row.message_id):
            return
        message_archive.upsert_many([row])

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if not after.guild:
            return
        try:
            row = message_archive.from_discord_message(after)
        except Exception:
            return
        message_archive.upsert_many([row])

    @commands.Cog.listener()
    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        if payload.guild_id is None:
            return
        channel = self.bot.get_channel(payload.channel_id)
        if channel is None:
            try:
                channel = await self.bot.fetch_channel(payload.channel_id)
            except Exception:
                return
        if not hasattr(channel, "fetch_message"):
            return
        try:
            message = await channel.fetch_message(payload.message_id)  # type: ignore[attr-defined]
        except Exception:
            return
        try:
            row = message_archive.from_discord_message(message)
        except Exception:
            return
        message_archive.upsert_many([row])

    # ------------------------ commands ------------------------

    @_BACKREAD_GROUP.command(
        name="start",
        description="Backread text channels into the message archive.",
    )
    @app_commands.describe(
        channel="Limit to a specific text channel.",
        include_archived_threads="Also scan archived public threads for each text channel.",
        include_private_threads="Scan private archived threads (TextChannels; requires Manage Threads).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def start(
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
            # add forums as well
            forum_channels = []
            try:
                forum_channels = list(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                pass
            if not forum_channels:
                forum_channels = [ch for ch in guild.channels if isinstance(ch, discord.ForumChannel)]
            targets.extend(forum_channels)

        stats = BackreadStats()
        for text_channel in targets:
            can_read, reason = self._can_backread(text_channel, me)
            label = self._label(text_channel)
            if not can_read:
                stats.skipped.append(f"{label} ({reason})")
                continue

            log.info(
                "backread.channel.start",
                extra={"guild_id": guild.id, "channel_id": text_channel.id, "label": label},
            )

            counted_by_history = False
            if isinstance(text_channel, discord.TextChannel):
                await self._archive_history(text_channel, stats, is_thread=False)
                counted_by_history = True

            if include_archived_threads or include_private_threads:
                threads = await self._gather_threads(text_channel, include_private_threads, me)
            else:
                threads = [t for t in getattr(text_channel, "threads", []) if isinstance(t, discord.Thread)]

            for thread in threads:
                t_can_read, t_reason = self._can_backread(thread, me)
                t_label = self._label(thread)
                if not t_can_read:
                    stats.skipped.append(f"{t_label} ({t_reason})")
                    continue
                await self._archive_history(thread, stats, is_thread=True)
                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

            if not counted_by_history:
                stats.channels_scanned += 1

            await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

        summary_lines = [
            f"Archived **{stats.messages_archived}** messages.",
            f"Scanned **{stats.channels_scanned}** channels and **{stats.threads_scanned}** threads.",
            "New messages will be archived automatically.",
        ]
        if stats.skipped:
            skipped_preview = ", ".join(stats.skipped[:5])
            if len(stats.skipped) > 5:
                skipped_preview += ", ..."
            summary_lines.append(f"Skipped (permissions): {skipped_preview}")
        if stats.errors:
            error_preview = "; ".join(stats.errors[:3])
            if len(stats.errors) > 3:
                error_preview += "; ..."
            summary_lines.append(f"Errors: {error_preview}")

        await interaction.followup.send("\n".join(summary_lines), ephemeral=True)

    @start.error
    async def _on_start_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message(
                "You need **Manage Server** to run this.", ephemeral=True
            )
        else:
            raise error

    @_BACKREAD_GROUP.command(name="stats", description="Show archive stats for this server.")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stats_cmd(self, interaction: discord.Interaction):
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

        msg_count = int(summary.get("messages", 0))
        ch_count = int(summary.get("channels", 0))
        user_count = int(summary.get("users", 0))

        lines = [
            f"Archive stats for **{guild.name}**",
            f"• Messages: **{msg_count:,}**",
            f"• Channels: **{ch_count:,}**",
            f"• Unique users: **{user_count:,}**",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @_BACKREAD_GROUP.command(
        name="audit",
        description="Audit readable channels/threads and what is skipped.",
    )
    @app_commands.describe(
        channel="Limit to a specific text or forum channel.",
        include_archived_threads="Scan archived threads for each channel (public).",
        include_private_threads="Try private archived threads (TextChannels only; forums unsupported).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def audit_cmd(
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
            can_read, reason = self._can_backread(parent, me)
            label = self._label(parent)
            if not can_read:
                skipped_channels.append(f"{label} ({reason})")
                continue

            readable_channels += 1

            if include_archived_threads or include_private_threads:
                threads = await self._gather_threads(parent, include_private_threads, me)
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

                t_can_read, t_reason = self._can_backread(t, me)
                if t_can_read:
                    readable_threads += 1
                else:
                    skipped_threads.append(f"{self._label(t)} ({t_reason})")

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
                "• Private threads (ForumChannels): **unsupported** to fetch when archived in this lib.",
            ]
        )

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @_BACKREAD_GROUP.command(
        name="export",
        description="Export a channel or thread to CSV. Fills missing messages first (unless disabled).",
    )
    @app_commands.describe(
        channel="A TextChannel or Thread to export.",
        fill_missing="Fetch any messages not in the DB first (default: true).",
        compress="Zip the CSV if large (auto if >7.5MB).",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def export_cmd(
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
        can_read, reason = self._can_backread(target, me)
        if not can_read:
            return await interaction.followup.send(
                f"I can’t read {self._label(target)}: {reason}.", ephemeral=True
            )

        import time, csv, io, zipfile, discord as _dpy

        label = self._label(target)
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
                await progress_msg.edit(content="\n".join(parts))  # never attach files on edit
                last_edit = now
            except _dpy.HTTPException as e:
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
            batch_rows: list[message_archive.ArchivedMessage] = []
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

            except _dpy.Forbidden:
                try:
                    await progress_msg.edit(content=f"**Exporting {label}…**\nForbidden: need **Read Message History**.")
                except Exception:
                    pass
                return
            except _dpy.HTTPException as exc:
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
        import io as _io
        buf = _io.StringIO()
        writer = csv.writer(buf, lineterminator="\n")
        writer.writerow([
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
        ])

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
            with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr(csv_name, csv_bytes)
            outgoing_file = _dpy.File(io.BytesIO(zbuf.getvalue()), filename=f"{filename_base}.zip")
        else:
            outgoing_file = _dpy.File(io.BytesIO(csv_bytes), filename=csv_name)

        summary = [
            f"**Export for {label}**",
            f"• Rows exported: **{exported:,}**",
            f"• Missing messages filled this run: **{missing_found:,}**" if fill_missing else "• Missing fill: skipped",
        ]
        text = "\n".join(summary)

        try:
            await interaction.followup.send(text, file=outgoing_file, ephemeral=True)
        except _dpy.HTTPException as e:
            # token likely expired → DM fallback
            if getattr(e, "code", None) == 50027 or e.status == 401:
                try:
                    await interaction.user.send(text, file=outgoing_file)
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


async def setup(bot: commands.Bot):
    await bot.add_cog(BackreadCog(bot))
