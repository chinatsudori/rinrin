from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass, field
from typing import Awaitable, Callable, List, Optional, Tuple
from .admin import AdminCog
import discord
from discord import app_commands
from datetime import datetime, timezone, time as dtime

from discord.ext import commands, tasks

from ..models import message_archive
from ..utils.admin import ensure_guild

log = logging.getLogger(__name__)

# Tuning
BATCH_SIZE = 100
DELAY_BETWEEN_CHANNELS = 0.5
PROGRESS_LOG_EVERY_SEC = 2.0
PROGRESS_LOG_BATCH = 5000  # also log every ~N messages per channel

# ---- Adjust if your archive module uses different names/schema ----
ARCHIVE_DB_PATH_ATTR = "DB_PATH"            # e.g., message_archive.DB_PATH
ARCHIVE_GET_CONN_ATTR = "get_connection"    # e.g., def get_connection() -> sqlite3.Connection
ARCHIVE_TABLE = "messages"
COL_GUILD_ID = "guild_id"
COL_CHANNEL_ID = "channel_id"
COL_AUTHOR_ID = "author_id"
# -------------------------------------------------------------------


@dataclass
class BackreadStats:
    channels_scanned: int = 0
    threads_scanned: int = 0
    messages_archived: int = 0
    skipped: List[str] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)


class ProgressReporter:
    """Periodic console logs and (optional) ephemeral status edits."""

    def __init__(self, stats: BackreadStats, *, log_every_sec: float = PROGRESS_LOG_EVERY_SEC):
        self.stats = stats
        self.log_every_sec = log_every_sec
        self._task: Optional[asyncio.Task] = None
        self._running = asyncio.Event()
        self._running.set()
        self._edit_cb = None  # async def() -> None

    def set_edit_callback(self, cb):
        self._edit_cb = cb

    async def _run(self):
        while self._running.is_set():
            log.info(
                "backread.progress",
                extra={
                    "channels_scanned": self.stats.channels_scanned,
                    "threads_scanned": self.stats.threads_scanned,
                    "messages_archived": self.stats.messages_archived,
                    "skipped": len(self.stats.skipped),
                    "errors": len(self.stats.errors),
                },
            )
            if self._edit_cb:
                try:
                    await self._edit_cb()
                except Exception:
                    log.debug("backread.progress.ephemeral_edit_failed", exc_info=True)
            await asyncio.sleep(self.log_every_sec)

    async def start(self):
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._running.clear()
        if self._task:
            try:
                await asyncio.wait_for(self._task, timeout=2.0)
            except asyncio.TimeoutError:
                self._task.cancel()


class BackreadCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._parent_group: app_commands.Group | None = None

    # your full group and commands already exist; keep them
    group = app_commands.Group(name="backread", description="Archive server message history")

    async def cog_load(self) -> None:
        """Attach /backread under /admin if AdminCog is present; else add to root."""
        admin_cog = self.bot.get_cog("AdminCog")
        if isinstance(admin_cog, AdminCog):
            # ensure no duplicate
            try: admin_cog.group.remove_command(self.group.name)
            except (KeyError, AttributeError): pass
            admin_cog.group.add_command(self.group)
            self._parent_group = admin_cog.group
        else:
            # root fallback
            try: self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError): pass
            self.bot.tree.add_command(self.group)
            self._parent_group = None

    async def cog_unload(self) -> None:
        if self._parent_group is not None:
            try: self._parent_group.remove_command(self.group.name)
            except (KeyError, AttributeError): pass
        else:
            try: self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError): pass
    # ------------------------
    # /backread start
    # ------------------------
    @group.command(name="start", description="Backread text channels into the message archive.")
    @app_commands.describe(
        channel="Limit to a specific text channel.",
        include_archived_threads="Also scan archived public threads for each text channel.",
        include_private_threads="Scan private archived threads (requires Manage Threads).",
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
        progress_msg = await interaction.followup.send("Starting backread…", ephemeral=True)

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

        # Target discovery: text + forum channels
        targets: List[discord.abc.GuildChannel] = []
        if channel:
            targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            try:
                forum_channels = list(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                forum_channels = []
            if not forum_channels:
                forum_channels = [ch for ch in guild.channels if isinstance(ch, discord.ForumChannel)]
            targets.extend(forum_channels)

        stats = BackreadStats()
        reporter = ProgressReporter(stats, log_every_sec=PROGRESS_LOG_EVERY_SEC)

        async def _edit_progress():
            lines = [
                f"**Backreading…**",
                f"Archived: **{stats.messages_archived}** messages",
                f"Scanned: **{stats.channels_scanned}** channels, **{stats.threads_scanned}** threads",
            ]
            if stats.skipped:
                lines.append(f"Skipped (perm): {len(stats.skipped)}")
            if stats.errors:
                lines.append(f"Errors: {len(stats.errors)}")
            try:
                await progress_msg.edit(content="\n".join(lines))
            except Exception:
                log.debug("backread.progress.ephemeral_edit_failed", exc_info=True)

        reporter.set_edit_callback(_edit_progress)
        await reporter.start()

        try:
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

                # Archive the channel's own history if it supports .history()
                if hasattr(text_channel, "history"):
                    await self._archive_history(text_channel, stats, is_thread=False)

                # Threads
                if include_archived_threads or include_private_threads:
                    threads = await self._gather_threads(text_channel, include_private_threads, me)
                else:
                    threads = [
                        t for t in getattr(text_channel, "threads", []) if isinstance(t, discord.Thread)
                    ]

                for thread in threads:
                    t_can_read, t_reason = self._can_backread(thread, me)
                    t_label = self._label(thread)
                    if not t_can_read:
                        stats.skipped.append(f"{t_label} ({t_reason})")
                        continue
                    await self._archive_history(thread, stats, is_thread=True)
                    await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

                await asyncio.sleep(DELAY_BETWEEN_CHANNELS)

        finally:
            await reporter.stop()

        # Final summary
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

        try:
            await progress_msg.edit(content="\n".join(summary_lines))
        except Exception:
            await interaction.followup.send("\n".join(summary_lines), ephemeral=True)
        # ------------------------
    # /backread export
    # ------------------------
    @group.command(
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

        label = self._label(target)
        guild_id = guild.id
        channel_id = target.id

        # progress message + throttled editor
        progress_msg = await interaction.followup.send(f"**Exporting {label}…**\nPreparing…", ephemeral=True)
        import time
        last_edit = 0.0

        async def _edit(stage: str, *, found: int = 0, scanned: int = 0, exported: int = 0):
            nonlocal last_edit
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
            except Exception:
                pass

        # 1) Load existing IDs
        await _edit("Loading existing IDs from DB…")
        try:
            existing_ids = self._load_existing_ids(guild_id, channel_id)
        except Exception:
            log.exception(
                "backread.export.load_existing_failed",
                extra={"guild_id": guild_id, "channel_id": channel_id},
            )
            return await progress_msg.edit(
                content=f"**Exporting {label}…**\nDB read failed while loading existing rows."
            )

        # 2) Fill missing (optional)
        missing_found = 0
        scanned = 0
        if fill_missing and hasattr(target, "history"):
            try:
                missing_found, scanned = await self._fill_missing_history(
                    target,
                    existing_ids=existing_ids,
                    progress_cb=lambda stage, found, scan: _edit(
                        stage, found=found, scanned=scan
                    ),
                )
            except discord.Forbidden:
                return await progress_msg.edit(
                    content=f"**Exporting {label}…**\nForbidden: need **Read Message History**."
                )
            except discord.HTTPException as exc:
                log.exception(
                    "backread.export.history_http",
                    extra={"status": getattr(exc, "status", "?")},
                )
                return await progress_msg.edit(
                    content=(
                        f"**Exporting {label}…**\nDiscord API error while reading history (HTTP {getattr(exc, 'status', '?')} )."
                    )
                )

        # 3) Dump to CSV from DB (authoritative)
        await _edit("Dumping rows from DB to CSV…", found=missing_found, scanned=scanned)
        import csv, io, zipfile

        buf = io.StringIO()
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
                fetch_sz = 1000
                rows = cur.fetchmany(fetch_sz)
                while rows:
                    for row in rows:
                        writer.writerow(row)
                        exported += 1
                    await _edit("Dumping rows from DB to CSV…", found=missing_found, scanned=scanned, exported=exported)
                    rows = cur.fetchmany(fetch_sz)
        except Exception:
            log.exception("backread.export.dump_failed", extra={"guild_id": guild_id, "channel_id": channel_id})
            return await progress_msg.edit(content=f"**Exporting {label}…**\nDB read failed during export.")

        # 4) Attach file (zip if big or requested)
        csv_bytes = buf.getvalue().encode("utf-8-sig")
        filename_base = f"{guild.name}-{getattr(target, 'name', target.id)}".replace("/", "_")
        csv_name = f"{filename_base}.csv"

        if compress or len(csv_bytes) > int(7.5 * 1024 * 1024):
            zbuf = io.BytesIO()
            with zipfile.ZipFile(zbuf, mode="w", compression=zipfile.ZIP_DEFLATED) as z:
                z.writestr(csv_name, csv_bytes)
            file = discord.File(io.BytesIO(zbuf.getvalue()), filename=f"{filename_base}.zip")
        else:
            file = discord.File(io.BytesIO(csv_bytes), filename=csv_name)

        summary = [
            f"**Export for {label}**",
            f"• Rows exported: **{exported:,}**",
            f"• Missing messages filled this run: **{missing_found:,}**" if fill_missing else "• Missing fill: skipped",
        ]
        try:
            await progress_msg.edit(content="\n".join(summary), attachments=[file])
        except Exception:
            # Fallback: send a new followup with the file if editing with attachment fails
            await interaction.followup.send("\n".join(summary), file=file, ephemeral=True)


    # ------------------------
    # /backread forum_scan
    # ------------------------
    @group.command(
        name="forum_scan",
        description="Fill missing messages for all threads in a forum channel.",
    )
    @app_commands.describe(
        forum="Forum parent channel to scan for missing thread messages.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def forum_scan_cmd(
        self,
        interaction: discord.Interaction,
        forum: discord.ForumChannel,
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

        can_read, reason = self._can_backread(forum, me)
        if not can_read:
            return await interaction.followup.send(
                f"I can’t read {self._label(forum)}: {reason}.", ephemeral=True
            )

        threads = await self._gather_threads(forum, include_private=False, me=me)
        threads = [t for t in threads if isinstance(t, discord.Thread)]
        if not threads:
            return await interaction.followup.send(
                f"No accessible threads found in {self._label(forum)}.", ephemeral=True
            )

        progress_msg = await interaction.followup.send(
            f"**Scanning {self._label(forum)}…**\nPreparing threads…", ephemeral=True
        )

        import time

        last_edit = 0.0
        threads_total = len(threads)
        threads_completed = 0
        total_missing = 0
        total_scanned = 0
        skipped: list[str] = []
        errors: list[str] = []

        async def _edit(
            *,
            stage: str,
            current: str | None,
            thread_found: int,
            thread_scanned: int,
            done: int,
            missing_sum: int,
            scanned_sum: int,
        ):
            nonlocal last_edit
            now = time.monotonic()
            if now - last_edit < 1.0:
                return
            parts = [f"**Scanning {self._label(forum)}…**"]
            if stage:
                parts.append(f"• {stage}")
            parts.extend(
                [
                    f"• Threads processed: **{done}/{threads_total}**",
                    f"• Missing messages filled: **{missing_sum:,}**",
                    f"• Messages scanned: **{scanned_sum:,}**",
                ]
            )
            if current:
                parts.append(
                    f"• Current: {current} — {stage} (found {thread_found:,}, scanned {thread_scanned:,})"
                )
            if skipped:
                parts.append(f"• Skipped threads: {len(skipped)}")
            if errors:
                parts.append(f"• Errors: {len(errors)}")
            try:
                await progress_msg.edit(content="\n".join(parts))
                last_edit = now
            except Exception:
                pass
        await _edit(
            stage="Preparing threads…",
            current=None,
            thread_found=0,
            thread_scanned=0,
            done=threads_completed,
            missing_sum=total_missing,
            scanned_sum=total_scanned,
        )

        thread_summaries: list[tuple[str, int, int]] = []

        for thread in threads:
            thread_label = self._label(thread)
            can_read_thread, reason_thread = self._can_backread(thread, me)
            if not can_read_thread:
                skipped.append(f"{thread_label} ({reason_thread})")
                await _edit(
                    stage="Skipped",
                    current=None,
                    thread_found=0,
                    thread_scanned=0,
                    done=threads_completed,
                    missing_sum=total_missing,
                    scanned_sum=total_scanned,
                )
                continue

            try:
                existing_ids = self._load_existing_ids(guild.id, thread.id)
            except Exception:
                log.exception(
                    "backread.forum_scan.load_existing_failed",
                    extra={"guild_id": guild.id, "channel_id": thread.id},
                )
                errors.append(f"{thread_label} (DB read failed)")
                await _edit(
                    stage="DB load failed",
                    current=thread_label,
                    thread_found=0,
                    thread_scanned=0,
                    done=threads_completed,
                    missing_sum=total_missing,
                    scanned_sum=total_scanned,
                )
                continue

            async def progress(stage: str, found: int, scanned: int, *, label=thread_label):
                await _edit(
                    stage=stage,
                    current=label,
                    thread_found=found,
                    thread_scanned=scanned,
                    done=threads_completed,
                    missing_sum=total_missing + found,
                    scanned_sum=total_scanned + scanned,
                )

            try:
                missing_found, scanned_count = await self._fill_missing_history(
                    thread,
                    existing_ids=existing_ids,
                    progress_cb=progress,
                )
            except discord.Forbidden:
                skipped.append(f"{thread_label} (missing Read Message History)")
                await _edit(
                    stage="Forbidden",
                    current=None,
                    thread_found=0,
                    thread_scanned=0,
                    done=threads_completed,
                    missing_sum=total_missing,
                    scanned_sum=total_scanned,
                )
                continue
            except discord.HTTPException as exc:
                log.exception(
                    "backread.forum_scan.history_http",
                    extra={"status": getattr(exc, "status", "?"), "channel_id": thread.id},
                )
                errors.append(
                    f"{thread_label} (Discord API HTTP {getattr(exc, 'status', '?')})"
                )
                await _edit(
                    stage="Discord API error",
                    current=thread_label,
                    thread_found=0,
                    thread_scanned=0,
                    done=threads_completed,
                    missing_sum=total_missing,
                    scanned_sum=total_scanned,
                )
                continue
            except Exception:
                log.exception(
                    "backread.forum_scan.thread_error",
                    extra={"guild_id": guild.id, "channel_id": thread.id},
                )
                errors.append(f"{thread_label} (unexpected error)")
                await _edit(
                    stage="Unexpected error",
                    current=thread_label,
                    thread_found=0,
                    thread_scanned=0,
                    done=threads_completed,
                    missing_sum=total_missing,
                    scanned_sum=total_scanned,
                )
                continue

            threads_completed += 1
            total_missing += missing_found
            total_scanned += scanned_count
            thread_summaries.append((thread_label, missing_found, scanned_count))

            await _edit(
                stage="Completed",
                current=thread_label,
                thread_found=missing_found,
                thread_scanned=scanned_count,
                done=threads_completed,
                missing_sum=total_missing,
                scanned_sum=total_scanned,
            )

        summary_lines = [
            f"**Forum scan complete for {self._label(forum)}.**",
            f"• Threads processed: **{threads_completed}/{threads_total}**",
            f"• Missing messages filled: **{total_missing:,}**",
            f"• Messages scanned: **{total_scanned:,}**",
        ]

        if thread_summaries:
            top = sorted(thread_summaries, key=lambda item: item[1], reverse=True)[:5]
            preview = ", ".join(
                f"{label}: +{missing:,}" for label, missing, _ in top if missing
            )
            if preview:
                summary_lines.append(f"• Top fills: {preview}")

        if skipped:
            preview = ", ".join(skipped[:5])
            if len(skipped) > 5:
                preview += ", …"
            summary_lines.append(f"• Skipped: {preview}")

        if errors:
            preview = "; ".join(errors[:3])
            if len(errors) > 3:
                preview += "; …"
            summary_lines.append(f"• Errors: {preview}")

        await progress_msg.edit(content="\n".join(summary_lines))

    # ------------------------
    # /backread audit
    # ------------------------
    @group.command(
        name="audit",
        description="Audit what the bot can read: channels/threads (incl. private) and what's skipped.",
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
            try:
                me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
            except Exception:
                return await interaction.followup.send(
                    "Unable to resolve my member object for permission checks.",
                    ephemeral=True,
                )

        # Target discovery: text + forum channels
        targets: List[discord.abc.GuildChannel] = []
        if channel:
            targets.append(channel)
        else:
            targets.extend(guild.text_channels)
            try:
                forum_channels = list(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                forum_channels = []
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
        notes: list[str] = []

        # Quick note about forum private threads limitation
        if include_private_threads:
            notes.append(
                "Private **forum** threads are not retrievable on this discord.py version; counted as unsupported."
            )

        for parent in targets:
            can_read, reason = self._can_backread(parent, me)
            label = self._label(parent)
            if not can_read:
                skipped_channels.append(f"{label} ({reason})")
                continue

            readable_channels += 1

            # Gather candidate threads (this can page archived public; private for TextChannel only)
            if include_archived_threads or include_private_threads:
                threads = await self._gather_threads(parent, include_private_threads, me)
            else:
                threads = [t for t in getattr(parent, "threads", []) if isinstance(t, discord.Thread)]

            # Tally and check each thread access
            for t in threads:
                total_threads += 1
                # classify private/public best-effort
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

        # Compose result
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
                f"• Threads discovered: **{total_threads:,}** "
                f"(public: **{public_threads:,}**, private: **{private_threads:,}**)",
                f"• Threads readable now: **{readable_threads:,}**",
            ]
        )
        if skipped_threads:
            tprev = ", ".join(skipped_threads[:8])
            if len(skipped_threads) > 8:
                tprev += ", ..."
            lines.append(f"• Threads skipped (perm): **{len(skipped_threads):,}**")
            lines.append(f"  ↳ {tprev}")

        if include_private_threads:
            lines.append("• Private threads (TextChannels): require **Manage Threads** to fetch archived ones.")
            lines.append("• Private threads (ForumChannels): **unsupported** to fetch when archived in this lib.")

        if notes:
            lines.append("\n".join(f"ℹ️ {n}" for n in notes))

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    # ------------------------
    # /backread stats
    # ------------------------
    @group.command(name="stats", description="Show archive stats for this server (messages, channels, unique users).")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def stats_cmd(self, interaction: discord.Interaction):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True)

        guild = interaction.guild
        if guild is None:
            return await interaction.followup.send("Guild not resolved.", ephemeral=True)

        try:
            msg_count, ch_count, user_count = self._fetch_archive_stats(guild.id)
        except Exception as exc:
            log.exception("backread.stats.failed", extra={"guild_id": getattr(guild, 'id', None)})
            return await interaction.followup.send(f"Failed to fetch stats: `{exc}`", ephemeral=True)

        lines = [
            f"**Archive stats for `{guild.name}`**",
            f"• Messages: **{msg_count:,}**",
            f"• Channels: **{ch_count:,}**",
            f"• Unique users: **{user_count:,}**",
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)


    def _load_existing_ids(self, guild_id: int, channel_id: int) -> set[int]:
        """Fetch all archived message IDs for a channel from the SQLite archive."""
        existing_ids: set[int] = set()
        con = message_archive.get_connection()
        with con:
            cur = con.cursor()
            cur.execute(
                "SELECT message_id FROM message_archive WHERE guild_id=? AND channel_id=?",
                (guild_id, channel_id),
            )
            for (mid,) in cur.fetchall():
                existing_ids.add(int(mid))
        return existing_ids

    async def _fill_missing_history(
        self,
        target: discord.abc.Messageable,
        *,
        existing_ids: set[int],
        progress_cb: Optional[Callable[[str, int, int], Awaitable[None]]] = None,
    ) -> tuple[int, int]:
        """Scan Discord history for a channel/thread and upsert any missing messages."""
        if not hasattr(target, "history"):
            return 0, 0

        missing_found = 0
        scanned = 0
        batch_rows: list[message_archive.ArchivedMessage] = []

        if progress_cb:
            await progress_cb("Scanning Discord history for gaps…", missing_found, scanned)

        async for msg in target.history(limit=None, oldest_first=True):  # type: ignore[attr-defined]
            scanned += 1
            if msg.id in existing_ids:
                if progress_cb and scanned % 250 == 0:
                    await progress_cb("Scanning Discord history for gaps…", missing_found, scanned)
                continue

            try:
                row = message_archive.from_discord_message(msg)
            except Exception as exc:
                log.debug(
                    "backread.fill.from_message_failed",
                    extra={"channel_id": getattr(target, "id", None), "msg_id": msg.id, "err": str(exc)},
                )
                if progress_cb and scanned % 250 == 0:
                    await progress_cb("Scanning Discord history for gaps…", missing_found, scanned)
                continue

            batch_rows.append(row)
            existing_ids.add(msg.id)
            missing_found += 1

            if len(batch_rows) >= 200:
                _, new_rows = message_archive.upsert_many(batch_rows, return_new=True)
                self._dispatch_archived_rows(new_rows)
                batch_rows.clear()
                if progress_cb:
                    await progress_cb("Filling gaps (writing batch)…", missing_found, scanned)

        if batch_rows:
            _, new_rows = message_archive.upsert_many(batch_rows, return_new=True)
            self._dispatch_archived_rows(new_rows)
            batch_rows.clear()
            if progress_cb:
                await progress_cb("Filling gaps (finalize)…", missing_found, scanned)

        return missing_found, scanned

    def _dispatch_archived_rows(self, rows: List[message_archive.ArchivedMessage]) -> None:
        if rows:
            self.bot.dispatch("backread_archive_batch", rows)

    @tasks.loop(time=dtime(hour=7, tzinfo=timezone.utc))
    async def _monthly_gap_scan(self) -> None:
        now = datetime.now(timezone.utc)
        month_key = now.strftime("%Y-%m")
        if now.day != 1 or self._last_gap_scan_month == month_key:
            return

        await self.bot.wait_until_ready()

        self._last_gap_scan_month = month_key

        for guild in list(self.bot.guilds):
            me = guild.me
            if not isinstance(me, discord.Member):
                try:
                    me = await guild.fetch_member(self.bot.user.id)  # type: ignore[arg-type]
                except Exception:
                    log.exception("backread.maintenance.resolve_member_failed", extra={"guild_id": guild.id})
                    continue

            channels: List[discord.abc.GuildChannel] = list(guild.text_channels)
            try:
                channels.extend(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                channels.extend(ch for ch in guild.channels if isinstance(ch, discord.ForumChannel))

            for channel in channels:
                can_read, reason = self._can_backread(channel, me)
                if not can_read:
                    log.debug(
                        "backread.maintenance.skip_channel",
                        extra={"guild_id": guild.id, "channel_id": getattr(channel, "id", None), "reason": reason},
                    )
                    continue

                messageables: List[discord.abc.Messageable] = []
                if isinstance(channel, discord.ForumChannel):
                    try:
                        threads = await self._gather_threads(channel, include_private=False, me=me)
                        messageables.extend(threads)
                    except Exception:
                        log.exception(
                            "backread.maintenance.gather_threads_failed",
                            extra={"guild_id": guild.id, "channel_id": getattr(channel, "id", None)},
                        )
                        continue
                else:
                    messageables.append(channel)  # type: ignore[arg-type]
                    try:
                        threads = await self._gather_threads(channel, include_private=False, me=me)
                        messageables.extend(threads)
                    except Exception:
                        log.debug(
                            "backread.maintenance.thread_scan_skipped",
                            extra={"guild_id": guild.id, "channel_id": getattr(channel, "id", None)},
                        )

                for target in messageables:
                    target_id = getattr(target, "id", None)
                    if target_id is None:
                        continue
                    try:
                        existing_ids = self._load_existing_ids(guild.id, target_id)  # type: ignore[arg-type]
                    except Exception:
                        log.exception(
                            "backread.maintenance.load_ids_failed",
                            extra={"guild_id": guild.id, "channel_id": target_id},
                        )
                        continue

                    try:
                        missing, scanned = await self._fill_missing_history(target, existing_ids=existing_ids)
                        log.info(
                            "backread.maintenance.channel_scanned",
                            extra={
                                "guild_id": guild.id,
                                "channel_id": target_id,
                                "missing_filled": missing,
                                "messages_scanned": scanned,
                            },
                        )
                    except discord.Forbidden:
                        log.debug(
                            "backread.maintenance.forbidden",
                            extra={"guild_id": guild.id, "channel_id": target_id},
                        )
                    except discord.HTTPException as exc:
                        log.exception(
                            "backread.maintenance.http_error",
                            extra={"guild_id": guild.id, "channel_id": target_id, "status": getattr(exc, "status", "?")},
                        )
                    except Exception:
                        log.exception(
                            "backread.maintenance.unexpected_error",
                            extra={"guild_id": guild.id, "channel_id": target_id},
                        )

    @_monthly_gap_scan.before_loop
    async def _monthly_gap_scan_ready(self) -> None:
        await self.bot.wait_until_ready()

    def cog_unload(self) -> None:
        try:
            self._monthly_gap_scan.cancel()
        except Exception:
            pass

    # ---- DB stats helper
    def _fetch_archive_stats(self, guild_id: int) -> Tuple[int, int, int]:
        """
        Returns (messages, distinct channels, distinct users) for the given guild_id.
        Tries, in order:
          1) message_archive.stats_summary(guild_id) -> {"messages": int, "channels": int, "users": int}
          2) message_archive.get_connection() -> sqlite3.Connection
          3) sqlite3.connect(message_archive.DB_PATH)
        """
        # 1) helper in your module
        if hasattr(message_archive, "stats_summary"):
            summary = message_archive.stats_summary(guild_id)  # type: ignore[attr-defined]
            return int(summary["messages"]), int(summary["channels"]), int(summary["users"])

        # 2/3) raw SQL
        conn = None
        close_after = False

        if hasattr(message_archive, ARCHIVE_GET_CONN_ATTR):
            get_conn = getattr(message_archive, ARCHIVE_GET_CONN_ATTR)
            conn = get_conn()  # type: ignore[call-arg]
        elif hasattr(message_archive, ARCHIVE_DB_PATH_ATTR):
            db_path = getattr(message_archive, ARCHIVE_DB_PATH_ATTR)
            conn = sqlite3.connect(db_path)
            close_after = True
        else:
            raise RuntimeError(
                "No way to reach the archive DB. Expose stats_summary(), get_connection(), or DB_PATH in message_archive."
            )

        try:
            cur = conn.cursor()
            sql_msg = f"SELECT COUNT(*) FROM {ARCHIVE_TABLE} WHERE {COL_GUILD_ID}=?"
            sql_ch = f"SELECT COUNT(DISTINCT {COL_CHANNEL_ID}) FROM {ARCHIVE_TABLE} WHERE {COL_GUILD_ID}=?"
            sql_user = f"SELECT COUNT(DISTINCT {COL_AUTHOR_ID}) FROM {ARCHIVE_TABLE} WHERE {COL_GUILD_ID}=?"

            cur.execute(sql_msg, (guild_id,))
            msg_count = int(cur.fetchone()[0])

            cur.execute(sql_ch, (guild_id,))
            ch_count = int(cur.fetchone()[0])

            cur.execute(sql_user, (guild_id,))
            user_count = int(cur.fetchone()[0])

            return msg_count, ch_count, user_count
        finally:
            try:
                cur.close()  # type: ignore[name-defined]
            except Exception:
                pass
            if close_after and conn:
                conn.close()

    # ------------------------
    # internals
    # ------------------------
    async def _gather_threads(
        self,
        channel: discord.abc.GuildChannel,
        include_private: bool,
        me: discord.Member,
    ) -> List[discord.Thread]:
        """Collect live and archived threads, handling API differences for forums."""
        threads: List[discord.Thread] = [
            t for t in getattr(channel, "threads", []) if isinstance(t, discord.Thread)
        ]
        seen = {thread.id for thread in threads}

        archived_iter = getattr(channel, "archived_threads", None)
        if callable(archived_iter):
            # Public archived threads (supported for TextChannel & ForumChannel)
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

        # Private archived threads: only pass private=True for TextChannel.
        if include_private and isinstance(channel, discord.TextChannel):
            perms = channel.permissions_for(me)
            if not perms.manage_threads:
                log.warning(
                    "backread.threads.private_missing_perm",
                    extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                )
                return threads
            if callable(archived_iter):
                try:
                    async for thread in archived_iter(limit=None, private=True):
                        if isinstance(thread, discord.Thread) and thread.id not in seen:
                            threads.append(thread)
                            seen.add(thread.id)
                except discord.Forbidden:
                    log.info(
                        "backread.threads.private_forbidden",
                        extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                    )
                except discord.HTTPException:
                    log.exception(
                        "backread.threads.private_error",
                        extra={"guild_id": channel.guild.id, "channel_id": channel.id},
                    )
        elif include_private and isinstance(channel, discord.ForumChannel):
            # Not supported in the library version you’re on.
            log.info(
                "backread.threads.private_forum_unsupported",
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
        """Stream a channel/thread history into the archive, with batch writes and progress logs."""
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
                    log.debug(
                        "backread.history.skip_existing",
                        extra={"guild_id": guild_id, "channel_id": channel.id, "label": label, "message_id": message.id},
                    )
                    continue
                try:
                    row = message_archive.from_discord_message(message)
                except Exception as exc:
                    stats.errors.append(f"{label}: {exc}")
                    continue

                if row.message_id in seen_ids:
                    log.debug(
                        "backread.history.skip_duplicate_batch",
                        extra={"guild_id": guild_id, "channel_id": channel.id, "label": label, "message_id": row.message_id},
                    )
                    continue

                seen_ids.add(row.message_id)
                batch.append(row)

                if len(batch) >= BATCH_SIZE:
                    first_batch_id = batch[0].message_id
                    last_batch_id = batch[-1].message_id
                    stored_now, new_rows = message_archive.upsert_many(batch, return_new=True)
                    self._dispatch_archived_rows(new_rows)
                    stored += stored_now
                    latest_seen_id = max(latest_seen_id or 0, last_batch_id)
                    if stored % PROGRESS_LOG_BATCH < BATCH_SIZE:
                        log.info(
                            "backread.history.progress",
                            extra={
                                "guild_id": guild_id,
                                "channel_id": channel.id,
                                "label": label,
                                "stored_so_far": stored,
                                "latest_seen_id": latest_seen_id,
                            },
                        )
                    log.debug(
                        "backread.history.store_batch",
                        extra={
                            "guild_id": guild_id,
                            "channel_id": channel.id,
                            "label": label,
                            "count": stored_now,
                            "first_id": first_batch_id,
                            "last_id": last_batch_id,
                        },
                    )
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
            stats.errors.append(f"{label}: unexpected error")
        finally:
            if batch:
                first_batch_id = batch[0].message_id
                last_batch_id = batch[-1].message_id
                stored_now, new_rows = message_archive.upsert_many(batch, return_new=True)
                self._dispatch_archived_rows(new_rows)
                stored += stored_now
                latest_seen_id = max(latest_seen_id or 0, last_batch_id)
                log.debug(
                    "backread.history.store_batch",
                    extra={
                        "guild_id": guild_id,
                        "channel_id": channel.id,
                        "label": label,
                        "count": stored_now,
                        "first_id": first_batch_id,
                        "last_id": last_batch_id,
                    },
                )

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

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild:
            return
        try:
            row = message_archive.from_discord_message(message)
        except Exception:
            return
        if message_archive.has_message(row.message_id):
            log.debug(
                "backread.live.skip_existing",
                extra={"guild_id": row.guild_id, "channel_id": row.channel_id, "message_id": row.message_id},
            )
            return

        message_archive.upsert_many([row])
        log.debug(
            "backread.live.stored",
            extra={"guild_id": row.guild_id, "channel_id": row.channel_id, "message_id": row.message_id},
        )

    @commands.Cog.listener()
    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        if not after.guild:
            return
        try:
            row = message_archive.from_discord_message(after)
        except Exception:
            return
        message_archive.upsert_many([row])
        log.debug(
            "backread.live.edit_stored",
            extra={"guild_id": row.guild_id, "channel_id": row.channel_id, "message_id": row.message_id},
        )

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
        log.debug(
            "backread.live.raw_edit_stored",
            extra={"guild_id": row.guild_id, "channel_id": row.channel_id, "message_id": row.message_id},
        )

    # Per-command error handler (correct signature for bound method)
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


async def setup(bot: commands.Bot):
    await bot.add_cog(BackreadCog(bot))
