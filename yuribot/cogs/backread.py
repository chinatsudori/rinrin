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

log = logging.getLogger(__name__)

BATCH_SIZE = 100
DELAY_BETWEEN_CHANNELS = 0.5


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

    group = app_commands.Group(name="backread", description="Archive server message history")

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

            try:
                forum_channels = list(guild.forum_channels)  # type: ignore[attr-defined]
            except AttributeError:
                forum_channels = []

            if not forum_channels:
                forum_channels = [
                    ch for ch in guild.channels if isinstance(ch, discord.ForumChannel)
                ]

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

    async def _gather_threads(
        self,
        channel: discord.abc.GuildChannel,
        include_private: bool,
        me: discord.Member,
    ) -> List[discord.Thread]:
        threads: List[discord.Thread] = [
            t for t in getattr(channel, "threads", []) if isinstance(t, discord.Thread)
        ]
        seen = {thread.id for thread in threads}

        archived_iter = getattr(channel, "archived_threads", None)
        if callable(archived_iter):
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

        if include_private:
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
                    log.debug(
                        "backread.history.skip_existing",
                        extra={
                            "guild_id": guild_id,
                            "channel_id": channel.id,
                            "label": label,
                            "message_id": message.id,
                        },
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
                        extra={
                            "guild_id": guild_id,
                            "channel_id": channel.id,
                            "label": label,
                            "message_id": row.message_id,
                        },
                    )
                    continue

                seen_ids.add(row.message_id)

                batch.append(row)
                if len(batch) >= BATCH_SIZE:
                    first_batch_id = batch[0].message_id
                    last_batch_id = batch[-1].message_id
                    stored_now = message_archive.upsert_many(batch)
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
                    batch.clear()
        except discord.Forbidden:
            stats.skipped.append(f"{label} (forbidden)")
            return
        except discord.HTTPException as exc:
            stats.errors.append(f"{label}: HTTP {getattr(exc, 'status', '?')}")
        except Exception as exc:  # pragma: no cover - safety net
            log.exception(
                "backread.history.error",
                extra={"guild_id": guild_id, "channel_id": channel.id, "label": label},
            )
            stats.errors.append(f"{label}: {exc}")
        finally:
            if batch:
                first_batch_id = batch[0].message_id
                last_batch_id = batch[-1].message_id
                stored_now = message_archive.upsert_many(batch)
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
                extra={
                    "guild_id": row.guild_id,
                    "channel_id": row.channel_id,
                    "message_id": row.message_id,
                },
            )
            return

        message_archive.upsert_many([row])
        log.debug(
            "backread.live.stored",
            extra={
                "guild_id": row.guild_id,
                "channel_id": row.channel_id,
                "message_id": row.message_id,
            },
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
            extra={
                "guild_id": row.guild_id,
                "channel_id": row.channel_id,
                "message_id": row.message_id,
            },
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
            extra={
                "guild_id": row.guild_id,
                "channel_id": row.channel_id,
                "message_id": row.message_id,
            },
        )

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
