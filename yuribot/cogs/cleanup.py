from __future__ import annotations

import asyncio
import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

log = logging.getLogger(__name__)

# Defaults you asked for
DEFAULT_FORUM_ID = 1428158868843921429
DEFAULT_BOT_AUTHOR_ID = 1266545197077102633


class CleanupCog(commands.Cog):
    """Utility cleanup commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Mod cleanup utilities")

    @group.command(name="mupurge", description="Purge messages posted by a bot from a Forum and its threads.")
    @app_commands.describe(
        forum_id="Forum channel ID (defaults to 1428158868843921429).",
        bot_author_id="Author ID to purge (defaults to 1266545197077102633).",
        include_private_archived="Also scan private archived threads (requires permissions).",
        dry_run="If true, only report what would be deleted."
    )
    @app_commands.checks.has_permissions(manage_messages=True)
    async def mupurge(
        self,
        interaction: discord.Interaction,
        forum_id: Optional[int] = None,
        bot_author_id: Optional[int] = None,
        include_private_archived: bool = True,
        dry_run: bool = False,
    ):
        """Crawl forum threads and delete messages authored by the specified bot."""
        await interaction.response.defer(ephemeral=True)

        forum_id = forum_id or DEFAULT_FORUM_ID
        bot_author_id = bot_author_id or DEFAULT_BOT_AUTHOR_ID

        # Resolve the forum channel
        chan = interaction.guild.get_channel(forum_id) if interaction.guild else None
        if chan is None:
            # try fetch as fallback
            try:
                chan = await self.bot.fetch_channel(forum_id)
            except Exception:
                return await interaction.followup.send(
                    f"Forum channel `{forum_id}` not found or not accessible.",
                    ephemeral=True,
                )

        if not isinstance(chan, discord.ForumChannel):
            return await interaction.followup.send(
                f"Channel `{forum_id}` is not a ForumChannel.",
                ephemeral=True,
            )

        # Permission sanity check
        me = chan.guild.me  # type: ignore
        perms = chan.permissions_for(me)
        if not (perms.read_message_history and perms.manage_messages and perms.view_channel):
            return await interaction.followup.send(
                "I need **View Channel**, **Read Message History**, and **Manage Messages** in that forum.",
                ephemeral=True,
            )

        # Collect threads to scan: active + archived (public [+ private opt-in])
        threads: list[discord.Thread] = []

        # Active threads present in cache
        try:
            threads.extend(list(chan.threads))
        except Exception:
            pass

        # Public archived threads
        try:
            async for th in chan.archived_threads(limit=None, private=False):
                threads.append(th)
        except Exception as e:
            log.warning("Failed to iterate public archived threads: %s", e)

        # Private archived threads (if requested and permitted)
        if include_private_archived:
            try:
                async for th in chan.archived_threads(limit=None, private=True):
                    threads.append(th)
            except Exception as e:
                log.warning("Failed to iterate private archived threads: %s", e)

        # Deduplicate by id (in case of overlap)
        seen = set()
        unique_threads: list[discord.Thread] = []
        for th in threads:
            if th.id not in seen:
                unique_threads.append(th)
                seen.add(th.id)

        # Walk each thread, delete authored messages
        total_scanned_threads = 0
        total_scanned_msgs = 0
        total_matches = 0
        total_deleted = 0

        # Progress ping cadence
        PROGRESS_EVERY = 100

        for th in unique_threads:
            total_scanned_threads += 1

            # Ensure we can read history for the thread
            # (joining helps for private/archived visibility in some cases)
            try:
                if not th.me:  # type: ignore[attr-defined]
                    await th.join()
            except Exception:
                # Not critical; continue
                pass

            try:
                async for msg in th.history(limit=None, oldest_first=True):
                    total_scanned_msgs += 1
                    if msg.author and msg.author.id == bot_author_id:
                        total_matches += 1
                        if not dry_run:
                            try:
                                await msg.delete()
                                total_deleted += 1
                            except discord.Forbidden:
                                log.warning("No permission to delete message %s in thread %s", msg.id, th.id)
                            except discord.HTTPException as e:
                                # Rate limit or other transient failure; small backoff
                                log.warning("HTTPException deleting %s in %s: %s", msg.id, th.id, e)
                                await asyncio.sleep(1.0)

                    # Light progress signal every N scanned messages
                    if total_scanned_msgs % PROGRESS_EVERY == 0:
                        await asyncio.sleep(0)  # cooperative yield

            except discord.Forbidden:
                log.info("Forbidden reading history in thread %s (%s)", th.id, th.name)
            except discord.HTTPException as e:
                log.warning("HTTP error reading history in thread %s: %s", th.id, e)

            # Be nice to the rate limiter between threads
            await asyncio.sleep(0.1)

        # Final summary
        dry = "DRY RUN — " if dry_run else ""
        await interaction.followup.send(
            f"{dry}Scanned **{total_scanned_threads}** threads and **{total_scanned_msgs}** messages "
            f"in forum <#{chan.id}>.\n"
            f"Found **{total_matches}** messages authored by `<@{bot_author_id}>`."
            f"{'' if dry_run else f' Deleted **{total_deleted}**.'}",
            ephemeral=True,
        )

    # Standard error surface for permission check
    @mupurge.error
    async def _mupurge_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message(
                "You need **Manage Messages** to run this.", ephemeral=True
            )
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
