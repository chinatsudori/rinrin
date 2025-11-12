from __future__ import annotations

import asyncio
import logging
from typing import List, Set

import discord
from discord import app_commands
from discord.ext import commands

from ..models import message_archive
from ..strings import S
from ..utils.archive import get_all_text_channels

log = logging.getLogger(__name__)


class ArchiveCog(
    commands.GroupCog, name="archive", description="Message archive tools"
):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._is_running: Set[int] = set()  # Set of guild_ids currently archiving

    # --- NEW: Automatic Listener ---
    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        """
        Automatically archives non-bot messages sent in guilds.
        """
        # Only log messages from guilds, and ignore bots
        if message.guild is None or message.author.bot:
            return

        try:
            # Convert the discord.Message to our database model
            archive_entry = message_archive.from_discord_message(message)
            # Insert/update it in the database
            message_archive.upsert_many([archive_entry])
        except ValueError:
            # Raised by from_discord_message if it's a DM or has no guild.
            # We already check for guild, but this is a safe fallback.
            pass
        except Exception as e:
            # Log any other DB errors but don't crash the bot
            log.error(
                f"Failed to auto-archive message {message.id} in guild {message.guild.id}: {e}",
                exc_info=e,
            )

    # --- Backfill Command ---

    @app_commands.command(
        name="backfill",
        description="Run a full message archive backfill for this server.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def archive_backfill(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        if interaction.guild.id in self._is_running:
            return await interaction.response.send_message(
                S("archive.backfill.already_running"), ephemeral=True
            )

        await interaction.response.send_message(
            S("archive.backfill.starting"), ephemeral=True
        )
        self._is_running.add(interaction.guild.id)

        total_messages_archived = 0
        total_channels_scanned = 0

        try:
            # Use the imported utility function
            channels_to_scan = await get_all_text_channels(interaction.guild)

            await interaction.followup.send(
                S("archive.backfill.found_channels", count=len(channels_to_scan)),
                ephemeral=True,
            )

            for i, channel in enumerate(channels_to_scan):
                if not isinstance(channel, discord.abc.Messageable):
                    continue

                # Check for permissions
                if not channel.permissions_for(
                    interaction.guild.me
                ).read_message_history:
                    log.warning(
                        f"Skipping channel {channel.name} ({channel.id}): Missing 'Read Message History' perms."
                    )
                    continue

                total_channels_scanned += 1
                channel_messages_archived = 0

                try:
                    # Find the last message ID we archived for this channel to resume
                    after_id = message_archive.max_message_id(
                        interaction.guild.id, channel.id
                    )
                    after_obj = discord.Object(id=after_id) if after_id else None

                    async for message in channel.history(
                        limit=None, after=after_obj, oldest_first=True
                    ):
                        if message.author.bot:
                            continue  # Skip bots

                        try:
                            archive_entry = message_archive.from_discord_message(
                                message
                            )
                            message_archive.upsert_many(
                                [archive_entry]
                            )  # Insert one by one
                            total_messages_archived += 1
                            channel_messages_archived += 1
                        except ValueError:
                            # Skip messages that the model rejects (partials)
                            pass
                        except Exception as e:
                            log.error(f"Failed to archive message {message.id}: {e}")

                    if channel_messages_archived > 0:
                        log.info(
                            f"Archived {channel_messages_archived} new messages from {channel.name} ({channel.id})"
                        )
                        if (
                            i > 0 and i % 10 == 0
                        ):  # Send an update every 10 channels (but not on the first one)
                            await interaction.followup.send(
                                S(
                                    "archive.backfill.progress_update",
                                    count=channel_messages_archived,
                                    channel=channel.mention,
                                ),
                                ephemeral=True,
                                allowed_mentions=discord.AllowedMentions.none(),
                            )

                except discord.Forbidden:
                    log.warning(
                        f"Skipping channel {channel.name} ({channel.id}): Forbidden."
                    )
                except Exception as e:
                    log.error(
                        f"Failed to scan channel {channel.name} ({channel.id}): {e}"
                    )

                await asyncio.sleep(1)  # Be nice to the API

            await interaction.followup.send(
                S(
                    "archive.backfill.complete",
                    channels=total_channels_scanned,
                    messages=total_messages_archived,
                ),
                ephemeral=True,
            )

        except Exception as e:
            await interaction.followup.send(
                S("archive.backfill.error", err=str(e)), ephemeral=True
            )
            log.exception(f"Archive task failed for guild {interaction.guild.id}")
        finally:
            if interaction.guild.id in self._is_running:
                self._is_running.remove(interaction.guild.id)


async def setup(bot: commands.Bot):
    await bot.add_cog(ArchiveCog(bot))
