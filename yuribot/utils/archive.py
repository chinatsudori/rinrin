from __future__ import annotations

import logging
from typing import List

import discord

log = logging.getLogger(__name__)


async def get_all_text_channels(guild: discord.Guild) -> List[discord.abc.Messageable]:
    """
    Gets all text channels, voice chats, stage chats, and all active/archived forum threads.
    """
    all_channels: List[discord.abc.Messageable] = []

    for channel in guild.channels:
        if isinstance(
            channel, (discord.TextChannel, discord.VoiceChannel, discord.StageChannel)
        ):
            all_channels.append(channel)
        elif isinstance(channel, discord.ForumChannel):
            # Add active threads
            all_channels.extend(channel.threads)
            try:
                # Also fetch archived threads in forums
                async for thread in channel.archived_threads(limit=None):
                    all_channels.append(thread)
            except discord.Forbidden:
                log.warning(
                    f"No perms to read archived threads in {channel.name} ({channel.id})"
                )
            except Exception as e:
                log.error(f"Failed to get archived threads for {channel.name}: {e}")

    # Also iterate all text channels to get their archived threads
    for channel in guild.text_channels:
        try:
            async for thread in channel.archived_threads(limit=None):
                if thread not in all_channels:
                    all_channels.append(thread)
        except discord.Forbidden:
            log.warning(
                f"No perms to read archived threads in text channel {channel.name} ({channel.id})"
            )
        except Exception:
            pass  # Ignore other errors

    return all_channels
