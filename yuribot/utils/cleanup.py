from __future__ import annotations

import asyncio
import logging
from typing import Iterable, List, Optional, Tuple

import discord

log = logging.getLogger(__name__)

DEFAULT_FORUM_ID = 1428158868843921429
DEFAULT_BOT_AUTHOR_ID = 1266545197077102633
PROGRESS_EVERY = 100


async def resolve_forum_channel(
    bot: discord.Client, guild: Optional[discord.Guild], forum_id: int
) -> Optional[discord.ForumChannel]:
    channel = guild.get_channel(forum_id) if guild else None  # type: ignore[arg-type]
    if channel is None:
        try:
            channel = await bot.fetch_channel(forum_id)
        except Exception:
            return None
    if not isinstance(channel, discord.ForumChannel):
        return None
    return channel


def has_purge_permissions(me: discord.Member, channel: discord.ForumChannel) -> bool:
    perms = channel.permissions_for(me)
    return perms.view_channel and perms.read_message_history and perms.manage_messages


async def collect_threads(
    forum: discord.ForumChannel, *, include_private_archived: bool
) -> List[discord.Thread]:
    threads: List[discord.Thread] = []
    try:
        threads.extend(list(forum.threads))
    except Exception:
        pass

    try:
        async for thread in forum.archived_threads(limit=None, private=False):
            threads.append(thread)
    except Exception as exc:
        log.warning(
            "cleanup.archived.public_failed",
            extra={"forum_id": forum.id, "error": str(exc)},
        )

    if include_private_archived:
        try:
            async for thread in forum.archived_threads(limit=None, private=True):
                threads.append(thread)
        except Exception as exc:
            log.warning(
                "cleanup.archived.private_failed",
                extra={"forum_id": forum.id, "error": str(exc)},
            )

    uniq: List[discord.Thread] = []
    seen: set[int] = set()
    for thread in threads:
        if thread.id not in seen:
            uniq.append(thread)
            seen.add(thread.id)
    return uniq


async def purge_messages_from_threads(
    threads: Iterable[discord.Thread],
    *,
    author_id: int,
    dry_run: bool,
) -> Tuple[int, int, int, int]:
    scanned_threads = 0
    scanned_messages = 0
    matches = 0
    deleted = 0

    for thread in threads:
        scanned_threads += 1
        try:
            if not thread.me:  # type: ignore[attr-defined]
                await thread.join()
        except Exception:
            pass

        try:
            async for message in thread.history(limit=None, oldest_first=True):
                scanned_messages += 1
                author = getattr(message, "author", None)
                if author and getattr(author, "id", None) == author_id:
                    matches += 1
                    if not dry_run:
                        try:
                            await message.delete()
                            deleted += 1
                        except discord.Forbidden:
                            log.warning(
                                "cleanup.delete.forbidden",
                                extra={
                                    "thread_id": thread.id,
                                    "message_id": message.id,
                                },
                            )
                        except discord.HTTPException as exc:
                            log.warning(
                                "cleanup.delete.http_error",
                                extra={
                                    "thread_id": thread.id,
                                    "message_id": message.id,
                                    "error": str(exc),
                                },
                            )
                            await asyncio.sleep(1.0)

                if scanned_messages % PROGRESS_EVERY == 0:
                    await asyncio.sleep(0)

        except discord.Forbidden:
            log.info(
                "cleanup.history.forbidden",
                extra={"thread_id": thread.id, "thread_name": thread.name},
            )
        except discord.HTTPException as exc:
            log.warning(
                "cleanup.history.http_error",
                extra={"thread_id": thread.id, "error": str(exc)},
            )

        await asyncio.sleep(0.1)

    return scanned_threads, scanned_messages, matches, deleted
