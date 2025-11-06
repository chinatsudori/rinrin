from __future__ import annotations

import datetime
from typing import AsyncIterator, Optional, Union

import discord

SnowflakeTime = Union[discord.abc.Snowflake, datetime.datetime]


async def iter_forum_archived_threads(
    forum: discord.ForumChannel,
    *,
    private: bool = False,
    joined: bool = False,
    limit: Optional[int] = 100,
    before: Optional[SnowflakeTime] = None,
) -> AsyncIterator[discord.Thread]:
    """Iterate over archived forum threads, including private ones when requested.

    discord.py 2.6 removed the ``private`` keyword argument from
    :meth:`discord.ForumChannel.archived_threads`. This helper mirrors the
    behaviour of :meth:`discord.TextChannel.archived_threads` so existing code can
    continue to iterate over private archived threads.
    """

    before_timestamp: Optional[str] = None
    if isinstance(before, datetime.datetime):
        before_timestamp = before.isoformat()
    elif before is not None:
        before_timestamp = discord.utils.snowflake_time(before.id).isoformat()

    update_before = lambda data: data["thread_metadata"]["archive_timestamp"]
    endpoint = forum.guild._state.http.get_public_archived_threads

    if joined:
        update_before = lambda data: data["id"]
        endpoint = forum.guild._state.http.get_joined_private_archived_threads
    elif private:
        endpoint = forum.guild._state.http.get_private_archived_threads

    remaining = limit

    while True:
        retrieve = 100
        if remaining is not None:
            if remaining <= 0:
                return
            retrieve = max(2, min(retrieve, remaining))

        data = await endpoint(forum.id, before=before_timestamp, limit=retrieve)
        threads = data.get("threads", [])
        if not threads:
            return

        for raw_thread in threads:
            yield discord.Thread(guild=forum.guild, state=forum.guild._state, data=raw_thread)
            if remaining is not None:
                remaining -= 1
                if remaining <= 0:
                    return

        if not data.get("has_more", False):
            return

        before_timestamp = update_before(threads[-1])
