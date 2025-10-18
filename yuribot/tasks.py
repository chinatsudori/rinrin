from __future__ import annotations

import asyncio
import logging
import os
import random
from typing import Optional

import discord

from .utils.time import now_local, to_iso
from . import models
from .strings import S

log = logging.getLogger(__name__)

# Singleflight-style lock so overlapping ticks don't stampede the API.
_RUN_LOCK = asyncio.Lock()

DEFAULT_INTERVAL_SEC = int(os.getenv("DISCUSSION_LOOP_INTERVAL_SEC", "30"))
MAX_THREADS_PER_TICK = int(os.getenv("DISCUSSION_LOOP_MAX_PER_TICK", "50"))

def _event_url(guild_id: int, event_id: Optional[int]) -> Optional[str]:
    if not event_id:
        return None
    # Standard guild scheduled events URL
    return f"https://discord.com/events/{guild_id}/{event_id}"

async def _resolve_forum(guild: discord.Guild, guild_id: int) -> Optional[discord.ForumChannel]:
    """Find the configured discussion forum for this guild.
    NOTE: If you need club-specific forums, return club_id in due_discussions() and switch lookup."""
    gcfg = models.get_guild_cfg(guild_id)
    forum_id = gcfg.get("discussion_forum_id") if gcfg else None
    if not forum_id:
        log.warning("discussion: no discussion_forum_id set for guild %s", guild_id)
        return None
    ch = guild.get_channel(forum_id)
    if not isinstance(ch, discord.ForumChannel):
        log.warning("discussion: configured discussion_forum_id=%s is not a Forum in guild %s", forum_id, guild_id)
        return None
    return ch

async def _create_thread(
    forum: discord.ForumChannel,
    *,
    name: str,
    content: str,
) -> Optional[discord.Thread]:
    """Create a forum thread with modest retry for transient HTTP failures."""
    # Minimal backoff: 0, 0.5, 1.0s
    for attempt, delay in enumerate((0.0, 0.5, 1.0), start=1):
        try:
            if delay:
                await asyncio.sleep(delay)
            return await forum.create_thread(name=name, content=content)
        except discord.Forbidden:
            log.error("discussion: forbidden creating thread in forum #%s", getattr(forum, "id", "n/a"))
            return None
        except discord.HTTPException as e:
            log.warning("discussion: HTTP error on create_thread attempt %s: %s", attempt, e)
        except Exception as e:
            log.exception("discussion: unexpected error on create_thread attempt %s: %s", attempt, e)
    return None

async def discussion_poster_loop(bot: discord.Client, interval_sec: int = DEFAULT_INTERVAL_SEC):
    """Background loop: post scheduled discussion threads when due.

    Safe to cancel (SIGINT/SIGTERM). Uses a lock to avoid concurrent runs.
    """
    # Wait for shards/gateway
    await bot.wait_until_ready()
    jitter = 0.0

    while not bot.is_closed():
        try:
            if _RUN_LOCK.locked():
                # Another tick is running; skip this cycle to avoid stampede
                await asyncio.sleep(interval_sec)
                continue

            async with _RUN_LOCK:
                now_iso = to_iso(now_local())
                rows = models.due_discussions(now_iso, limit=MAX_THREADS_PER_TICK)
                if not rows:
                    log.debug("discussion: no due sections at %s", now_iso)
                else:
                    log.info("discussion: %s section(s) due at %s", len(rows), now_iso)

                for row in rows:
                    # Schema from your models.due_discussions():
                    # id, series_id, label, start_ch, end_ch, discussion_start, discussion_event_id, title, link
                    (
                        section_id,
                        series_id,
                        label,
                        start_ch,
                        end_ch,
                        start_iso,
                        event_id,
                        series_title,
                        series_link,
                    ) = row

                    # get_series() returns: (id, guild_id, title, link, status)
                    srow = models.get_series(series_id)
                    if not srow:
                        log.warning("discussion: series %s not found; skipping section %s", series_id, section_id)
                        continue

                    series_id_db, guild_id, _title, _link, _status = srow

                    guild = bot.get_guild(guild_id)
                    if not guild:
                        log.warning("discussion: guild %s not found in cache; skipping section %s", guild_id, section_id)
                        continue

                    forum = await _resolve_forum(guild, guild_id)
                    if not forum:
                        continue

                    name = S("discuss.thread.title", title=series_title, label=label)
                    lines = [S("discuss.thread.body.header", title=series_title, label=label)]
                    if series_link:
                        lines.append(S("discuss.thread.body.ref", link=series_link))
                    evt_url = _event_url(guild_id, event_id)
                    if evt_url:
                        lines.append(S("discuss.thread.body.event", url=evt_url))
                    content = "\n".join(lines)

                    thread = await _create_thread(forum, name=name, content=content)
                    if thread is None:
                        continue

                    try:
                        models.mark_discussion_posted(section_id, thread.id)
                        log.info("discussion: posted section %s → thread #%s (%s)", section_id, thread.id, thread.name)
                    except Exception as ex:
                        log.exception("discussion: failed to mark section %s posted: %s", section_id, ex)

            jitter = random.uniform(0, 0.25 * interval_sec)
            await asyncio.sleep(interval_sec + jitter)

        except asyncio.CancelledError:
            log.info("discussion: loop cancelled; exiting cleanly.")
            break
        except Exception as ex:
            # Catch-all to avoid loop dying; wait a normal interval before retry
            log.exception("discussion: iteration failed: %s", ex)
            await asyncio.sleep(interval_sec)
