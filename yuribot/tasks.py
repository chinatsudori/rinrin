from __future__ import annotations
import asyncio
import logging
import discord

from .utils.time import now_local, to_iso
from . import models
from .strings import S

log = logging.getLogger(__name__)

async def discussion_poster_loop(bot: discord.Client, interval_sec: int = 30):
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now_iso = to_iso(now_local())
            due = models.due_discussions(now_iso, limit=50)

            for row in due:
                section_id, series_id, label, s, e, start_iso, event_id, series_title, series_link = row

                srow = models.get_series(series_id)
                if not srow:
                    continue

              
                guild_id = srow[1]
                club_id = srow[5] if len(srow) >= 6 else None

                cfg = None
                discussion_forum_id = None

                if club_id is not None and hasattr(models, "get_club_cfg_by_id"):
                    try:
                        cfg = models.get_club_cfg_by_id(guild_id, club_id)
                        discussion_forum_id = cfg.get("discussion_forum_id") if cfg else None
                    except Exception:
                        cfg = None
                        discussion_forum_id = None

                if not discussion_forum_id and hasattr(models, "get_guild_cfg"):
                    gcfg = models.get_guild_cfg(guild_id)
                    discussion_forum_id = gcfg.get("discussion_forum_id") if gcfg else None

                if not discussion_forum_id:
                    log.warning("No discussion_forum_id set for guild %s; skipping section %s", guild_id, section_id)
                    continue

                guild = bot.get_guild(guild_id)
                if not guild:
                    continue

                forum = guild.get_channel(discussion_forum_id)
                if not isinstance(forum, discord.ForumChannel):
                    log.warning("Configured discussion_forum_id %s is not a Forum in guild %s", discussion_forum_id, guild_id)
                    continue

                name = S("discuss.thread.title", title=series_title, label=label)

                lines = [S("discuss.thread.body.header", title=series_title, label=label)]
                if series_link:
                    lines.append(S("discuss.thread.body.ref", link=series_link))
                if event_id:
                    url = f"https://discord.com/events/{guild_id}/{event_id}"
                    lines.append(S("discuss.thread.body.event", url=url))
                content = "\n".join(lines)

                try:
                    thread = await forum.create_thread(name=name, content=content)
                    models.mark_discussion_posted(section_id, thread.id)
                except Exception as ex:
                    log.exception("Failed to create discussion thread for section %s: %s", section_id, ex)

            await asyncio.sleep(interval_sec)

        except asyncio.CancelledError:
            break
        except Exception as ex:
            log.exception("discussion_poster_loop iteration failed: %s", ex)
            await asyncio.sleep(interval_sec)
