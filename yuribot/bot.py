from __future__ import annotations

import os
import sys
import asyncio
import logging
import signal
from contextlib import suppress
from typing import Optional, Iterable

import discord
from discord.ext import commands

from .db import ensure_db
from .tasks import discussion_poster_loop  # assumed: async coroutine or discord.ext.tasks.Loop
from .strings import _STRINGS  # imported so strings are loaded at startup


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("yuribot")

def build_intents() -> discord.Intents:
    intents = discord.Intents.default()
    # Privileged intents — must be enabled in the Discord Developer Portal
    intents.members = True
    intents.message_content = True
    # Additional signals your cogs use
    intents.guilds = True
    intents.emojis_and_stickers = True
    intents.voice_states = True
    intents.guild_messages = True
    return intents


INTENTS = build_intents()


class YuriBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)
        self._bg_tasks: list[asyncio.Task] = []

    async def setup_hook(self):
        # DB first
        ensure_db()
        log.info("Database ensured/connected.")

        # Load cogs (resilient)
        extensions = [
            "yuribot.cogs.admin",
            "yuribot.cogs.modlog",
            "yuribot.cogs.botlog",
            "yuribot.cogs.welcome",
            "yuribot.cogs.timeout",
            "yuribot.cogs.music",
            "yuribot.cogs.stats",
            "yuribot.cogs.emoji_stats",
            "yuribot.cogs.activity",
            "yuribot.cogs.movebot",
            "yuribot.cogs.coin_dice",
            "yuribot.cogs.mangaupdates",
            "yuribot.cogs.booked",
            "yuribot.cogs.collection",
            "yuribot.cogs.polls",
            "yuribot.cogs.series",
            "yuribot.cogs.movie",
        ]
        await self._load_extensions(extensions)

        # Start background jobs (store for shutdown)
        # If discussion_poster_loop is an async coroutine → create_task it.
        # If it's a discord.ext.tasks.Loop → .start()
        try:
            if hasattr(discussion_poster_loop, "start"):
                discussion_poster_loop.start(self)  # type: ignore[attr-defined]
                log.info("Started tasks.Loop discussion_poster_loop.")
            else:
                task = asyncio.create_task(discussion_poster_loop(self))
                self._bg_tasks.append(task)
                log.info("Started background task discussion_poster_loop.")
        except Exception:
            log.exception("Failed to start discussion_poster_loop.")

        # Command sync
        await self._sync_commands()

    async def _load_extensions(self, names: Iterable[str]) -> None:
        for ext in names:
            try:
                await self.load_extension(ext)
                log.info("Loaded extension: %s", ext)
            except Exception:
                log.exception("Failed to load extension: %s", ext)

    async def _sync_commands(self) -> None:
        dev_guild = os.getenv("DEV_GUILD_ID")
        try:
            if dev_guild:
                gid = int(dev_guild)
                guild = discord.Object(id=gid)
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                log.info("Synced %d commands to dev guild %s.", len(synced), gid)
            else:
                synced = await self.tree.sync()
                log.info("Globally synced %d commands.", len(synced))
        except Exception:
            log.exception("Command sync failed.")

    async def on_ready(self):
        user = self.user
        if user:
            log.info("Logged in as %s (%s)", user, user.id)

    async def close(self):
        log.info("Shutdown initiated: stopping background tasks and closing bot.")
        with suppress(Exception):
            if hasattr(discussion_poster_loop, "is_running") and discussion_poster_loop.is_running():  # type: ignore[attr-defined]
                discussion_poster_loop.stop()  # type: ignore[attr-defined]

        for t in self._bg_tasks:
            t.cancel()
        if self._bg_tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self._bg_tasks)
        await super().close()



async def _run_bot() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        log.error("Set DISCORD_TOKEN env var.")
        raise SystemExit(1)

    bot = YuriBot()

    stop_event = asyncio.Event()

    def _signal_handler(signame: str):
        log.warning("Received %s — requesting shutdown…", signame)
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with suppress(NotImplementedError):
            loop.add_signal_handler(sig, _signal_handler, sig.name)

    async def _start():
        try:
            await bot.start(token)
        except Exception:
            log.exception("Bot.start crashed")

    start_task = asyncio.create_task(_start())

    await stop_event.wait()

    with suppress(Exception):
        await bot.close()

    with suppress(asyncio.CancelledError):
        if not start_task.done():
            start_task.cancel()
        await start_task

    log.info("Shutdown complete.")


def main() -> None:
    try:
        asyncio.run(_run_bot())
    except KeyboardInterrupt:
        log.warning("KeyboardInterrupt — exiting.")
    except SystemExit:
        raise
    except Exception:
        log.exception("Fatal error in main()")
        raise


if __name__ == "__main__":
    main()
