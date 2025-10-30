from __future__ import annotations

import os
import sys
import asyncio
import logging
import signal
from contextlib import suppress
from typing import Iterable

import discord
from discord.ext import commands

from .db import ensure_db
from .strings import _STRINGS  # noqa: F401  # force-load strings at startup

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("yuribot")

# --- Intents ---------------------------------------------------------------

def build_intents() -> discord.Intents:
    intents = discord.Intents.default()
    intents.members = True               # privileged (enable in Dev Portal)
    intents.message_content = True       # privileged (enable in Dev Portal)
    intents.guilds = True
    intents.emojis_and_stickers = True
    intents.voice_states = True
    intents.guild_messages = True
    return intents

INTENTS = build_intents()

# --- Bot -------------------------------------------------------------------

class YuriBot(commands.Bot):
    def __init__(self):
        super().__init__(command_prefix="!", intents=INTENTS)
        self._bg_tasks: list[asyncio.Task] = []
        self._shutdown_signal: str | None = None  # SIGINT/SIGTERM name set by runner

    # ---- utilities ----

    def _log_channel_id(self) -> int | None:
        # Prefer BOTLOG_CHANNEL_ID; fallback LOG_CHANNEL_ID
        cid = os.getenv("BOTLOG_CHANNEL_ID") or os.getenv("LOG_CHANNEL_ID")
        try:
            return int(cid) if cid else None
        except Exception:
            return None

    async def _post_botlog(self, content: str) -> bool:
        """Best-effort: post a message to the configured botlog channel."""
        cid = self._log_channel_id()
        if not cid:
            return False
        ch = self.get_channel(cid)
        if not isinstance(ch, (discord.TextChannel, discord.Thread)):
            # Try fetching if not cached yet
            with suppress(Exception):
                ch = await self.fetch_channel(cid)  # type: ignore[assignment]
        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            with suppress(Exception):
                await ch.send(content, allowed_mentions=discord.AllowedMentions.none())
                return True
        return False

    # ---- lifecycle ----

    async def setup_hook(self):
        # DB first
        ensure_db()
        log.info("Database ensured/connected.")

        # Load cogs
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
            "yuribot.cogs.cleanup",
            "yuribot.cogs.timestamp",
            "yuribot.cogs.booly",
            "yuribot.cogs.polls",
            "yuribot.cogs.lifecycle",
            "yuribot.cogs.maintact",
        ]
        await self._load_extensions(extensions)

        # Sync application commands (no duplication)
        await self._sync_commands()

    async def _load_extensions(self, names: Iterable[str]) -> None:
        for ext in names:
            try:
                await self.load_extension(ext)
                log.info("Loaded extension: %s", ext)
            except Exception:
                log.exception("Failed to load extension: %s", ext)

    async def _sync_commands(self) -> None:
        """
        Sync application commands without clearing the tree.

        Env:
          COMMAND_SYNC_MODE = "guild" | "global"  (default: "global")
          DEV_GUILD_ID      = <id>  (required if mode=guild)
        """
        mode = (os.getenv("COMMAND_SYNC_MODE") or "global").strip().lower()
        try:
            if mode == "guild":
                gid = int(os.environ["DEV_GUILD_ID"])
                guild = discord.Object(id=gid)
                # Copy all currently-registered global commands into the dev guild and sync there only.
                self.tree.copy_global_to(guild=guild)
                synced = await self.tree.sync(guild=guild)
                log.info("Synced %d commands to dev guild %s.", len(synced), gid)
            else:
                # Global sync only. Do NOT clear; just publish what cogs registered.
                synced = await self.tree.sync()
                log.info("Globally synced %d commands.", len(synced))
        except KeyError:
            log.error("COMMAND_SYNC_MODE=guild requires DEV_GUILD_ID.")
        except Exception:
            log.exception("Command sync failed.")


    async def on_ready(self):
        if self.user:
            log.info("Logged in as %s (%s)", self.user, self.user.id)

    async def close(self):
        # Post a restart/shutdown notice before disconnecting
        note = None
        if self._shutdown_signal:
            note = f"Rebooting… ({self._shutdown_signal})"
        else:
            note = "Rebooting… (shutdown requested)"

        with suppress(Exception):
            await self._post_botlog(note)

        log.info("Shutdown initiated: stopping background tasks and closing bot.")
        # Stop any tasks your cogs may have registered on the bot object
        with suppress(Exception):
            if "discussion_poster_loop" in globals():
                loop_obj = globals()["discussion_poster_loop"]
                if getattr(loop_obj, "is_running", lambda: False)():
                    loop_obj.stop()

        for t in self._bg_tasks:
            t.cancel()
        if self._bg_tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self._bg_tasks)

        await super().close()

# --- Runner ----------------------------------------------------------------

async def _run_bot() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        log.error("Set DISCORD_TOKEN env var.")
        raise SystemExit(1)

    bot = YuriBot()
    stop_event = asyncio.Event()

    def _signal_handler(signame: str):
        # record signal for the bot; trigger graceful shutdown
        bot._shutdown_signal = signame
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

    # Wait for signal
    await stop_event.wait()

    # Close gracefully (will post reboot message)
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
