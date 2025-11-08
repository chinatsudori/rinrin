from __future__ import annotations

import os
import sys
import asyncio
import logging
import signal
from contextlib import suppress
from typing import Iterable, List, Sequence

import discord
from discord.ext import commands

from .db import ensure_db
from .strings import _STRINGS  # noqa: F401  (force-load strings at startup)

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("yuribot")


# -----------------------------------------------------------------------------
# Intents
# -----------------------------------------------------------------------------
def build_intents() -> discord.Intents:
    """Privileged intents must also be enabled in the Developer Portal."""
    intents = discord.Intents.default()
    intents.guilds = True
    intents.members = True  # privileged
    intents.message_content = True  # privileged
    intents.emojis_and_stickers = True
    intents.voice_states = True
    intents.guild_messages = True
    return intents


INTENTS = build_intents()


# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _parse_sync_guilds(value: str) -> List[int]:
    """Parse CSV of guild IDs into ints, ignoring empties; log bad tokens."""
    gids: List[int] = []
    for tok in (value or "").split(","):
        tok = tok.strip()
        if not tok:
            continue
        try:
            gids.append(int(tok))
        except ValueError:
            log.warning("Ignoring invalid guild id in SYNC_GUILDS: %r", tok)
    return gids


def _sync_mode() -> str:
    """Return validated command sync mode."""
    mode = (os.getenv("COMMAND_SYNC_MODE") or "guilds").strip().lower()
    if mode not in {"guilds", "global", "none"}:
        log.warning("Unknown COMMAND_SYNC_MODE=%r; defaulting to 'guilds'", mode)
        mode = "guilds"
    return mode


# -----------------------------------------------------------------------------
# Bot
# -----------------------------------------------------------------------------
class YuriBot(commands.Bot):
    def __init__(self) -> None:
        prefix = os.getenv("COMMAND_PREFIX", "!")
        super().__init__(command_prefix=prefix, intents=INTENTS)
        self._bg_tasks: List[asyncio.Task] = []
        self._shutdown_signal: str | None = None  # SIGINT/SIGTERM set by runner

    # ---- utilities ----
    def _log_channel_id(self) -> int | None:
        """Prefer BOTLOG_CHANNEL_ID; fallback LOG_CHANNEL_ID."""
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
            with suppress(Exception):
                ch = await self.fetch_channel(cid)  # type: ignore[assignment]

        if isinstance(ch, (discord.TextChannel, discord.Thread)):
            with suppress(Exception):
                await ch.send(content, allowed_mentions=discord.AllowedMentions.none())
                return True
        return False

    # ---- lifecycle ----
    async def setup_hook(self) -> None:
        ensure_db()
        log.info("Database ensured/connected.")

        clear_once = os.getenv("CLEAR_GLOBALS_ONCE") == "1"
        raw_guilds = os.getenv("SYNC_GUILDS") or os.getenv("DEV_GUILD_ID") or ""
        guild_ids = _parse_sync_guilds(raw_guilds)
        mode = _sync_mode()

        log.info(
            "sync env: clear_globals_once=%s, mode=%s, sync_guilds=%s",
            clear_once,
            mode,
            guild_ids or "<none>",
        )

        if clear_once:
            try:
                self.tree.clear_commands(guild=None)  # clear local GLOBAL table
                await self.tree.sync()  # push deletion to Discord
                log.warning("Cleared all GLOBAL commands.")
            except Exception:
                log.exception("Failed clearing global commands")

        # Load cogs to build the in-process command tree
        extensions: Sequence[str] = (
            "yuribot.cogs.admin",
            "yuribot.cogs.mod",
            "yuribot.cogs.botlog",
            "yuribot.cogs.welcome",
            "yuribot.cogs.coin_dice",
            "yuribot.cogs.mangaupdates",
            "yuribot.cogs.booked",
            "yuribot.cogs.timestamp",
            "yuribot.cogs.booly",
            "yuribot.cogs.polls",
            "yuribot.cogs.lifecycle",
        )
        await self._load_extensions(extensions)

        # Sync commands
        await self._sync_commands(guild_ids, mode)

        try:
            names = [cmd.qualified_name for cmd in self.tree.get_commands()]
            log.info(
                "Command tree built: %d commands registered in-process: %s",
                len(names),
                names,
            )
        except Exception:
            pass

    async def _load_extensions(self, names: Iterable[str]) -> None:
        for ext in names:
            try:
                await self.load_extension(ext)
                log.info("Loaded extension: %s", ext)
            except Exception:
                log.exception("Failed to load extension: %s", ext)

    async def _sync_commands(self, guild_ids: List[int], mode: str) -> None:
        """
        Publish commands according to mode:
          - 'guilds': copy global in-process tree into each guild and sync there.
          - 'global': push global commands.
          - 'none'  : skip publishing (useful in CI or read-only maintenance).
        """
        try:
            if mode == "none":
                log.info("Command sync skipped (mode=none).")
                return

            if mode == "global":
                synced = await self.tree.sync()
                log.info("Globally synced %d commands.", len(synced))
                return

            # guild mode
            if not guild_ids:
                log.error("No guild IDs provided. Set SYNC_GUILDS='gid1,gid2'.")
                return

            for gid in guild_ids:
                gobj = discord.Object(id=gid)
                # Copy the in-process GLOBAL definitions into this guild's table (local),
                # then push with a guild-scoped sync. This avoids publishing globals.
                self.tree.copy_global_to(guild=gobj)
                synced = await self.tree.sync(guild=gobj)
                log.info("Synced %d commands to guild %s.", len(synced), gid)

        except Exception:
            log.exception("Command sync failed.")

    async def on_ready(self) -> None:
        if self.user:
            log.info("Logged in as %s (%s)", self.user, self.user.id)

    async def close(self) -> None:
        # Post restart/shutdown notice before disconnecting
        note = (
            f"Rebooting… ({self._shutdown_signal})"
            if self._shutdown_signal
            else "Rebooting… (shutdown requested)"
        )
        with suppress(Exception):
            await self._post_botlog(note)

        log.info("Shutdown initiated: stopping background tasks and closing bot.")

        for t in self._bg_tasks:
            t.cancel()
        if self._bg_tasks:
            with suppress(asyncio.CancelledError):
                await asyncio.gather(*self._bg_tasks)

        await super().close()


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
async def _run_bot() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        log.error("Set DISCORD_TOKEN env var.")
        raise SystemExit(1)

    bot = YuriBot()
    stop_event = asyncio.Event()

    def _signal_handler(signame: str) -> None:
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
