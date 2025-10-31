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
    intents = discord.Intents.default()
    intents.members = True               # privileged (enable in Dev Portal)
    intents.message_content = True       # privileged (enable in Dev Portal)
    intents.guilds = True
    intents.emojis_and_stickers = True
    intents.voice_states = True
    intents.guild_messages = True
    return intents

INTENTS = build_intents()

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------
def _schema_bump_suffix() -> str:
    bump = (os.getenv("COMMAND_SCHEMA_BUMP") or "").strip()
    return f" · v{bump}" if bump else ""

# -----------------------------------------------------------------------------
# Bot
# -----------------------------------------------------------------------------
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

        # 0) One-time global clear BEFORE building the tree (your order)
        if (os.getenv("CLEAR_GLOBALS_ONCE") or "").strip() == "1":
            try:
                self.tree.clear_commands(guild=None)
                await self.tree.sync()
                log.warning("Global command registry cleared (one-time).")
            except Exception:
                log.exception("Failed clearing global commands")

        # 1) Load cogs -> builds the in-process command tree
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
        ]
        await self._load_extensions(extensions)

        # 1.5) Optional schema-bump to force Discord to accept a diff
        suffix = _schema_bump_suffix()
        if suffix:
            changed = 0
            for cmd in self.tree.walk_commands():
                desc = (cmd.description or "").rstrip()
                if not desc.endswith(suffix):
                    cmd.description = (desc + suffix)[:100]  # safety cap
                    changed += 1
            log.info("schema-bump: tagged %d commands with %r", changed, suffix)

        # 2) Sync to guilds (no globals)
        await self._sync_commands()

    async def _load_extensions(self, names: Iterable[str]) -> None:
        for ext in names:
            try:
                await self.load_extension(ext)
                log.info("Loaded extension: %s", ext)
            except Exception:
                log.exception("Failed to load extension: %s", ext)

        # After loading, log the commands we actually registered
        try:
            built = sorted([c.qualified_name for c in self.tree.walk_commands()])
            log.info("Command tree built: %d commands registered in-process: %s", len(built), built)
        except Exception:
            pass

    async def _sync_commands(self) -> None:
        """
        Sync application commands without using global commands by default.

        Env:
          COMMAND_SYNC_MODE   = "guilds" | "global"     (default: "guilds")
          SYNC_GUILDS         = comma-separated guild IDs (e.g. "123,456")
        """
        mode = (os.getenv("COMMAND_SYNC_MODE") or "guilds").strip().lower()
        raw = os.getenv("SYNC_GUILDS") or os.getenv("DEV_GUILD_ID")  # backward compat
        gids = [int(x.strip()) for x in (raw or "").split(",") if x.strip()]

        log.info("sync env: mode=%s, guilds=%s", mode, gids or "(none)")

        try:
            if mode == "global":
                synced = await self.tree.sync()
                log.info("Globally synced %d commands.", len(synced))
                return

            if not gids:
                log.error("No guild IDs provided. Set SYNC_GUILDS='gid1,gid2'.")
                return

            for gid in gids:
                guild_obj = discord.Object(id=gid)
                synced = await self.tree.sync(guild=guild_obj)
                log.info("Synced %d commands to guild %s.", len(synced), gid)

        except Exception:
            log.exception("Command sync failed.")

    async def on_ready(self):
        # Strong diagnostics: app identity + invite URL
        try:
            app = await self.application_info()
            log.info(
                "app: name=%s id=%s owner=%s (%s)",
                getattr(app, "name", None),
                getattr(app, "id", None),
                getattr(app.owner, "name", None),
                getattr(app.owner, "id", None),
            )
            log.info(
                "oauth url: https://discord.com/oauth2/authorize?client_id=%s&scope=bot%%20applications.commands&permissions=0",
                getattr(app, "id", None),
            )
        except Exception:
            log.exception("could not fetch application_info")

        if self.user:
            log.info("Logged in as %s (%s)", self.user, self.user.id)

    async def close(self):
        # Post a restart/shutdown notice before disconnecting
        note = f"Rebooting… ({self._shutdown_signal})" if self._shutdown_signal else "Rebooting… (shutdown requested)"
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

# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------
async def _run_bot() -> None:
    token = os.getenv("DISCORD_TOKEN")
    log.info(
        "boot env: clear_globals_once=%s, schema_bump=%s, mode=%s, guilds=%s",
        os.getenv("CLEAR_GLOBALS_ONCE"),
        os.getenv("COMMAND_SCHEMA_BUMP"),
        os.getenv("COMMAND_SYNC_MODE"),
        os.getenv("SYNC_GUILDS"),
    )
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
