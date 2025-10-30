# cogs/lifecycle.py
from __future__ import annotations
import asyncio
import signal
import logging
from typing import Optional

import discord
from discord.ext import commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

class LifecycleCog(commands.Cog):
    """Posts a reboot/shutdown notice to each guild’s bot-log channel on SIGINT/SIGTERM."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._posted = False
        self._sigint_old: Optional[callable] = None
        self._sigterm_old: Optional[callable] = None

    async def cog_load(self):
        loop = asyncio.get_running_loop()
        try:
            self._sigint_old = loop.add_signal_handler(signal.SIGINT, self._signal_trap, "SIGINT")
        except NotImplementedError:
            pass  # Windows or environment doesn’t support it
        try:
            self._sigterm_old = loop.add_signal_handler(signal.SIGTERM, self._signal_trap, "SIGTERM")
        except NotImplementedError:
            pass

    async def cog_unload(self):
        # nothing to restore; add_signal_handler doesn’t return previous on all loops
        pass

    def _signal_trap(self, sig_name: str):
        # schedule the notice once; don’t block the handler
        if not self._posted:
            self._posted = True
            self.bot.loop.create_task(self._post_shutdown_notice(sig_name))

    async def _post_shutdown_notice(self, sig_name: str):
        # Try to post to each guild’s configured bot-logs channel.
        # If one send fails, continue with others.
        text = f"🔄 {S('stats.botinfo.title')} — rebooting (signal: {sig_name})"
        sent_any = False
        for g in list(self.bot.guilds):
            try:
                ch_id = models.get_bot_logs_channel(g.id)
                if not ch_id:
                    continue
                ch = self.bot.get_channel(ch_id)
                if isinstance(ch, (discord.TextChannel, discord.Thread)):
                    await ch.send(text, allowed_mentions=discord.AllowedMentions.none())
                    sent_any = True
            except Exception:
                continue
        log.info("lifecycle.shutdown_notice", extra={"sent_any": sent_any, "signal": sig_name})

async def setup(bot: commands.Bot):
    await bot.add_cog(LifecycleCog(bot))
