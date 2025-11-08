from __future__ import annotations

import asyncio
import logging
from typing import Optional

import discord
from discord.ext import commands

from ..strings import S
from ..utils.lifecycle import (
    botlog_channels,
    build_shutdown_message,
    configure_signal_handlers,
)

log = logging.getLogger(__name__)


class LifecycleCog(commands.Cog):
    """Posts a reboot/shutdown notice to each guild's bot-log channel on SIGINT/SIGTERM."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._posted = False

    async def cog_load(self):
        loop = asyncio.get_running_loop()

        def handler(sig_name: str):
            if not self._posted:
                self._posted = True
                self.bot.loop.create_task(self._post_shutdown_notice(sig_name))

        configure_signal_handlers(self.bot, handler)

    async def _post_shutdown_notice(self, sig_name: str):
        message = build_shutdown_message(sig_name)
        sent_any = False
        for channel in botlog_channels(self.bot):
            try:
                await channel.send(
                    message, allowed_mentions=discord.AllowedMentions.none()
                )
                sent_any = True
            except Exception:
                continue
        log.info(
            "lifecycle.shutdown_notice",
            extra={"sent_any": sent_any, "signal": sig_name},
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(LifecycleCog(bot))
