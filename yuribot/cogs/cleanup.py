"""Shim extension maintained for backwards compatibility.

Cleanup commands now live in :class:`yuribot.cogs.admin.AdminCog`.  Importing this
module ensures the consolidated cog is loaded exactly once.
"""

from __future__ import annotations

import logging

from discord.ext import commands

from .admin import AdminCog

log = logging.getLogger(__name__)


async def setup(bot: commands.Bot):
    if bot.get_cog("AdminCog") is None:
        await bot.add_cog(AdminCog(bot))
        log.info("Loaded AdminCog via cleanup shim")
    else:
        log.info("AdminCog already loaded; cleanup shim skipped")
