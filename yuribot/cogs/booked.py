from __future__ import annotations

import logging
from typing import Iterable

import discord
from discord.ext import commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

# Target role to watch
TARGET_ROLE_ID = 1417963012492623892  # <- change if needed


def _ids(roles: Iterable[discord.Role]) -> set[int]:
    return {r.id for r in roles if isinstance(r, discord.Role)}


class RoleWelcomeCog(commands.Cog):
    """DM users a welcome message the first time they receive TARGET_ROLE_ID."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        # Sanity
        if not after.guild:
            return
        if after.bot:
            return

        before_ids = _ids(before.roles)
        after_ids = _ids(after.roles)

        # Role newly added?
        if TARGET_ROLE_ID not in after_ids:
            return  # user doesn't have it (or lost it)
        if TARGET_ROLE_ID in before_ids:
            return  # they already had it before this update

        guild_id = after.guild.id
        user_id = after.id

        # First time ever? (tracked in DB)
        try:
            already = models.role_welcome_already_sent(guild_id, user_id, TARGET_ROLE_ID)
        except Exception as e:
            log.exception("rolewelcome: DB check failed (g=%s u=%s): %r", guild_id, user_id, e)
            already = True  # be safe: avoid duplicate DM spam on DB failure

        if already:
            log.debug("rolewelcome: already sent (g=%s u=%s)", guild_id, user_id)
            return

        # Compose DM
        title = S("rolewelcome.title")
        desc = S("rolewelcome.desc")
        try:
            embed = discord.Embed(title=title, description=desc, color=discord.Color.green())
            embed.set_footer(text=S("rolewelcome.footer", guild=after.guild.name))
            await after.send(embed=embed)
            models.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            log.info("rolewelcome: DM sent to %s (%s) for role %s in guild %s",
                     after, user_id, TARGET_ROLE_ID, guild_id)
        except discord.Forbidden:
            # Can't DM — mark as sent anyway to avoid reattempting every role toggle
            try:
                models.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            except Exception:
                pass
            log.warning("rolewelcome: cannot DM %s (%s) — privacy settings.", after, user_id)
        except Exception as e:
            log.exception("rolewelcome: unexpected error DMing %s (%s): %r", after, user_id, e)


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleWelcomeCog(bot))
