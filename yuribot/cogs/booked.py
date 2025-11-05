from __future__ import annotations

import logging

import discord
from discord.ext import commands

from .. import models
from ..ui.booked import build_role_welcome_embed
from ..utils.booked import TARGET_ROLE_ID, role_ids

log = logging.getLogger(__name__)


class RoleWelcomeCog(commands.Cog):
    """DM users a welcome message the first time they receive TARGET_ROLE_ID."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if not after.guild or after.bot:
            return

        before_ids = role_ids(before.roles)
        after_ids = role_ids(after.roles)

        if TARGET_ROLE_ID not in after_ids or TARGET_ROLE_ID in before_ids:
            return

        guild_id = after.guild.id
        user_id = after.id

        try:
            already = models.role_welcome_already_sent(guild_id, user_id, TARGET_ROLE_ID)
        except Exception as exc:
            log.exception(
                "rolewelcome.db_check_failed",
                extra={"guild_id": guild_id, "user_id": user_id, "error": str(exc)},
            )
            already = True

        if already:
            log.debug("rolewelcome.already_sent", extra={"guild_id": guild_id, "user_id": user_id})
            return

        embed = build_role_welcome_embed(after.guild.name)
        try:
            await after.send(embed=embed)
            models.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            log.info(
                "rolewelcome.dm_sent",
                extra={"guild_id": guild_id, "user_id": user_id, "role_id": TARGET_ROLE_ID},
            )
        except discord.Forbidden:
            try:
                models.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            except Exception:
                pass
            log.warning(
                "rolewelcome.dm_blocked",
                extra={"guild_id": guild_id, "user_id": user_id, "role_id": TARGET_ROLE_ID},
            )
        except Exception as exc:
            log.exception(
                "rolewelcome.dm_failed",
                extra={"guild_id": guild_id, "user_id": user_id, "error": str(exc)},
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleWelcomeCog(bot))
