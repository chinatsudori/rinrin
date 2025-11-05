from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from .. import models
from ..strings import S
from ..ui.timeout import build_dm_embed
from ..utils.time import now_local, to_iso
from ..utils.timeout import MAX_TIMEOUT_DAYS, can_act, clamp_duration

log = logging.getLogger(__name__)


class TimeoutCog(commands.Cog):
    """Moderation: timeouts with arbitrary durations (up to Discord's max)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="timeout",
        description="Timeout a member for custom days/hours/minutes/seconds (Discord cap ~28 days).",
    )
    @app_commands.describe(
        user="Member to timeout",
        days=f"Days (0-{MAX_TIMEOUT_DAYS})",
        hours="Hours (0-23)",
        minutes="Minutes (0-59)",
        seconds="Seconds (0-59)",
        reason="Optional reason (appears in Audit Log and DM)",
        dm_user="Attempt to DM the user about the timeout",
        post="If true, post publicly in this channel",
    )
    async def timeout(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        days: app_commands.Range[int, 0, MAX_TIMEOUT_DAYS] = 0,
        hours: app_commands.Range[int, 0, 23] = 0,
        minutes: app_commands.Range[int, 0, 59] = 0,
        seconds: app_commands.Range[int, 0, 59] = 0,
        reason: Optional[str] = None,
        dm_user: bool = True,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=not post)

        me = interaction.guild.me
        ok, why_key = can_act(interaction.user, user, me)
        if not ok:
            message = S(why_key) if why_key else S("timeout.error.unknown")
            log.info(
                "mod.timeout.denied",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "reason": why_key,
                },
            )
            return await interaction.followup.send(message, ephemeral=not post)

        try:
            delta = clamp_duration(days, hours, minutes, seconds)
        except ValueError as exc:
            return await interaction.followup.send(S(str(exc)), ephemeral=not post)

        until = discord.utils.utcnow() + delta

        if dm_user:
            try:
                duration_display = S("timeout.dm.value.duration", d=days, h=hours, m=minutes, s=seconds)
                embed = build_dm_embed(
                    guild_name=interaction.guild.name,
                    reason=reason or S("timeout.dm.no_reason"),
                    until_timestamp=int(until.timestamp()),
                    duration_display=duration_display,
                )
                await user.send(embed=embed)
            except Exception:
                pass

        try:
            await user.timeout(until, reason=reason[:512] if reason else None)
        except discord.Forbidden:
            return await interaction.followup.send(S("timeout.error.bot_perms"), ephemeral=not post)
        except discord.HTTPException:
            return await interaction.followup.send(S("timeout.error.http"), ephemeral=not post)

        try:
            models.add_timeout(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                actor_user_id=interaction.user.id,
                duration_seconds=int(delta.total_seconds()),
                reason=reason or "",
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("mod.timeout.persist_failed", extra={"guild_id": interaction.guild_id})

        await interaction.followup.send(
            S(
                "timeout.success",
                user=user.mention,
                duration=int(delta.total_seconds()),
                until=int(until.timestamp()),
            ),
            ephemeral=not post,
        )

        log.info(
            "mod.timeout.used",
            extra={
                "guild_id": interaction.guild_id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "duration": int(delta.total_seconds()),
                "dm_user": dm_user,
                "post": post,
            },
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(TimeoutCog(bot))
