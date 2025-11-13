from __future__ import annotations

import logging

import discord
from discord.ext import commands

from discord.ext.commands import guild_only
from ..models import role_welcome
from ..ui.booked import build_role_welcome_embed
from ..utils.booked import TARGET_ROLE_ID, role_ids

log = logging.getLogger(__name__)


class RoleWelcomeCog(commands.Cog):
    """DM users a welcome message the first time they receive TARGET_ROLE_ID."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.hybrid_group(
        name="booked", description="Book club utilities", invoke_without_command=True
    )
    @guild_only()
    async def booked(self, ctx: commands.Context) -> None:
        """Group for book club helpers."""
        # Minimal help if invoked without a subcommand
        await ctx.reply("Use `/booked dm` to test the welcome DM.")

    @booked.command(
        name="dm", description="DM yourself the book club welcome message (test)"
    )
    @guild_only()
    async def booked_dm(
        self,
        ctx: commands.Context,
        record: bool = False,
    ) -> None:
        """
        Send the rolewelcome embed to the invoker's DMs.
        - record=False (default): does NOT mark as sent in DB.
        - record=True: marks as sent in DB for TARGET_ROLE_ID.
        """
        guild = ctx.guild
        if not guild:
            await ctx.reply("This command must be used in a server.")
            return

        embed = build_role_welcome_embed(guild.name)
        try:
            await ctx.author.send(embed=embed)
            if record:
                try:
                    role_welcome.role_welcome_mark_sent(
                        guild.id, ctx.author.id, TARGET_ROLE_ID
                    )
                except Exception as exc:
                    # Log but don't fail the user flow
                    log.exception(
                        "rolewelcome.test_mark_failed",
                        extra={
                            "guild_id": guild.id,
                            "user_id": ctx.author.id,
                            "error": str(exc),
                        },
                    )
            await ctx.reply(
                f"Sent the welcome DM to you{' and recorded it' if record else ''}.",
                ephemeral=bool(getattr(ctx, "interaction", None)),
            )
        except discord.Forbidden:
            # User's DMs are closed or bot blocked
            await ctx.reply(
                "I couldn't DM you (permissions/DMs closed). Open your DMs and try again.",
                ephemeral=bool(getattr(ctx, "interaction", None)),
            )
        except Exception as exc:
            log.exception(
                "rolewelcome.test_dm_failed",
                extra={
                    "guild_id": guild.id,
                    "user_id": ctx.author.id,
                    "error": str(exc),
                },
            )
            await ctx.reply(
                "Something went wrong while sending your test DM.",
                ephemeral=bool(getattr(ctx, "interaction", None)),
            )

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
            already = role_welcome.role_welcome_already_sent(
                guild_id, user_id, TARGET_ROLE_ID
            )
        except Exception as exc:
            log.exception(
                "rolewelcome.db_check_failed",
                extra={"guild_id": guild_id, "user_id": user_id, "error": str(exc)},
            )
            already = True

        if already:
            log.debug(
                "rolewelcome.already_sent",
                extra={"guild_id": guild_id, "user_id": user_id},
            )
            return

        embed = build_role_welcome_embed(after.guild.name)
        try:
            await after.send(embed=embed)
            role_welcome.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            log.info(
                "rolewelcome.dm_sent",
                extra={
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "role_id": TARGET_ROLE_ID,
                },
            )
        except discord.Forbidden:
            try:
                role_welcome.role_welcome_mark_sent(guild_id, user_id, TARGET_ROLE_ID)
            except Exception:
                pass
            log.warning(
                "rolewelcome.dm_blocked",
                extra={
                    "guild_id": guild_id,
                    "user_id": user_id,
                    "role_id": TARGET_ROLE_ID,
                },
            )
        except Exception as exc:
            log.exception(
                "rolewelcome.dm_failed",
                extra={"guild_id": guild_id, "user_id": user_id, "error": str(exc)},
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(RoleWelcomeCog(bot))
