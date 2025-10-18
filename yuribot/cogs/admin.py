from __future__ import annotations
import logging
import os
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg"}

async def _require_guild(inter: discord.Interaction) -> bool:
    if not inter.guild:
        if not inter.response.is_done():
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
        else:
            await inter.followup.send(S("common.guild_only"), ephemeral=True)
        return False
    return True

def _validate_image_filename(name: str) -> Optional[str]:
    fn = (name or "").strip()
    _, ext = os.path.splitext(fn.lower())
    if ext not in ALLOWED_IMAGE_EXTS:
        return None
    if "/" in fn or "\\" in fn:
        return None
    return fn

class AdminCog(commands.Cog):
    """Admin commands: per-club configuration."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot


    @app_commands.command(
        name="set_welcome",
        description="Set welcome channel and image filename (PNG/JPG in bot folder)",
    )
    @app_commands.describe(
        channel="Target text channel",
        image_filename="Filename relative to the bot folder (e.g., welcome.png)",
        post="If true, post publicly in this channel",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def set_welcome(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        image_filename: str = "welcome.png",
        post: bool = False,
    ):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post)
        try:
            validated = _validate_image_filename(image_filename)
            if not validated:
                return await interaction.followup.send(S("admin.welcome.bad_filename"), ephemeral=not post)

            models.set_welcome_settings(interaction.guild_id, channel.id, validated)
            await interaction.followup.send(
                S("admin.welcome.set_ok", channel=channel.mention, filename=validated),
                ephemeral=not post,
            )

            log.info(
                "admin.set_welcome.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "channel_id": channel.id,
                    "filename": validated,
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "admin.set_welcome.failed",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "channel_id": channel.id,
                    "filename": image_filename,
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="set_bot_logs", description="Set the channel where audit/bot logs are posted")
    @app_commands.describe(
        channel="Target text channel for bot logs",
        post="If true, post publicly in this channel",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def set_bot_logs(self, interaction: discord.Interaction, channel: discord.TextChannel, post: bool = False):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post)
        try:
            models.set_bot_logs_channel(interaction.guild_id, channel.id)
            await interaction.followup.send(
                S("admin.botlogs.set_ok", channel=channel.mention),
                ephemeral=not post,
            )
            log.info(
                "admin.set_bot_logs.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "channel_id": channel.id,
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "admin.set_bot_logs.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "channel_id": channel.id},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="set_mod_logs", description="Set the channel where moderation logs are posted")
    @app_commands.describe(
        channel="Target text channel",
        post="If true, post publicly in this channel",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def set_mod_logs(self, interaction: discord.Interaction, channel: discord.TextChannel, post: bool = False):
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post)
        try:
            models.set_mod_logs_channel(interaction.guild_id, channel.id)
            await interaction.followup.send(
                S("admin.modlogs.set_ok", channel=channel.mention),
                ephemeral=not post,
            )
            log.info(
                "admin.set_mod_logs.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "channel_id": channel.id,
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "admin.set_mod_logs.failed",
                extra={"guild_id": interaction.guild_id, "user_id": interaction.user.id, "channel_id": channel.id},
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @app_commands.command(name="setup", description="Configure a club's channels/forums")
    @app_commands.describe(
        club="Club type name (e.g., manga, ln, vn, or any string; default: manga)",
        announcements_channel="Text channel for announcements",
        planning_forum="Forum channel for submissions/intake",
        polls_channel="Text channel for polls",
        discussion_forum="Forum channel for weekly chapter discussions",
        post="If true, post publicly in this channel",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def setup(
        self,
        interaction: discord.Interaction,
        club: str,
        announcements_channel: discord.TextChannel,
        planning_forum: discord.ForumChannel,
        polls_channel: discord.TextChannel,
        discussion_forum: discord.ForumChannel,
        post: bool = False,
    ):
        """Create/update the config row for a given club type in this guild."""
        if not await _require_guild(interaction):
            return
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        await interaction.response.defer(ephemeral=not post)
        try:
            club_norm = (club or "").strip() or "manga"
            club_id = models.upsert_club_cfg(
                guild_id=interaction.guild_id,
                club_type=club_norm,
                ann=announcements_channel.id,
                planning=planning_forum.id,
                polls=polls_channel.id,
                discussion=discussion_forum.id,
            )

            await interaction.followup.send(
                S(
                    "admin.setup.configured",
                    club=club_norm,
                    id=club_id,
                    ann=announcements_channel.mention,
                    planning=planning_forum.mention,
                    polls=polls_channel.mention,
                    discussion=discussion_forum.mention,
                ),
                ephemeral=not post,
            )

            log.info(
                "admin.setup.used",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "club": club_norm,
                    "announcements_channel": announcements_channel.id,
                    "planning_forum": planning_forum.id,
                    "polls_channel": polls_channel.id,
                    "discussion_forum": discussion_forum.id,
                    "post": post,
                },
            )
        except Exception:
            log.exception(
                "admin.setup.failed",
                extra={
                    "guild_id": interaction.guild_id,
                    "user_id": interaction.user.id,
                    "club": club,
                    "announcements_channel": getattr(announcements_channel, "id", None),
                    "planning_forum": getattr(planning_forum, "id", None),
                    "polls_channel": getattr(polls_channel, "id", None),
                    "discussion_forum": getattr(discussion_forum, "id", None),
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
