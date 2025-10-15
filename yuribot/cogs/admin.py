from __future__ import annotations
import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S


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
    )
    async def set_welcome(
        self,
        interaction: discord.Interaction,
        channel: discord.TextChannel,
        image_filename: str = "welcome.png",
    ):
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        models.set_welcome_settings(interaction.guild_id, channel.id, image_filename.strip())
        await interaction.response.send_message(
            S("admin.welcome.set_ok", channel=channel.mention, filename=image_filename.strip()),
            ephemeral=True,
        )

    @app_commands.command(name="set_bot_logs", description="Set the channel where audit/bot logs are posted")
    @app_commands.describe(channel="Target text channel for bot logs")
    async def set_bot_logs(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        models.set_bot_logs_channel(interaction.guild_id, channel.id)
        await interaction.response.send_message(
            S("admin.botlogs.set_ok", channel=channel.mention),
            ephemeral=True,
        )

    @app_commands.command(name="set_mod_logs", description="Set the channel where moderation logs are posted")
    @app_commands.describe(channel="Target text channel")
    async def set_mod_logs(self, interaction: discord.Interaction, channel: discord.TextChannel):
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        models.set_mod_logs_channel(interaction.guild_id, channel.id)
        await interaction.response.send_message(
            S("admin.modlogs.set_ok", channel=channel.mention),
            ephemeral=True,
        )

    @app_commands.command(name="setup", description="Configure a club's channels/forums")
    @app_commands.describe(
        club="Club type name (e.g., manga, ln, vn, or any string; default: manga)",
        announcements_channel="Text channel for announcements",
        planning_forum="Forum channel for submissions/intake",
        polls_channel="Text channel for polls",
        discussion_forum="Forum channel for weekly chapter discussions",
    )
    async def setup(
        self,
        interaction: discord.Interaction,
        club: str,
        announcements_channel: discord.TextChannel,
        planning_forum: discord.ForumChannel,
        polls_channel: discord.TextChannel,
        discussion_forum: discord.ForumChannel,
    ):
        """Create/update the config row for a given club type in this guild."""
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)

        club = (club or "").strip() or "manga"

        club_id = models.upsert_club_cfg(
            guild_id=interaction.guild_id,
            club_type=club,
            ann=announcements_channel.id,
            planning=planning_forum.id,
            polls=polls_channel.id,
            discussion=discussion_forum.id,
        )

        await interaction.response.send_message(
            S(
                "admin.setup.configured",
                club=club,
                id=club_id,
                ann=announcements_channel.mention,
                planning=planning_forum.mention,
                polls=polls_channel.mention,
                discussion=discussion_forum.mention,
            ),
            ephemeral=True,
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
