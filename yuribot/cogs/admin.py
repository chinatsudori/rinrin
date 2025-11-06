from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import guilds
from ..strings import S
from ..ui.admin import build_club_config_embed
from ..utils.admin import ensure_guild, validate_image_filename

log = logging.getLogger(__name__)


class AdminCog(commands.Cog):
    """Admin commands: per-club configuration and maintenance."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._registered_group: Optional[app_commands.Group] = None

    async def cog_load(self) -> None:
        """Ensure the slash command group is registered with the tree."""
        # discord.py 2.6 removed the public ``Group.copy`` helper that used to be
        # available on app commands.  ``Group._copy_with`` performs the same work
        # (binding the group and all of its children to this cog instance) and is
        # what the library now uses internally.  Use it here so we can register a
        # per-instance copy of the group with the tree.
        group = type(self).group._copy_with(parent=None, binding=self)
        existing = self.bot.tree.get_command(group.name, type=group.type)
        if existing is not None:
            self.bot.tree.remove_command(group.name, type=group.type)

        try:
            self.bot.tree.add_command(group)
        except app_commands.CommandAlreadyRegistered:
            log.warning("admin.group.already_registered", extra={"name": group.name})
            self.bot.tree.remove_command(group.name, type=group.type)
            self.bot.tree.add_command(group)
        self._registered_group = group

    def cog_unload(self) -> None:
        if self._registered_group is None:
            return

        try:
            self.bot.tree.remove_command(
                self._registered_group.name,
                type=self._registered_group.type,
            )
        except Exception:
            log.exception("admin.group.remove_failed")
        finally:
            self._registered_group = None

    group = app_commands.Group(name="admin", description="Admin tools")

    @group.command(name="club_config", description="Show configured club IDs and assets.")
    @app_commands.describe(post="If true, post publicly in this channel")
    async def club_config(self, interaction: discord.Interaction, post: bool = False):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        try:
            cfg = guilds.get_club_map(interaction.guild_id)
        except Exception:
            log.exception("admin.club_config.lookup_failed", extra={"guild_id": interaction.guild_id})
            return await interaction.followup.send(S("admin.club_config.error"), ephemeral=not post)

        pairs = [(club, str(info.get("club_id", "-"))) for club, info in cfg.items()]
        embed = build_club_config_embed(guild=interaction.guild, club_pairs=pairs)
        await interaction.followup.send(embed=embed, ephemeral=not post)

    @group.command(name="set_image", description="Upload an image asset for a club.")
    @app_commands.describe(
        club_slug="Club slug (e.g. movie)",
        image="PNG/JPG file",
        filename="Optional filename (defaults to uploaded name)",
        post="If true, post publicly in this channel",
    )
    async def set_image(
        self,
        interaction: discord.Interaction,
        club_slug: str,
        image: discord.Attachment,
        filename: Optional[str] = None,
        post: bool = False,
    ):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        name = filename or image.filename
        valid_name = validate_image_filename(name)
        if not valid_name:
            return await interaction.followup.send(S("admin.set_image.invalid_name"), ephemeral=not post)

        try:
            data = await image.read()
            guilds.store_club_image(interaction.guild_id, club_slug, valid_name, data)
        except Exception:
            log.exception(
                "admin.set_image.store_failed",
                extra={"guild_id": interaction.guild_id, "club": club_slug},
            )
            return await interaction.followup.send(S("admin.set_image.error"), ephemeral=not post)

        await interaction.followup.send(S("admin.set_image.ok"), ephemeral=not post)

    @group.command(name="set_link", description="Set an external link for a club.")
    @app_commands.describe(
        club_slug="Club slug (e.g. movie)",
        url="URL to store",
        post="If true, post publicly in this channel",
    )
    async def set_link(
        self,
        interaction: discord.Interaction,
        club_slug: str,
        url: str,
        post: bool = False,
    ):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=not post)

        try:
            guilds.store_club_link(interaction.guild_id, club_slug, url)
        except Exception:
            log.exception(
                "admin.set_link.store_failed",
                extra={"guild_id": interaction.guild_id, "club": club_slug},
            )
            return await interaction.followup.send(S("admin.set_link.error"), ephemeral=not post)

        await interaction.followup.send(S("admin.set_link.ok"), ephemeral=not post)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
