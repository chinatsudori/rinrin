from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..utils.cleanup import (
    DEFAULT_BOT_AUTHOR_ID,
    DEFAULT_FORUM_ID,
    collect_threads,
    has_purge_permissions,
    purge_messages_from_threads,
    resolve_forum_channel,
)
from .admin import AdminCog

log = logging.getLogger(__name__)

# Placeholder group for decorators
_CLEANUP_GROUP = app_commands.Group(
    name="cleanup",
    description="Mod cleanup utilities",
)


class CleanupCog(commands.Cog):
    """Utility cleanup commands."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._parent_group: app_commands.Group | None = None

        self.group = app_commands.Group(name="cleanup", description="Mod cleanup utilities")
        for cmd in list(_CLEANUP_GROUP.commands):
            self.group.add_command(cmd)

    async def cog_load(self) -> None:
        admin_cog = self.bot.get_cog("AdminCog")
        if isinstance(admin_cog, AdminCog):
            try: admin_cog.group.remove_command(self.group.name)
            except (KeyError, AttributeError): pass
            admin_cog.group.add_command(self.group)
            self._parent_group = admin_cog.group
        else:
            try: self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError): pass
            self.bot.tree.add_command(self.group)
            self._parent_group = None

    async def cog_unload(self) -> None:
        if self._parent_group is not None:
            try: self._parent_group.remove_command(self.group.name)
            except (KeyError, AttributeError): pass
        else:
            try: self.bot.tree.remove_command(self.group.name, type=self.group.type)
            except (KeyError, AttributeError): pass

    # ---------------- Commands ----------------

    @_CLEANUP_GROUP.command(
        name="mupurge",
        description="Purge messages posted by a bot from a Forum and its threads.",
    )
    @app_commands.describe(
        forum_id="Forum channel ID (defaults to 1428158868843921429).",
        bot_author_id="Author ID to purge (defaults to 1266545197077102633).",
        include_private_archived="Also scan private archived threads (requires permissions).",
        dry_run="If true, only report what would be deleted.",
    )
    @app_commands.checks.has_permissions(manage_messages=True)
    async def mupurge(
        self,
        interaction: discord.Interaction,
        forum_id: Optional[int] = None,
        bot_author_id: Optional[int] = None,
        include_private_archived: bool = True,
        dry_run: bool = False,
    ):
        """Crawl forum threads and delete messages authored by the specified bot."""
        await interaction.response.defer(ephemeral=True)

        forum_id = forum_id or DEFAULT_FORUM_ID
        bot_author_id = bot_author_id or DEFAULT_BOT_AUTHOR_ID

        forum = await resolve_forum_channel(self.bot, interaction.guild, forum_id)
        if forum is None:
            return await interaction.followup.send(
                f"Forum channel `{forum_id}` not found or not accessible.",
                ephemeral=True,
            )

        me = forum.guild.me  # type: ignore[assignment]
        if not isinstance(me, discord.Member) or not has_purge_permissions(me, forum):
            return await interaction.followup.send(
                "I need **View Channel**, **Read Message History**, and **Manage Messages** in that forum.",
                ephemeral=True,
            )

        threads = await collect_threads(forum, include_private_archived=include_private_archived)
        (
            scanned_threads,
            scanned_messages,
            matches,
            deleted,
        ) = await purge_messages_from_threads(
            threads,
            author_id=bot_author_id,
            dry_run=dry_run,
        )

        dry_prefix = "DRY RUN - " if dry_run else ""
        summary = (
            f"{dry_prefix}Scanned **{scanned_threads}** threads and **{scanned_messages}** messages "
            f"in forum <#{forum.id}>.\n"
            f"Found **{matches}** messages authored by `<@{bot_author_id}>`."
            f"{'' if dry_run else f' Deleted **{deleted}**.'}"
        )
        await interaction.followup.send(summary, ephemeral=True)

    @mupurge.error
    async def _mupurge_error(
        self,
        interaction: discord.Interaction,
        error: app_commands.AppCommandError,
    ):
        if isinstance(error, app_commands.errors.MissingPermissions):
            await interaction.response.send_message(
                "You need **Manage Messages** to run this.", ephemeral=True
            )
        else:
            raise error


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
