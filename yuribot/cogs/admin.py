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


class AdminCog(commands.GroupCog, name="admin", description="Admin tools"):
    """
    Top-level /admin group.
    Other cogs (Backread, MaintActivity, Cleanup, etc.) dynamically nest their
    own groups under this one at runtime, e.g. /admin backread …, /admin maint …
    """

    def __init__(self, bot: commands.Bot):
        super().__init__()
        self.bot = bot

    # ------------------------------------------------------------------
    # Core admin commands
    # ------------------------------------------------------------------

    @app_commands.command(name="club_config", description="Show configured club IDs and assets.")
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

    @app_commands.command(name="set_image", description="Upload an image asset for a club.")
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

    @app_commands.command(name="set_link", description="Set an external link for a club.")
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

    # ------------------------------------------------------------------
    # Slash-command sync & diagnostics (fixes signature mismatches fast)
    # ------------------------------------------------------------------

    @app_commands.command(
        name="sync_guild",
        description="Force-sync slash commands to THIS guild (instant).",
    )
    async def sync_guild(self, interaction: discord.Interaction):
        if not interaction.guild:
            await interaction.response.send_message("Run this in a guild.", ephemeral=True)
            return
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            # Clear only this guild's cached paths before pushing the fresh tree
            self.bot.tree.clear_commands(guild=interaction.guild)
            cmds = await self.bot.tree.sync(guild=interaction.guild)
            await interaction.followup.send(
                f"Synced **{len(cmds)}** command(s) for **{interaction.guild.name}**.", ephemeral=True
            )
        except Exception:
            log.exception("admin.sync_guild.failed", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send("Guild sync failed. Check logs.", ephemeral=True)

    @app_commands.command(
        name="sync_global",
        description="Force-sync slash commands globally (propagation may take up to ~1 hour).",
    )
    async def sync_global(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            cmds = await self.bot.tree.sync()
            await interaction.followup.send(
                f"Synced **{len(cmds)}** global command(s). "
                "Note: allow time for Discord to propagate.",
                ephemeral=True,
            )
        except Exception:
            log.exception("admin.sync_global.failed")
            await interaction.followup.send("Global sync failed. Check logs.", ephemeral=True)

    @app_commands.command(
        name="show_tree",
        description="Show the locally-registered slash command paths (debug).",
    )
    async def show_tree(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        lines: list[str] = []
        for cmd in self.bot.tree.get_commands():
            lines.append(f"/{cmd.name}")
            if hasattr(cmd, "commands"):
                for sub in cmd.commands:
                    lines.append(f"/{cmd.name} {sub.name}")
                    if hasattr(sub, "commands"):
                        for sub2 in sub.commands:
                            lines.append(f"/{cmd.name} {sub.name} {sub2.name}")
        preview = "\n".join(lines[:100]) or "(no commands registered locally)"
        await interaction.followup.send(preview, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(AdminCog(bot))
    log.info("Loaded AdminCog")
