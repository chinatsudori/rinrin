from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import collections as collection_models
from ..models import guilds
from ..strings import S
from ..ui.collection import build_collection_list_embed
from ..utils.collection import first_url, normalized_club
from ..utils.time import now_local, to_iso

log = logging.getLogger(__name__)


class CollectionCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="start_collection", description="Open a collection window for N days")
    @app_commands.describe(
        days="Number of days the collection is open",
        club="Club type (default: manga)",
    )
    async def start_collection(
        self,
        interaction: discord.Interaction,
        days: app_commands.Range[int, 1, 30],
        club: str = "manga",
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        club = normalized_club(club)

        cfg = guilds.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg_with_hint", club=club), ephemeral=True
            )

        opens = now_local()
        closes = opens + timedelta(days=days)
        collection_id = collection_models.open_collection(
            interaction.guild_id,
            cfg["club_id"],
            to_iso(opens),
            to_iso(closes),
        )

        announcements = interaction.guild.get_channel(cfg["announcements_channel_id"])
        planning_forum = interaction.guild.get_channel(cfg["planning_forum_id"])
        planning_name = planning_forum.name if isinstance(planning_forum, discord.ForumChannel) else "planning"

        if isinstance(announcements, discord.TextChannel):
            await announcements.send(
                S(
                    "collection.announce.open",
                    club=club,
                    closes_unix=int(closes.timestamp()),
                    planning_name=planning_name,
                )
            )

        await interaction.response.send_message(
            S("collection.reply.opened", club=club, id=collection_id), ephemeral=True
        )

    @app_commands.command(name="close_collection", description="Manually close the current collection window")
    @app_commands.describe(club="Club type (default: manga)")
    async def close_collection(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = normalized_club(club)
        cfg = guilds.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg", club=club), ephemeral=True
            )
        collection = collection_models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not collection or collection[3] != "open":
            return await interaction.response.send_message(S("collection.error.no_open"), ephemeral=True)

        collection_models.close_collection_by_id(collection[0])
        await interaction.response.send_message(
            S("collection.reply.closed", club=club, id=collection[0]), ephemeral=True
        )

    @app_commands.command(
        name="list_current_submissions",
        description="List submissions in the current collection (numbered)",
    )
    @app_commands.describe(club="Club type (default: manga)")
    async def list_current_submissions(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = normalized_club(club)
        cfg = guilds.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg", club=club), ephemeral=True
            )

        collection = collection_models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not collection:
            return await interaction.response.send_message(S("collection.error.no_windows"), ephemeral=True)

        submissions = collection_models.list_submissions_for_collection(collection[0])
        if not submissions:
            return await interaction.response.send_message(S("collection.error.no_submissions"), ephemeral=True)

        embed = build_collection_list_embed(
            club=club,
            collection_id=collection[0],
            status=collection[3],
            submissions=[(sid, title, link, author_id, thread_id) for sid, title, link, author_id, thread_id, _ in submissions],
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        if thread.guild is None:
            return
        hit = guilds.get_club_by_planning_forum(thread.guild.id, thread.parent_id)
        if not hit:
            return

        club_id, club_type = hit
        collection = collection_models.latest_collection(thread.guild.id, club_id)
        if not collection or collection[3] != "open":
            return

        starter = None
        try:
            async for message in thread.history(limit=1, oldest_first=True):
                starter = message
                break
        except Exception:
            starter = None

        link = first_url(starter.content) if starter else ""
        title = thread.name or (starter.content[:80] if starter else "Untitled Submission")

        collection_models.add_submission(
            thread.guild.id,
            club_id,
            collection[0],
            thread.owner_id or 0,
            title.strip(),
            link.strip(),
            thread.id,
            to_iso(now_local()),
        )

        try:
            await thread.send(S("collection.thread.registered", club_upper=(club_type or "").upper()))
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(CollectionCog(bot))
