from __future__ import annotations
import re
from datetime import timedelta
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..utils.time import now_local, to_iso
from ..strings import S

URL_RE = re.compile(r'(https?://\S+)', re.IGNORECASE)


def _first_url(text: str) -> str:
    m = URL_RE.search(text or "")
    return m.group(1) if m else ""


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

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg_with_hint", club=club), ephemeral=True
            )

        opens = now_local()
        closes = opens + timedelta(days=days)
        cid = models.open_collection(interaction.guild_id, cfg["club_id"], to_iso(opens), to_iso(closes))

        ann = interaction.guild.get_channel(cfg["announcements_channel_id"])
        pf = interaction.guild.get_channel(cfg["planning_forum_id"])
        pfname = pf.name if isinstance(pf, discord.ForumChannel) else "planning"

        if isinstance(ann, discord.TextChannel):
            await ann.send(
                S(
                    "collection.announce.open",
                    club=club,
                    closes_unix=int(closes.timestamp()),
                    planning_name=pfname,
                )
            )

        await interaction.response.send_message(
            S("collection.reply.opened", club=club, id=cid), ephemeral=True
        )

    @app_commands.command(name="close_collection", description="Manually close the current collection window")
    @app_commands.describe(club="Club type (default: manga)")
    async def close_collection(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg", club=club), ephemeral=True
            )
        cw = models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not cw or cw[3] != "open":
            return await interaction.response.send_message(S("collection.error.no_open"), ephemeral=True)

        models.close_collection_by_id(cw[0])
        await interaction.response.send_message(S("collection.reply.closed", club=club, id=cw[0]), ephemeral=True)

    @app_commands.command(
        name="list_current_submissions",
        description="List submissions in the current collection (numbered)",
    )
    @app_commands.describe(club="Club type (default: manga)")
    async def list_current_submissions(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("collection.error.no_cfg", club=club), ephemeral=True
            )

        cw = models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not cw:
            return await interaction.response.send_message(S("collection.error.no_windows"), ephemeral=True)

        subs = models.list_submissions_for_collection(cw[0])
        if not subs:
            return await interaction.response.send_message(S("collection.error.no_submissions"), ephemeral=True)

        title = S("collection.embed.title", club=club, id=cw[0], status=cw[3])
        embed = discord.Embed(title=title, color=discord.Color.pink())

        for i, (sid, title_text, link, author_id, thread_id, created_at) in enumerate(subs, start=1):
            field_name = S("collection.embed.item_name", i=i, title=title_text)
            field_value = S(
                "collection.embed.item_value",
                link=(link or S("collection.common.no_link")),
                author_id=author_id,
                thread_id=thread_id,
            )
            embed.add_field(name=field_name, value=field_value, inline=False)

        await interaction.response.send_message(embed=embed, ephemeral=True)

    @commands.Cog.listener()
    async def on_thread_create(self, thread: discord.Thread):
        # Auto-scrape new planning forum posts during an open collection, per club
        if thread.guild is None:
            return
        hit = models.get_club_by_planning_forum(thread.guild.id, thread.parent_id)
        if not hit:
            return

        club_id, club_type = hit
        cw = models.latest_collection(thread.guild.id, club_id)
        if not cw or cw[3] != "open":
            return

        starter = None
        try:
            async for m in thread.history(limit=1, oldest_first=True):
                starter = m
                break
        except Exception:
            starter = None

        link = _first_url(starter.content) if starter else ""
        title = thread.name or (starter.content[:80] if starter else "Untitled Submission")

        models.add_submission(
            thread.guild.id,
            club_id,
            cw[0],
            thread.owner_id or 0,
            title.strip(),
            link.strip(),
            thread.id,
            to_iso(now_local()),
        )

        try:
            await thread.send(
                S("collection.thread.registered", club_upper=(club_type or "").upper())
            )
        except Exception:
            pass


async def setup(bot: commands.Bot):
    await bot.add_cog(CollectionCog(bot))
