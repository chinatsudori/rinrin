# yuribot/cogs/rpg.py
from __future__ import annotations

import logging
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import rpg as rpg_model
from ..db import connect

log = logging.getLogger(__name__)


class RPGCog(commands.GroupCog, name="rpg", description="RPG: levels, stats, rebuilds"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    # /rpg progress
    @app_commands.command(name="progress", description="Show a member's RPG level, XP, and stats.")
    @app_commands.describe(user="Member (defaults to you).")
    async def progress(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild_id:
            return await interaction.followup.send("Run this in a server.", ephemeral=True)

        target = user or interaction.user
        data = rpg_model.get_rpg_progress(interaction.guild_id, target.id)
        lvl, cur, nxt = rpg_model.xp_progress(data["xp"])

        lines = [
            f"**{target.mention}**",
            f"Level **{data['level']}** (calc: {lvl}) · XP **{data['xp']:,}** · **{cur:,}/{nxt:,}** to next",
            f"STR **{data['str']}**  INT **{data['int']}**  DEX **{data['dex']}**",
            f"WIS **{data['wis']}**  CHA **{data['cha']}**  VIT **{data['vit']}**",
        ]
        if data.get("last_level_up"):
            lines.append(f"Last level-up: `{data['last_level_up']}`")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    # /rpg top
    @app_commands.command(name="top", description="Show top RPG levels in this server.")
    @app_commands.describe(limit="How many to list (default 20)")
    async def top(
        self,
        interaction: discord.Interaction,
        limit: int = 20,
    ):
        await interaction.response.defer(ephemeral=True)
        if not interaction.guild_id:
            return await interaction.followup.send("Run this in a server.", ephemeral=True)

        limit = max(1, min(int(limit), 50))
        rows = rpg_model.top_levels(interaction.guild_id, limit)
        if not rows:
            return await interaction.followup.send("No RPG data yet.", ephemeral=True)

        out = ["**Top RPG**"]
        for rank, (uid, level, xp) in enumerate(rows, 1):
            member = interaction.guild.get_member(uid) if interaction.guild else None  # type: ignore[arg-type]
            name = member.mention if member else f"<@{uid}>"
            out.append(f"{rank:>2}. {name} — L{int(level)} · {int(xp):,} XP")
        await interaction.followup.send("\n".join(out), ephemeral=True)

    # /rpg respec_snapshot
    @app_commands.command(
        name="respec_snapshot",
        description="Re-allocate stats NOW from the current 7-day activity ranking (keeps XP/level).",
    )
    @app_commands.describe(user="Limit to one member (optional)")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def respec_snapshot(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        if not interaction.guild_id:
            return await interaction.followup.send("Run this in a server.", ephemeral=True)
        count = rpg_model.respec_stats_to_formula(interaction.guild_id, user.id if user else None)
        who = user.mention if user else "all members"
        await interaction.followup.send(f"Respecced **{count}** member(s) ({who}).", ephemeral=True)

    # /rpg rebuild_progress
    @app_commands.command(
        name="rebuild_progress",
        description="Rebuild XP/levels per message chronologically (rolling 7-day stat allocation).",
    )
    @app_commands.describe(
        user="Limit to one member (optional)",
        since_day="YYYY-MM-DD (inclusive), optional",
        until_day="YYYY-MM-DD (inclusive), optional",
        reset="If true, wipes RPG progress rows for the target(s) first (default: true)",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def rebuild_progress(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        since_day: Optional[str] = None,
        until_day: Optional[str] = None,
        reset: bool = True,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)
        if not interaction.guild_id:
            return await interaction.followup.send("Run this in a server.", ephemeral=True)

        processed = rpg_model.rebuild_progress_chronological(
            interaction.guild_id,
            user_id=user.id if user else None,
            since_day=since_day,
            until_day=until_day,
            reset=bool(reset),
        )
        who = user.mention if user else "all members"
        extras = []
        if since_day:
            extras.append(f"since `{since_day}`")
        if until_day:
            extras.append(f"until `{until_day}`")
        suffix = f" ({', '.join(extras)})" if extras else ""
        await interaction.followup.send(
            f"Rebuilt RPG progress for **{processed}** member(s): {who}{suffix}.", ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(RPGCog(bot))
    log.info("Loaded RPGCog")
