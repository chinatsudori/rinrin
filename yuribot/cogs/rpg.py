# yuribot/cogs/rpg.py
from __future__ import annotations
import logging
from typing import Optional
import discord
from discord import app_commands
from discord.ext import commands

from ..utils.admin import ensure_guild
from ..db import connect
from ..models import rpg as rpg_model

log = logging.getLogger(__name__)

class RPGCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="rpg_rebuild_progress",
        description="Rebuild RPG XP/levels/stats chronologically with 7-day rolling windows at each level up."
    )
    @app_commands.describe(
        member="Only rebuild a single member (optional).",
        since_day="Start at this day (YYYY-MM-DD). Leave blank to auto-detect first activity.",
        until_day="Stop at this day (YYYY-MM-DD), inclusive.",
        reset="If true, clear existing progress before rebuilding (default: true)."
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def rpg_rebuild_progress(
        self,
        interaction: discord.Interaction,
        member: Optional[discord.Member] = None,
        since_day: Optional[str] = None,
        until_day: Optional[str] = None,
        reset: bool = True,
    ):
        if not await ensure_guild(interaction):
            return
        await interaction.response.defer(ephemeral=True, thinking=True)

        gid = interaction.guild_id
        uid = member.id if member else None

        # Auto-detect since_day if not provided
        if since_day is None:
            with connect() as con:
                cur = con.cursor()
                row = cur.execute("""
                    SELECT MIN(day)
                      FROM (
                        SELECT MIN(day) AS day FROM member_metrics_daily WHERE guild_id=? {flt}
                        UNION
                        SELECT MIN(day) AS day FROM member_messages_day WHERE guild_id=? {flt2}
                      ) t
                """.replace("{flt}",  "AND user_id=?" if uid else "")
                  .replace("{flt2}","AND user_id=?" if uid else ""),
                  (gid,) + ((uid,) if uid else ()) + (gid,) + ((uid,) if uid else ())
                ).fetchone()
                since_day = row[0] if row and row[0] else None

        count = rpg_model.rebuild_progress_chronological(
            guild_id=gid,
            user_id=uid,
            since_day=since_day,
            until_day=until_day,
            reset=reset,
        )

        await interaction.followup.send(
            f"Rebuilt RPG progress for **{count}** member(s). "
            f"{'(reset applied)' if reset else '(no reset)'} "
            f"{f'from {since_day} ' if since_day else ''}{f'to {until_day}' if until_day else ''}".strip(),
            ephemeral=True,
        )

    @app_commands.command(
        name="rpg_debug_feed",
        description="Preview the daily message/word rows the rebuild will read (first 10)."
    )
    @app_commands.describe(user="Member (defaults to you).", since_day="YYYY-MM-DD", until_day="YYYY-MM-DD")
    @app_commands.checks.has_permissions(manage_guild=True)
    async def rpg_debug_feed(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        since_day: Optional[str] = None,
        until_day: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True)
        uid = (user or interaction.user).id
        gid = interaction.guild_id

        total_msgs = 0
        total_words = 0
        days = 0
        preview = []
        with connect() as con:
            for day_iso, msgs, words in rpg_model._iter_daily_msgs_words(con, gid, uid, since_day, until_day):
                days += 1
                total_msgs += int(msgs or 0)
                total_words += int(words or 0)
                if len(preview) < 10:
                    preview.append(f"{day_iso}: msgs {int(msgs or 0):,}, words {int(words or 0):,}")

        if days == 0:
            return await interaction.followup.send("No daily rows found.", ephemeral=True)

        lines = [
            f"Feed for <@{uid}> — days: **{days}**",
            f"Messages: **{total_msgs:,}** · Words: **{total_words:,}**",
            "First rows:",
            *preview,
        ]
        await interaction.followup.send("\n".join(lines), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(RPGCog(bot))
    log.info("Loaded RPGCog")
