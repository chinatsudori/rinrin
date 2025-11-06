from __future__ import annotations

from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..models import emoji_stats
from ..strings import S
from ..ui.emoji_stats import build_emoji_embed, build_sticker_embed
from ..utils.emoji_stats import (
    CUSTOM_EMOJI_RE,
    export_usage_csv,
    iter_unicode_emojis,
    label_for_unicode,
    month_default,
    now_iso,
)


class EmojiStatsCog(commands.Cog):
    """Monthly emoji & sticker usage analytics."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return

        when_iso = now_iso()

        if message.stickers:
            for st in message.stickers:
                sid = int(st.id)
                sname = st.name or f"sticker:{sid}"
                emoji_stats.bump_sticker_usage(message.guild.id, when_iso, sid, sname, 1)

        content = message.content or ""
        for m in CUSTOM_EMOJI_RE.finditer(content):
            eid = m.group("id")
            name = m.group("name")
            key = f"custom:{eid}"
            emoji_stats.bump_emoji_usage(message.guild.id, when_iso, key, f":{name}:", True, False, 1)
        if content:
            for uni in iter_unicode_emojis(content):
                key = f"uni:{uni}"
                emoji_stats.bump_emoji_usage(
                    message.guild.id,
                    when_iso,
                    key,
                    label_for_unicode(uni),
                    False,
                    False,
                    1,
                )

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not payload.guild_id:
            return
        when_iso = now_iso()
        e = payload.emoji
        if e.is_custom_emoji():
            key = f"custom:{e.id}"
            name = f":{e.name}:"
            emoji_stats.bump_emoji_usage(payload.guild_id, when_iso, key, name, True, True, 1)
        else:
            uni = str(e)
            key = f"uni:{uni}"
            emoji_stats.bump_emoji_usage(
                payload.guild_id,
                when_iso,
                key,
                label_for_unicode(uni),
                False,
                True,
                1,
            )

    grp = app_commands.Group(name="stats", description="Usage statistics")

    @grp.command(name="emoji", description="Show top emoji usage for a month.")
    @app_commands.describe(month="YYYY-MM (default: current)", limit="How many to show (5-30)")
    async def emoji_stats(
        self,
        interaction: discord.Interaction,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 30] = 20,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        month = month or month_default()
        rows = emoji_stats.top_emojis(interaction.guild_id, month, int(limit))
        if not rows:
            return await interaction.response.send_message(
                S("emoji.none_for_month", month=month), ephemeral=True
            )

        embed = build_emoji_embed(month, rows, limit=int(limit))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @grp.command(name="stickers", description="Show top sticker usage for a month.")
    @app_commands.describe(month="YYYY-MM (default: current)", limit="How many to show (5-30)")
    async def sticker_stats(
        self,
        interaction: discord.Interaction,
        month: Optional[str] = None,
        limit: app_commands.Range[int, 5, 30] = 20,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        month = month or month_default()
        rows = emoji_stats.top_stickers(interaction.guild_id, month, int(limit))
        if not rows:
            return await interaction.response.send_message(
                S("sticker.none_for_month", month=month), ephemeral=True
            )

        embed = build_sticker_embed(month, rows, limit=int(limit))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @grp.command(name="export", description="Export emoji & sticker usage for a month as CSV.")
    @app_commands.describe(month="YYYY-MM (default: current)")
    async def export_csv(self, interaction: discord.Interaction, month: Optional[str] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        month = month or month_default()
        e_rows = emoji_stats.top_emojis(interaction.guild_id, month, 1000)
        s_rows = emoji_stats.top_stickers(interaction.guild_id, month, 1000)

        csv_buf = export_usage_csv(month, e_rows, s_rows)
        filename = f"usage-{interaction.guild_id}-{month}.csv"
        file = discord.File(fp=csv_buf, filename=filename)
        await interaction.response.send_message(file=file, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(EmojiStatsCog(bot))
