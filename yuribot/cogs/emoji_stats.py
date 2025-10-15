from __future__ import annotations
import re
import csv
from io import StringIO
from typing import Optional, List
from datetime import datetime, timezone

import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..strings import S

_CUSTOM_EMOJI_RE = re.compile(r"<a?:(?P<name>\w+):(?P<id>\d+)>")

def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")

def _month_default() -> str:
    dt = datetime.now(timezone.utc)
    return dt.strftime("%Y-%m")

def _label_for_unicode(emoji_str: str) -> str:
    return emoji_str

def _iter_unicode_emojis(s: str) -> List[str]:
    try:
        import emoji as _emoji_lib  # optional
        return [m["emoji"] for m in _emoji_lib.emoji_list(s)]
    except Exception:
        return [ch for ch in s if ord(ch) >= 0x2190]

class EmojiStatsCog(commands.Cog):
    """Monthly emoji & sticker usage analytics."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if not message.guild or message.author.bot:
            return

        when_iso = _now_iso()

        if message.stickers:
            for st in message.stickers:
                sid = int(st.id)
                sname = st.name or f"sticker:{sid}"
                models.bump_sticker_usage(message.guild.id, when_iso, sid, sname, 1)

        content = message.content or ""
        for m in _CUSTOM_EMOJI_RE.finditer(content):
            eid = m.group("id")
            name = m.group("name")
            key = f"custom:{eid}"
            models.bump_emoji_usage(message.guild.id, when_iso, key, f":{name}:", True, False, 1)

        if content:
            for uni in _iter_unicode_emojis(content):
                key = f"uni:{uni}"
                models.bump_emoji_usage(message.guild.id, when_iso, key, _label_for_unicode(uni), False, False, 1)

    @commands.Cog.listener()
    async def on_raw_reaction_add(self, payload: discord.RawReactionActionEvent):
        if not payload.guild_id:
            return
        when_iso = _now_iso()
        e = payload.emoji
        if e.is_custom_emoji():
            key = f"custom:{e.id}"
            name = f":{e.name}:"
            models.bump_emoji_usage(payload.guild_id, when_iso, key, name, True, True, 1)
        else:
            uni = str(e)
            key = f"uni:{uni}"
            models.bump_emoji_usage(payload.guild_id, when_iso, key, _label_for_unicode(uni), False, True, 1)

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

        month = month or _month_default()
        rows = models.top_emojis(interaction.guild_id, month, int(limit))
        if not rows:
            return await interaction.response.send_message(
                S("emoji.none_for_month", month=month), ephemeral=True
            )

        lines = []
        for (key, name, is_custom, via_reaction, count) in rows:
            src = S("emoji.src.reaction") if via_reaction else S("emoji.src.message")
            display = name if name else (key.split(":", 1)[1])
            lines.append(S("emoji.row", display=display, count=count, src=src))

        embed = discord.Embed(
            title=S("emoji.title", month=month),
            description="\n".join(lines[: int(limit)]),
            color=discord.Color.blurple(),
        )
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

        month = month or _month_default()
        rows = models.top_stickers(interaction.guild_id, month, int(limit))
        if not rows:
            return await interaction.response.send_message(
                S("sticker.none_for_month", month=month), ephemeral=True
            )

        lines = [S("sticker.row", name=(r[1] or r[0]), count=r[2]) for r in rows]
        embed = discord.Embed(
            title=S("sticker.title", month=month),
            description="\n".join(lines[: int(limit)]),
            color=discord.Color.green(),
        )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @grp.command(name="export", description="Export emoji & sticker usage for a month as CSV.")
    @app_commands.describe(month="YYYY-MM (default: current)")
    async def export_csv(self, interaction: discord.Interaction, month: Optional[str] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        month = month or _month_default()
        e_rows = models.top_emojis(interaction.guild_id, month, 1000)
        s_rows = models.top_stickers(interaction.guild_id, month, 1000)

        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(["type", "key_or_id", "name", "is_custom", "via_reaction", "count", "month"])
        for (key, name, is_custom, via_reaction, count) in e_rows:
            w.writerow(["emoji", key, name, is_custom, via_reaction, count, month])
        for (sid, sname, count) in s_rows:
            w.writerow(["sticker", sid, sname, "", "", count, month])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(fp=discord.BytesIO(data), filename=f"usage-{interaction.guild_id}-{month}.csv")
        await interaction.response.send_message(file=file, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(EmojiStatsCog(bot))
