from __future__ import annotations
from pathlib import Path
import discord
from discord.ext import commands

from .. import models
from ..strings import S


def _ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


class WelcomeCog(commands.Cog):
    """Welcome messages for new members."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        cfg = models.get_welcome_settings(member.guild.id)
        if not cfg:
            return  # not configured

        ch = member.guild.get_channel(cfg["welcome_channel_id"])
        if not isinstance(ch, discord.TextChannel):
            return

        try:
            human_count = sum(1 for m in member.guild.members if not m.bot)
        except Exception:
            human_count = None

        number = human_count if (human_count and human_count > 0) else (member.guild.member_count or 0)
        number = max(int(number), 1)
        ordinal = _ordinal(number)

        # Build embed
        title = S("welcome.title")
        desc = S("welcome.desc", mention=member.mention, ordinal=ordinal)
        embed = discord.Embed(title=title, description=desc, color=discord.Color.green())
        embed.timestamp = discord.utils.utcnow()

        filename = (cfg.get("welcome_image_filename") or "welcome.png").strip()
        root = _project_root()
        path = (root / filename).resolve()
        if not path.exists():
            alt = (root / "assets" / filename).resolve()
            if alt.exists():
                path = alt

        file = None
        if path.exists():
            file = discord.File(str(path), filename=path.name)
            embed.set_image(url=f"attachment://{path.name}")

        content = S("welcome.content", mention=member.mention)
        if file:
            await ch.send(content=content, embed=embed, file=file)
        else:
            await ch.send(content=content, embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(WelcomeCog(bot))
