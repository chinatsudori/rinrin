from __future__ import annotations
from pathlib import Path
import logging

import discord
from discord.ext import commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)


def _ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"


def _pkg_root() -> Path:
    # /app/yuribot
    return Path(__file__).resolve().parents[1]


def _app_root() -> Path:
    # /app
    return Path(__file__).resolve().parents[2]


def _resolve_welcome_image(filename: str) -> Path | None:
    """Try a few common locations; return the first existing path."""
    candidates = [
        _app_root() / filename,                 # /app/welcome.png  <-- your new location
        _app_root() / "assets" / filename,      # /app/assets/welcome.png
        _pkg_root() / filename,                 # /app/yuribot/welcome.png
        _pkg_root() / "assets" / filename,      # /app/yuribot/assets/welcome.png
        Path.cwd() / filename,                  # working dir fallback
        Path.cwd() / "assets" / filename,
    ]
    for p in candidates:
        if p.exists():
            log.debug("welcome: using image at %s", p)
            return p
    log.warning("welcome: image '%s' not found. Tried: %s", filename, ", ".join(str(p) for p in candidates))
    return None


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

        # Count humans if cache is available; fall back to guild member_count
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
        path = _resolve_welcome_image(filename)

        file = None
        if path:
            try:
                file = discord.File(str(path), filename=path.name)
                embed.set_image(url=f"attachment://{path.name}")
            except Exception as e:
                log.warning("welcome: failed to attach image %s: %r", path, e)

        content = S("welcome.content", mention=member.mention)
        try:
            if file:
                await ch.send(content=content, embed=embed, file=file, allowed_mentions=discord.AllowedMentions(users=True))
            else:
                await ch.send(content=content, embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
        except discord.Forbidden:
            log.error("welcome: forbidden posting in #%s (%s). Check permissions (Send Messages, Attach Files).",
                      getattr(ch, "name", "unknown"), ch.id)
        except Exception as e:
            log.exception("welcome: failed to send welcome message: %r", e)


async def setup(bot: commands.Bot):
    await bot.add_cog(WelcomeCog(bot))
