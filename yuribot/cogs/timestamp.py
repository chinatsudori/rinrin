from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from ..config import LOCAL_TZ  # server default tz (tzinfo)
from ..strings import S

log = logging.getLogger(__name__)

def _coerce_tz(tz_str: Optional[str]):
    """Return a tzinfo. Prefer explicit tz_str, else LOCAL_TZ, else UTC."""
    if tz_str:
        if ZoneInfo:
            try:
                return ZoneInfo(tz_str)
            except Exception:
                # fall through to LOCAL_TZ / UTC
                pass
    return LOCAL_TZ if isinstance(LOCAL_TZ, timezone.__class__) or getattr(LOCAL_TZ, "key", None) else timezone.utc

def _tz_display(tzinfo) -> str:
    return getattr(tzinfo, "key", None) or str(tzinfo)

def _parse_date(s: str) -> datetime.date | None:
    try:
        y, m, d = (int(p) for p in s.split("-", 2))
        return datetime(y, m, d).date()
    except Exception:
        return None

def _parse_time(s: str) -> tuple[int, int, int] | None:
    # HH:MM or HH:MM:SS (24h)
    parts = s.split(":")
    try:
        if len(parts) == 2:
            hh, mm = int(parts[0]), int(parts[1])
            return (hh, mm, 0)
        elif len(parts) == 3:
            hh, mm, ss = int(parts[0]), int(parts[1]), int(parts[2])
            return (hh, mm, ss)
    except Exception:
        pass
    return None

class TimestampCog(commands.Cog):
    """Convert a local date/time into Discord timestamp tags."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="timestamp",
        description="Convert a local date/time to Discord timestamp tags you can copy-paste.",
    )
    @app_commands.describe(
        date="YYYY-MM-DD (your local date)",
        time="HH:MM or HH:MM:SS (24h, your local time)",
        tz="Optional IANA timezone like America/New_York (defaults to server timezone)",
        post="If true, post publicly in this channel",
    )
    async def timestamp_cmd(
        self,
        interaction: discord.Interaction,
        date: str,
        time: str,
        tz: Optional[str] = None,
        post: bool = False,
    ):
        # Defer promptly; mirror visibility with `post`
        await interaction.response.defer(ephemeral=not post)

        # Parse inputs
        d = _parse_date(date)
        t = _parse_time(time)
        if not d or not t:
            log.info(
                "tools.timestamp.invalid_input",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date, "time": time, "tz": tz, "post": post,
                },
            )
            return await interaction.followup.send(
                S("tools.timestamp.invalid_dt"), ephemeral=not post
            )

        tzinfo = _coerce_tz(tz)
        hh, mm, ss = t

        try:
            local_dt = datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=tzinfo)
        except Exception:
            log.exception(
                "tools.timestamp.build_failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date, "time": time, "tz": tz,
                },
            )
            return await interaction.followup.send(
                S("tools.timestamp.build_failed"), ephemeral=not post
            )

        try:
            # Convert to epoch (UTC)
            epoch = int(local_dt.astimezone(timezone.utc).timestamp())

            # Build common formats
            tags = {
                S("tools.timestamp.label.relative"):      f"<t:{epoch}:R>",
                S("tools.timestamp.label.full"):          f"<t:{epoch}:F>",
                S("tools.timestamp.label.short_dt"):      f"<t:{epoch}:f>",
                S("tools.timestamp.label.date"):          f"<t:{epoch}:D>",
                S("tools.timestamp.label.date_short"):    f"<t:{epoch}:d>",
                S("tools.timestamp.label.time"):          f"<t:{epoch}:T>",
                S("tools.timestamp.label.time_short"):    f"<t:{epoch}:t>",
            }

            # Preview + copy blocks
            preview_lines = [f"**{k}:** {v}" for k, v in tags.items()]
            copy_lines = [f"{k}: `{v}`" for k, v in tags.items()]

            embed = discord.Embed(
                title=S("tools.timestamp.title"),
                description="\n".join(preview_lines),
                color=discord.Color.blurple(),
            )
            embed.add_field(name=S("tools.timestamp.copy_field"), value="\n".join(copy_lines), inline=False)
            embed.set_footer(text=S("tools.timestamp.footer", local_iso=local_dt.isoformat(), tz=_tz_display(tzinfo)))

            await interaction.followup.send(embed=embed, ephemeral=not post)

            log.info(
                "tools.timestamp.used",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date, "time": time, "tz": tz,
                    "epoch": epoch, "post": post,
                },
            )
        except Exception:
            log.exception(
                "tools.timestamp.unexpected_error",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date, "time": time, "tz": tz,
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(TimestampCog(bot))
