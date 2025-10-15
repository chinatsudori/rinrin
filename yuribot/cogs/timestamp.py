# yuribot/cogs/timestamp.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import discord
from discord.ext import commands
from discord import app_commands

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:  # pragma: no cover
    ZoneInfo = None  # type: ignore

from ..config import LOCAL_TZ  # your server's default tz (ZoneInfo)
# If LOCAL_TZ is a tzinfo, we can use it directly; else fallback to system UTC.

def _coerce_tz(tz_str: Optional[str]):
    """Return a tzinfo. Prefer explicit tz_str, else LOCAL_TZ, else UTC."""
    if tz_str:
        if ZoneInfo:
            try:
                return ZoneInfo(tz_str)
            except Exception:
                pass
        # Fallback: ignore bad tz; use LOCAL_TZ/UTC
    return getattr(LOCAL_TZ, "key", None) and LOCAL_TZ or timezone.utc

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
    )
    async def timestamp_cmd(
        self,
        interaction: discord.Interaction,
        date: str,
        time: str,
        tz: Optional[str] = None,
    ):
        # Parse inputs
        d = _parse_date(date)
        t = _parse_time(time)
        if not d or not t:
            return await interaction.response.send_message(
                "Invalid date/time. Use `YYYY-MM-DD` and `HH:MM` (or `HH:MM:SS`).",
                ephemeral=True,
            )

        tzinfo = _coerce_tz(tz)
        hh, mm, ss = t
        try:
            local_dt = datetime(d.year, d.month, d.day, hh, mm, ss, tzinfo=tzinfo)
        except Exception:
            return await interaction.response.send_message(
                "Could not build that date/time. Double-check values.",
                ephemeral=True,
            )

        # Convert to epoch
        epoch = int(local_dt.astimezone(timezone.utc).timestamp())

        # Build all common formats
        tags = {
            "Relative": f"<t:{epoch}:R>",
            "Full":     f"<t:{epoch}:F>",
            "Short DT": f"<t:{epoch}:f>",
            "Date":     f"<t:{epoch}:D>",
            "Date (short)": f"<t:{epoch}:d>",
            "Time":     f"<t:{epoch}:T>",
            "Time (short)": f"<t:{epoch}:t>",
        }

        # Pretty preview + copy block
        preview_lines = [f"**{k}:** {v}" for k, v in tags.items()]
        copy_lines = [f"{k}: `{v}`" for k, v in tags.items()]

        embed = discord.Embed(
            title="🕰 Timestamp Builder",
            description="\n".join(preview_lines),
            color=discord.Color.blurple(),
        )
        embed.add_field(name="Copy-paste", value="\n".join(copy_lines), inline=False)
        embed.set_footer(text=f"Local input: {local_dt.isoformat()}  •  TZ: {getattr(tzinfo, 'key', str(tzinfo))}")

        await interaction.response.send_message(embed=embed, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(TimestampCog(bot))

