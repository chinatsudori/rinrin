from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..ui.timestamp import build_timestamp_embed
from ..utils.timestamp import coerce_timezone, parse_date, parse_time, to_epoch

log = logging.getLogger(__name__)


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
        await interaction.response.defer(ephemeral=not post)

        parsed_date = parse_date(date)
        parsed_time = parse_time(time)
        if not parsed_date or not parsed_time:
            log.info(
                "tools.timestamp.invalid_input",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date,
                    "time": time,
                    "tz": tz,
                    "post": post,
                },
            )
            return await interaction.followup.send(
                S("tools.timestamp.invalid_dt"), ephemeral=not post
            )

        tzinfo = coerce_timezone(tz)
        hh, mm, ss = parsed_time

        try:
            local_dt = datetime(
                parsed_date.year,
                parsed_date.month,
                parsed_date.day,
                hh,
                mm,
                ss,
                tzinfo=tzinfo,
            )
        except Exception:
            log.exception(
                "tools.timestamp.build_failed",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date,
                    "time": time,
                    "tz": tz,
                },
            )
            return await interaction.followup.send(
                S("tools.timestamp.build_failed"), ephemeral=not post
            )

        try:
            epoch = to_epoch(local_dt)
            embed = build_timestamp_embed(
                epoch=epoch, local_iso=local_dt.isoformat(), tzinfo=tzinfo
            )
            await interaction.followup.send(embed=embed, ephemeral=not post)
        except Exception:
            log.exception(
                "tools.timestamp.unexpected_error",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": getattr(interaction.user, "id", None),
                    "date": date,
                    "time": time,
                    "tz": tz,
                },
            )
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(TimestampCog(bot))
