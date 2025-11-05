from __future__ import annotations

from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

try:
    from .. import models
except Exception:  # pragma: no cover
    models = None

from ..strings import S
from ..ui.movie import build_description, build_embed
from ..utils.movie import (
    attachment_to_image_bytes,
    infer_entity_type,
    local_dt,
    next_saturday,
    parse_date_yyyy_mm_dd,
    to_utc,
)


class MovieClubCog(commands.Cog):
    """Schedule paired movie events for the club."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="movie_schedule",
        description="Schedule a movie for Saturday at 05:00 and 17:00 (local).",
    )
    @app_commands.describe(
        title="Movie title (shown on events)",
        link="Optional link (trailer/IMDb/etc.) appended to description",
        date="YYYY-MM-DD for the Saturday to schedule (defaults to next Saturday)",
        venue="Voice/Stage channel for an in-server event; leave empty to use External + location",
        location="External location string (used if no channel is provided)",
        duration_min="Event duration in minutes (default 120)",
        image="Attach a poster (PNG/JPG) to set as the event image",
    )
    async def movie_schedule(
        self,
        interaction: discord.Interaction,
        title: str,
        link: Optional[str] = None,
        date: Optional[str] = None,
        venue: Optional[discord.abc.GuildChannel] = None,
        location: Optional[str] = None,
        duration_min: app_commands.Range[int, 30, 600] = 120,
        image: Optional[discord.Attachment] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        perms = interaction.user.guild_permissions
        if not (perms.manage_guild or perms.manage_events):
            return await interaction.response.send_message(S("movie.error.perms"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        target_date = parse_date_yyyy_mm_dd(date) or next_saturday()
        start_local_am = local_dt(target_date.year, target_date.month, target_date.day, 5, 0)
        start_local_pm = local_dt(target_date.year, target_date.month, target_date.day, 17, 0)
        end_local_am = start_local_am + timedelta(minutes=int(duration_min))
        end_local_pm = start_local_pm + timedelta(minutes=int(duration_min))

        start_utc_am = to_utc(start_local_am)
        end_utc_am = to_utc(end_local_am)
        start_utc_pm = to_utc(start_local_pm)
        end_utc_pm = to_utc(end_local_pm)

        default_location = location or S("movie.location.default")
        entity_type, entity_kwargs = infer_entity_type(venue, default_location=default_location)

        description = build_description(title, link)
        image_bytes = await attachment_to_image_bytes(image)

        guild: discord.Guild = interaction.guild
        try:
            event_am = await guild.create_scheduled_event(
                name=S("movie.event.name_morning", title=title),
                start_time=start_utc_am,
                end_time=end_utc_am,
                entity_type=entity_type,
                privacy_level=discord.PrivacyLevel.guild_only,
                description=description,
                image=image_bytes,
                **entity_kwargs,
            )

            event_pm = await guild.create_scheduled_event(
                name=S("movie.event.name_evening", title=title),
                start_time=start_utc_pm,
                end_time=end_utc_pm,
                entity_type=entity_type,
                privacy_level=discord.PrivacyLevel.guild_only,
                description=description,
                image=image_bytes,
                **entity_kwargs,
            )
        except discord.Forbidden:
            return await interaction.followup.send(S("movie.error.forbidden"), ephemeral=True)
        except discord.HTTPException as exc:
            return await interaction.followup.send(S("movie.error.http", error=str(exc)), ephemeral=True)

        if models and hasattr(models, "create_movie_events"):
            try:
                show_date_iso = target_date.isoformat()
                club_id = 0
                if hasattr(models, "get_club_cfg"):
                    try:
                        club_id = models.get_club_cfg(guild.id, "movie")["club_id"]  # type: ignore[index]
                    except Exception:
                        club_id = 0
                models.create_movie_events(
                    guild_id=guild.id,
                    club_id=club_id,
                    title=title,
                    link=link or "",
                    show_date_iso=show_date_iso,
                    event_id_morning=event_am.id,
                    event_id_evening=event_pm.id,
                )
            except Exception:
                pass

        embed = build_embed(
            title=title,
            start_am=int(start_utc_am.timestamp()),
            start_pm=int(start_utc_pm.timestamp()),
            venue=venue,
            location=entity_kwargs.get("location"),
            duration_minutes=int(duration_min),
            event_am=event_am,
            event_pm=event_pm,
            link=link,
        )
        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MovieClubCog(bot))
