from __future__ import annotations

from datetime import datetime, date as date_cls, timedelta, timezone
from typing import Optional, Tuple

import discord
from discord.ext import commands
from discord import app_commands

try:
    from .. import models  
except Exception:  # pragma: no cover
    models = None 

from ..config import LOCAL_TZ
from ..strings import S


def _to_local(dt_utc: datetime) -> datetime:
    """Ensure tz-aware in LOCAL_TZ."""
    return dt_utc.astimezone(LOCAL_TZ)

def _local_dt(y: int, m: int, d: int, hh: int, mm: int) -> datetime:
    return datetime(y, m, d, hh, mm, tzinfo=LOCAL_TZ)

def _to_utc(dt_local: datetime) -> datetime:
    """Return UTC tz-aware for Discord scheduled events."""
    return dt_local.astimezone(timezone.utc)

def _next_saturday(base: datetime | None = None) -> date_cls:
    base = base or datetime.now(LOCAL_TZ)
    # Monday=0 ... Sunday=6; Saturday=5
    offset = (5 - base.weekday()) % 7
    return (base + timedelta(days=offset)).date()

def _parse_date_yyyy_mm_dd(s: Optional[str]) -> Optional[date_cls]:
    if not s:
        return None
    try:
        y, m, d = (int(part) for part in s.split("-", 2))
        return date_cls(y, m, d)
    except Exception:
        return None

def _infer_entity_type_and_kwargs(
    venue: discord.abc.GuildChannel | None,
    location: str | None
) -> Tuple[discord.EntityType, dict]:
    if isinstance(venue, discord.StageChannel):
        return discord.EntityType.stage_instance, {"channel": venue}
    if isinstance(venue, discord.VoiceChannel):
        return discord.EntityType.voice, {"channel": venue}
    loc = (location or S("movie.location.default")).strip()
    return discord.EntityType.external, {"location": loc[:100]}

async def _attachment_to_image_bytes(att: Optional[discord.Attachment]) -> Optional[bytes]:
    if not att:
        return None
    if not (att.content_type or "").startswith("image/"):
        return None
    try:
        return await att.read()
    except Exception:
        return None


class MovieClubCog(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="movie_schedule",
        description="Schedule a movie for Saturday at 05:00 and 17:00 (local)."
    )
    @app_commands.describe(
        title="Movie title (shown on events)",
        link="Optional link (trailer/IMDb/etc.) appended to description",
        date="YYYY-MM-DD for the Saturday to schedule (defaults to next Saturday)",
        venue="Voice/Stage channel for an in-server event; leave empty to use External + location",
        location="External location string (used if no channel is provided)",
        duration_min="Event duration in minutes (default 120)",
        image="Attach a poster (PNG/JPG) to set as the event image"
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
            return await interaction.response.send_message(
                S("movie.error.perms"),
                ephemeral=True
            )

        await interaction.response.defer(ephemeral=True, thinking=True)

        # Determine the Saturday
        target_date = _parse_date_yyyy_mm_dd(date) or _next_saturday()
        start_local_am = _local_dt(target_date.year, target_date.month, target_date.day, 5, 0)
        start_local_pm = _local_dt(target_date.year, target_date.month, target_date.day, 17, 0)
        end_local_am = start_local_am + timedelta(minutes=int(duration_min))
        end_local_pm = start_local_pm + timedelta(minutes=int(duration_min))

        start_utc_am = _to_utc(start_local_am)
        end_utc_am = _to_utc(end_local_am)
        start_utc_pm = _to_utc(start_local_pm)
        end_utc_pm = _to_utc(end_local_pm)

        entity_type, entity_kwargs = _infer_entity_type_and_kwargs(venue, location)

        desc_lines = [S("movie.desc.header", title=title)]
        if link:
            desc_lines.append(S("movie.desc.link", link=link))
        desc = "\n".join(desc_lines)[:1000]

        image_bytes = await _attachment_to_image_bytes(image)

        guild: discord.Guild = interaction.guild
        try:
            event_am = await guild.create_scheduled_event(
                name=S("movie.event.name_morning", title=title),
                start_time=start_utc_am,
                end_time=end_utc_am,
                entity_type=entity_type,
                privacy_level=discord.PrivacyLevel.guild_only,
                description=desc,
                image=image_bytes,
                **entity_kwargs,
            )

            event_pm = await guild.create_scheduled_event(
                name=S("movie.event.name_evening", title=title),
                start_time=start_utc_pm,
                end_time=end_utc_pm,
                entity_type=entity_type,
                privacy_level=discord.PrivacyLevel.guild_only,
                description=desc,
                image=image_bytes,
                **entity_kwargs,
            )
        except discord.Forbidden:
            return await interaction.followup.send(
                S("movie.error.forbidden"),
                ephemeral=True
            )
        except discord.HTTPException as e:
            return await interaction.followup.send(
                S("movie.error.http", error=str(e)),
                ephemeral=True
            )

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

        embed = discord.Embed(
            title=S("movie.scheduled.title"),
            description=S(
                "movie.scheduled.desc",
                title=title,
                am=int(start_utc_am.timestamp()),
                pm=int(start_utc_pm.timestamp()),
            ),
            color=discord.Color.green()
        )
        if venue:
            embed.add_field(name=S("movie.field.venue"), value=f"{venue.mention}", inline=True)
        else:
            embed.add_field(name=S("movie.field.location"), value=entity_kwargs.get("location", "External"), inline=True)
        embed.add_field(name=S("movie.field.duration"), value=S("movie.value.duration_min", minutes=int(duration_min)), inline=True)
        if link:
            embed.add_field(name=S("movie.field.link"), value=link[:256], inline=False)
        embed.add_field(
            name=S("movie.field.events"),
            value=S("movie.value.events_links", am_url=event_am.url, pm_url=event_pm.url),
            inline=False
        )

        await interaction.followup.send(embed=embed, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MovieClubCog(bot))
