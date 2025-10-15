from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timedelta, timezone
import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..config import LOCAL_TZ
from ..utils.time import to_iso
from ..strings import S

log = logging.getLogger(__name__)

def next_friday_at(hour: int) -> datetime:
    now = datetime.now(tz=LOCAL_TZ)
    # Monday=0..Sunday=6 ; Friday=4
    days = (4 - now.weekday()) % 7
    if days == 0:  # if today is Friday, schedule next Friday
        days = 7
    return now.replace(hour=hour, minute=0, second=0, microsecond=0) + timedelta(days=days)

def _to_utc(dt_local: datetime) -> datetime:
    return dt_local.astimezone(timezone.utc)

class SeriesCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(name="set_series_from_number", description="Set active series from a submission number")
    @app_commands.describe(
        number="Number from /club list_current_submissions",
        club="Club type (default: manga)",
    )
    async def set_series_from_number(
        self,
        interaction: discord.Interaction,
        number: int,
        club: str = "manga",
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("series.error.no_cfg", club=club), ephemeral=True
            )
        cw = models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not cw:
            return await interaction.response.send_message(S("series.error.no_collection"), ephemeral=True)
        sub = models.get_submission(cw[0], number)
        if not sub:
            return await interaction.response.send_message(S("series.error.bad_number"), ephemeral=True)

        sid, title, link, author_id, thread_id, created_at = sub
        series_id = models.create_series(interaction.guild_id, cfg["club_id"], title, link or "", sid)
        await interaction.response.send_message(
            S("series.set_from_number.ok", club=club, title=title, id=series_id), ephemeral=True
        )

    @app_commands.command(name="set_series", description="Create and set active series (manual)")
    @app_commands.describe(
        title="Title",
        link="Optional link",
        club="Club type (default: manga)",
    )
    async def set_series(
        self,
        interaction: discord.Interaction,
        title: str,
        link: str | None = None,
        club: str = "manga",
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("series.error.no_cfg", club=club), ephemeral=True
            )
        series_id = models.create_series(
            interaction.guild_id, cfg["club_id"], title.strip(), (link or "").strip(), None
        )
        await interaction.response.send_message(
            S("series.set_manual.ok", club=club, title=title, id=series_id), ephemeral=True
        )

    @app_commands.command(name="list_series", description="List series in this club")
    @app_commands.describe(club="Club type (default: manga)")
    async def list_series(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(
                S("series.error.no_cfg", club=club), ephemeral=True
            )
        rows = models.list_series(interaction.guild_id, cfg["club_id"])
        if not rows:
            return await interaction.response.send_message(S("series.list.none"), ephemeral=True)

        embed = discord.Embed(title=S("series.list.title", club=club), color=discord.Color.pink())
        for sid, title, link, status in rows[:25]:
            embed.add_field(
                name=S("series.list.row_title", id=sid, title=title, status=status),
                value=(link or S("series.list.no_link")),
                inline=False,
            )
        await interaction.response.send_message(embed=embed, ephemeral=True)

    @app_commands.command(name="plan_discussions", description="Create discussion events for a series")
    @app_commands.describe(
        total_chapters="Total chapter count",
        chapters_per_section="Chapters per discussion (e.g., 10)",
        hour_local="Hour (0-23) for the first discussion start (defaults to next Friday at this hour)",
        duration_hours="Duration per event (default 2)",
        days_between="Days between discussions (default 7 = weekly)",
        club="Club type (default: manga)",
        series_id="Optional explicit series id",
    )
    async def plan_discussions(
        self,
        interaction: discord.Interaction,
        total_chapters: app_commands.Range[int, 1, 5000],
        chapters_per_section: app_commands.Range[int, 1, 200],
        hour_local: app_commands.Range[int, 0, 23] = 18,
        duration_hours: app_commands.Range[int, 1, 12] = 2,
        days_between: app_commands.Range[int, 1, 60] = 7,
        club: str = "manga",
        series_id: int | None = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=True)

        club = (club or "").strip() or "manga"
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.followup.send(S("series.error.no_cfg", club=club), ephemeral=True)

        if series_id:
            srow = models.get_series(series_id)
            if not srow or srow[1] != interaction.guild_id:
                return await interaction.followup.send(S("series.plan.error.not_found"), ephemeral=True)
            sid = srow[0]; title = srow[2]; link = srow[3]
            series = (sid, title, link)
        else:
            series = models.latest_active_series(interaction.guild_id, cfg["club_id"])
        if not series:
            return await interaction.followup.send(S("series.plan.error.no_active"), ephemeral=True)

        series_id, title, link = series

        sections = []
        start = 1
        while start <= total_chapters:
            end = min(start + chapters_per_section - 1, total_chapters)
            sections.append((start, end))
            start = end + 1

        first_event_local = next_friday_at(hour_local)

        created = 0
        failures = []
        for idx, (s, e) in enumerate(sections):
            label = S("series.plan.label", s=s, e=e)  # "Ch. s–e"
            start_dt_local = first_event_local + timedelta(days=days_between * idx)
            end_dt_local = start_dt_local + timedelta(hours=duration_hours)

            start_dt = _to_utc(start_dt_local)
            end_dt = _to_utc(end_dt_local)

            try:
                event = await interaction.guild.create_scheduled_event(
                    name=S("series.plan.event_name", title=title, label=label),
                    start_time=start_dt,
                    end_time=end_dt,
                    privacy_level=discord.PrivacyLevel.guild_only,
                    entity_type=discord.EntityType.external,
                    description=(S("series.plan.desc_with_link", title=title, label=label, link=link)
                                 if link else S("series.plan.desc_no_link", title=title, label=label)),
                    location=S("series.plan.location"),
                )
                models.add_discussion_section(series_id, label, s, e, to_iso(start_dt), event.id)
                created += 1
            except Exception as ex:
                failures.append((label, str(ex)))
                log.exception("Failed to create scheduled event for %s (%s-%s): %s", title, s, e, ex)

            if (idx + 1) % 5 == 0:
                await asyncio.sleep(1.0)

        msg = S(
            "series.plan.summary",
            club=club,
            created=created,
            total=len(sections),
            title=title,
            first_ts=int(first_event_local.astimezone(timezone.utc).timestamp()),
            cadence=days_between,
        )
        if failures:
            msg += S("series.plan.summary_fail_tail", fail=len(failures))
        await interaction.followup.send(msg, ephemeral=True)

async def setup(bot: commands.Bot):
    await bot.add_cog(SeriesCog(bot))
