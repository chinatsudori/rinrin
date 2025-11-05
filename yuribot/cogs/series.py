from __future__ import annotations

import asyncio
import logging
from datetime import timedelta, timezone
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from .. import models
from ..strings import S
from ..ui.series import build_series_list_embed
from ..utils.collection import normalized_club
from ..utils.series import build_sections, next_friday_at, to_utc
from ..utils.time import to_iso

log = logging.getLogger(__name__)


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

        club = normalized_club(club)
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(S("series.error.no_cfg", club=club), ephemeral=True)

        collection = models.latest_collection(interaction.guild_id, cfg["club_id"])
        if not collection:
            return await interaction.response.send_message(S("series.error.no_collection"), ephemeral=True)

        submission = models.get_submission(collection[0], number)
        if not submission:
            return await interaction.response.send_message(S("series.error.bad_number"), ephemeral=True)

        submission_id, title, link, *_ = submission
        series_id = models.create_series(interaction.guild_id, cfg["club_id"], title, link or "", submission_id)
        await interaction.response.send_message(
            S("series.set_from_number.ok", club=club, title=title, id=series_id),
            ephemeral=True,
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
        link: Optional[str] = None,
        club: str = "manga",
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = normalized_club(club)
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(S("series.error.no_cfg", club=club), ephemeral=True)

        series_id = models.create_series(
            interaction.guild_id,
            cfg["club_id"],
            title.strip(),
            (link or "").strip(),
            None,
        )
        await interaction.response.send_message(
            S("series.set_manual.ok", club=club, title=title, id=series_id),
            ephemeral=True,
        )

    @app_commands.command(name="list_series", description="List series in this club")
    @app_commands.describe(club="Club type (default: manga)")
    async def list_series(self, interaction: discord.Interaction, club: str = "manga"):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        club = normalized_club(club)
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.response.send_message(S("series.error.no_cfg", club=club), ephemeral=True)

        rows = models.list_series(interaction.guild_id, cfg["club_id"])
        if not rows:
            return await interaction.response.send_message(S("series.list.none"), ephemeral=True)

        embed = build_series_list_embed(club=club, rows=[(sid, title, link, status) for sid, title, link, status in rows[:25]])
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
        series_id: Optional[int] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=True)

        club = normalized_club(club)
        cfg = models.get_club_cfg(interaction.guild_id, club)
        if not cfg:
            return await interaction.followup.send(S("series.error.no_cfg", club=club), ephemeral=True)

        if series_id:
            series_row = models.get_series(series_id)
            if not series_row or series_row[1] != interaction.guild_id:
                return await interaction.followup.send(S("series.plan.error.not_found"), ephemeral=True)
            sid, title, link = series_row[0], series_row[2], series_row[3]
        else:
            latest = models.latest_active_series(interaction.guild_id, cfg["club_id"])
            if not latest:
                return await interaction.followup.send(S("series.plan.error.no_active"), ephemeral=True)
            sid, title, link = latest

        sections = build_sections(total_chapters, chapters_per_section)
        first_event_local = next_friday_at(hour_local)

        created = 0
        failures = []
        for idx, (start_ch, end_ch) in enumerate(sections):
            label = S("series.plan.label", s=start_ch, e=end_ch)
            start_local = first_event_local + timedelta(days=days_between * idx)
            end_local = start_local + timedelta(hours=duration_hours)

            start_dt = to_utc(start_local)
            end_dt = to_utc(end_local)

            description = (
                S("series.plan.desc_with_link", title=title, label=label, link=link)
                if link
                else S("series.plan.desc_no_link", title=title, label=label)
            )

            try:
                event = await interaction.guild.create_scheduled_event(
                    name=S("series.plan.event_name", title=title, label=label),
                    start_time=start_dt,
                    end_time=end_dt,
                    privacy_level=discord.PrivacyLevel.guild_only,
                    entity_type=discord.EntityType.external,
                    description=description,
                    location=S("series.plan.location"),
                )
                models.add_discussion_section(
                    sid,
                    label,
                    start_ch,
                    end_ch,
                    to_iso(start_dt),
                    event.id,
                )
                created += 1
            except Exception as exc:
                failures.append((label, str(exc)))
                log.exception(
                    "series.plan.event_failed",
                    extra={
                        "guild_id": interaction.guild_id,
                        "series_id": sid,
                        "label": label,
                        "error": str(exc),
                    },
                )

            if (idx + 1) % 5 == 0:
                await asyncio.sleep(1.0)

        summary = S(
            "series.plan.summary",
            club=club,
            created=created,
            total=len(sections),
            title=title,
            first_ts=int(first_event_local.astimezone(timezone.utc).timestamp()),
            cadence=days_between,
        )
        if failures:
            summary += S("series.plan.summary_fail_tail", fail=len(failures))
        await interaction.followup.send(summary, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(SeriesCog(bot))
