from __future__ import annotations

import csv
import io
import logging
from typing import Optional, Set

import discord
from discord import app_commands
from discord.ext import commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)


def _month_from_day(day: str) -> str:
    return day[:7]  # "YYYY-MM-DD" -> "YYYY-MM"


# A reusable permission check that BLOCKS execution but doesn't hide the command
def _require_manage_guild() -> app_commands.Check:
    async def predicate(inter: discord.Interaction) -> bool:
        if not inter.guild:
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
            return False
        if not inter.user.guild_permissions.manage_guild:  # type: ignore[attr-defined]
            await inter.response.send_message(S("common.need_manage_server"), ephemeral=True)
            return False
        return True
    return app_commands.check(predicate)


class MaintActivityCog(commands.Cog):
    """Admin tools to import day/month CSVs and rebuild aggregates."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="maint", description="Admin: activity maintenance")

    @group.command(name="import_day_csv", description="ADMIN: import day-scope CSV and rebuild months.")
    @app_commands.describe(
        file="CSV exported via /activity export scope=day",
        month="Optional YYYY-MM filter; if set, only rows for this month are imported",
    )
    @_require_manage_guild()
    async def import_day_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        month: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        raw = await file.read()
        text = raw.decode("utf-8", errors="replace")
        reader = csv.reader(io.StringIO(text))

        header = next(reader, None) or []
        try:
            idx_g = header.index("guild_id")
            idx_d = header.index("day")
            idx_u = header.index("user_id")
            idx_c = header.index("messages")
        except ValueError:
            return await interaction.followup.send(
                "Bad CSV header. Expected columns: guild_id, day, user_id, messages.",
                ephemeral=True,
            )

        touched: Set[str] = set()
        rows_imported = 0
        for row in reader:
            try:
                gid = int(row[idx_g])
                if gid != interaction.guild_id:
                    continue
                day = row[idx_d]
                if month and not day.startswith(month):
                    continue
                uid = int(row[idx_u])
                cnt = int(row[idx_c])
                if cnt <= 0:
                    continue
                models.upsert_member_messages_day(interaction.guild_id, uid, day, cnt)
                touched.add(_month_from_day(day))
                rows_imported += 1
            except Exception:
                log.exception("maint.import_day_csv.row_failed", extra={"guild_id": interaction.guild_id, "row": row})

        rebuilt = 0
        for m in sorted(touched):
            try:
                models.rebuild_month_from_days(interaction.guild_id, m)
                rebuilt += 1
            except Exception:
                log.exception("maint.rebuild_month.failed", extra={"guild_id": interaction.guild_id, "month": m})

        await interaction.followup.send(
            f"Imported **{rows_imported}** day rows. Rebuilt **{rebuilt}** month aggregates.",
            ephemeral=True,
        )

    @group.command(name="import_month_csv", description="ADMIN: import month-scope CSV (direct month upserts).")
    @app_commands.describe(
        file="CSV exported via /activity export scope=month",
        month="Optional YYYY-MM filter; if set, only rows for this month are imported",
    )
    @_require_manage_guild()
    async def import_month_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        month: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        raw = await file.read()
        text = raw.decode("utf-8", errors="replace")
        reader = csv.reader(io.StringIO(text))

        header = next(reader, None) or []
        try:
            idx_g = header.index("guild_id")
            idx_m = header.index("month")
            idx_u = header.index("user_id")
            idx_c = header.index("messages")
        except ValueError:
            return await interaction.followup.send(
                "Bad CSV header. Expected columns: guild_id, month, user_id, messages.",
                ephemeral=True,
            )

        rows_imported = 0
        months_touched: Set[str] = set()
        for row in reader:
            try:
                gid = int(row[idx_g])
                if gid != interaction.guild_id:
                    continue
                mon = row[idx_m]
                if month and mon != month:
                    continue
                uid = int(row[idx_u])
                cnt = int(row[idx_c])
                if cnt <= 0:
                    continue
                models.upsert_member_messages_month(interaction.guild_id, uid, mon, cnt)
                months_touched.add(mon)
                rows_imported += 1
            except Exception:
                log.exception("maint.import_month_csv.row_failed", extra={"guild_id": interaction.guild_id, "row": row})

        await interaction.followup.send(
            f"Imported **{rows_imported}** month rows into {len(months_touched)} month(s).",
            ephemeral=True,
        )

    @group.command(name="rebuild_month", description="ADMIN: rebuild a month aggregate from day table.")
    @app_commands.describe(month="YYYY-MM")
    @_require_manage_guild()
    async def rebuild_month(self, interaction: discord.Interaction, month: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            models.rebuild_month_from_days(interaction.guild_id, month)
            await interaction.followup.send(f"Rebuilt aggregates for **{month}**.", ephemeral=True)
        except Exception:
            log.exception("maint.rebuild_month.failed", extra={"guild_id": interaction.guild_id, "month": month})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MaintActivityCog(bot))
    log.info("Loaded MaintActivityCog")
