from __future__ import annotations

import csv
import json
import io
import logging
import time
from typing import Optional, Set

import discord
from discord import app_commands
from discord.ext import commands

from ..models import activity, activity_report, message_archive, rpg
from ..strings import S
from ..utils.maintact import month_from_day

log = logging.getLogger(__name__)


def require_manage_guild() -> app_commands.Check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
            return False
        if not interaction.user.guild_permissions.manage_guild:  # type: ignore[attr-defined]
            await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)
            return False
        return True

    return app_commands.check(predicate)


def read_csv(attachment: discord.Attachment):
    raw = attachment.read()
    text = raw.decode("utf-8", errors="replace")
    return csv.reader(io.StringIO(text))


class MaintActivityCog(commands.Cog):
    """Admin tools to import day/month CSVs and rebuild aggregates."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="maint", description="Admin: activity maintenance")

    @group.command(
        name="activity_report",
        description="ADMIN: Generate a full activity analytics report from the archive.",
    )
    @app_commands.describe(
        timezone="IANA timezone name for heatmaps (default America/Los_Angeles)",
        apply_rpg_stats="If enabled, recompute RPG stats from the generated report.",
    )
    @require_manage_guild()
    async def activity_report_cmd(
        self,
        interaction: discord.Interaction,
        timezone: str = activity_report.DEFAULT_TIMEZONE,
        apply_rpg_stats: bool = False,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        guild = interaction.guild
        guild_id = interaction.guild_id
        if guild_id is None:
            await interaction.followup.send("Guild not resolved.", ephemeral=True)
            return

        bot_ids = {member.id for member in (guild.members if guild else []) if member.bot}
        member_count = getattr(guild, "member_count", None)

        try:
            report = activity_report.generate_activity_report(
                guild_id,
                timezone_name=timezone or activity_report.DEFAULT_TIMEZONE,
                member_count=member_count,
                bot_user_ids=bot_ids,
            )
        except Exception:
            log.exception("maint.activity_report.failed", extra={"guild_id": guild_id})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)
            return

        json_payload = json.dumps(report.to_dict(), indent=2, ensure_ascii=False)
        buffer = io.BytesIO(json_payload.encode("utf-8"))
        buffer.seek(0)
        filename = f"activity_report_{guild_id}.json"

        if apply_rpg_stats:
            try:
                stat_map = activity_report.compute_rpg_stats_from_report(report)
                updated = rpg.apply_stat_snapshot(guild_id, stat_map)
            except Exception:
                log.exception("maint.activity_report.apply_stats_failed", extra={"guild_id": guild_id})
                updated = 0
            buffer.seek(0)
            await interaction.followup.send(
                content=f"Generated report and updated stats for **{updated}** member(s).",
                files=[discord.File(buffer, filename=filename)],
                ephemeral=True,
            )
        else:
            buffer.seek(0)
            await interaction.followup.send(
                content="Generated archive analytics report.",
                files=[discord.File(buffer, filename=filename)],
                ephemeral=True,
            )

    @group.command(name="import_day_csv", description="ADMIN: import day-scope CSV and rebuild months.")
    @app_commands.describe(
        file="CSV exported via /activity export scope=day",
        month="Optional YYYY-MM filter; if set, only rows for this month are imported",
    )
    @require_manage_guild()
    async def import_day_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        month: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
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
                activity.upsert_member_messages_day(interaction.guild_id, uid, day, cnt)
                touched.add(month_from_day(day))
                rows_imported += 1
            except Exception:
                log.exception("maint.import_day_csv.row_failed", extra={"guild_id": interaction.guild_id, "row": row})

        rebuilt = 0
        for m in sorted(touched):
            try:
                activity.rebuild_month_from_days(interaction.guild_id, m)
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
    @require_manage_guild()
    async def import_month_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        month: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        reader = csv.reader(io.StringIO((await file.read()).decode("utf-8", errors="replace")))
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
                activity.upsert_member_messages_month(interaction.guild_id, uid, mon, cnt)
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
    @require_manage_guild()
    async def rebuild_month(self, interaction: discord.Interaction, month: str):
        await interaction.response.defer(ephemeral=True, thinking=True)
        try:
            activity.rebuild_month_from_days(interaction.guild_id, month)
            await interaction.followup.send(f"Rebuilt aggregates for **{month}**.", ephemeral=True)
        except Exception:
            log.exception("maint.rebuild_month.failed", extra={"guild_id": interaction.guild_id, "month": month})
            await interaction.followup.send(S("common.error_generic"), ephemeral=True)

    @group.command(
        name="replay_archive",
        description="ADMIN: Replay archived messages into activity metrics and RPG XP.",
    )
    @app_commands.describe(
        reset_metrics="Delete existing message/word/emoji/mention metrics before replaying.",
        reset_xp="Delete RPG progress rows before replaying.",
        respec_stats="Redistribute stat points using the current formula after replay.",
        chunk_size="Messages to process between progress updates (default 1000).",
    )
    @require_manage_guild()
    async def replay_archive(
        self,
        interaction: discord.Interaction,
        reset_metrics: bool = True,
        reset_xp: bool = True,
        respec_stats: bool = True,
        chunk_size: int = 1000,
    ):
        await interaction.response.defer(ephemeral=True, thinking=True)

        guild_id = interaction.guild_id
        if guild_id is None:
            await interaction.edit_original_response(content="Guild not resolved.")
            return

        activity_cog = self.bot.get_cog("ActivityCog")
        if activity_cog is None or not hasattr(activity_cog, "replay_archived_messages"):
            await interaction.edit_original_response(
                content="Activity cog is not loaded; cannot replay archive."
            )
            return

        if chunk_size <= 0:
            chunk_size = 1000

        await interaction.edit_original_response(content="Preparing archive replay…")

        if reset_metrics:
            try:
                activity.reset_member_activity(guild_id, "all")
                activity.reset_member_words(guild_id, "all")
                activity.reset_member_mentions(guild_id, "all")
                activity.reset_member_mentions_sent(guild_id, "all")
                activity.reset_member_emoji_chat(guild_id, "all")
                activity.reset_member_emoji_only(guild_id, "all")
                activity.reset_member_emoji_react(guild_id, "all")
                activity.reset_member_reactions_received(guild_id, "all")
                activity.reset_member_channel_totals(guild_id)
            except Exception:
                log.exception("maint.replay_archive.reset_metrics_failed", extra={"guild_id": guild_id})
                await interaction.edit_original_response(
                    content="Failed to reset metrics. Check logs for details.",
                )
                return

        cleared_rows = 0
        if reset_xp:
            try:
                cleared_rows = rpg.reset_progress(guild_id)
            except Exception:
                log.exception("maint.replay_archive.reset_xp_failed", extra={"guild_id": guild_id})
                await interaction.edit_original_response(
                    content="Failed to reset RPG progress. Check logs for details.",
                )
                return

        summary = message_archive.stats_summary(guild_id)
        total_messages = int(summary.get("messages", 0))
        base_line = (
            f"Replaying **{total_messages:,}** archived messages…"
            if total_messages
            else "Replaying archived messages…"
        )

        last_update = 0.0

        async def progress_cb(processed: int) -> None:
            nonlocal last_update
            now = time.monotonic()
            if processed and now - last_update < 1.5 and processed < total_messages:
                return
            last_update = now
            lines = [base_line, f"Processed **{processed:,}** message(s)…"]
            await interaction.edit_original_response(content="\n".join(lines))

        await progress_cb(0)

        processed = await activity_cog.replay_archived_messages(
            message_archive.iter_guild_messages(guild_id, chunk_size=chunk_size),
            yield_every=chunk_size,
            progress_cb=progress_cb,
        )

        redistributed = 0
        if respec_stats:
            try:
                redistributed = rpg.respec_stats_to_formula(guild_id)
            except Exception:
                log.exception("maint.replay_archive.respec_failed", extra={"guild_id": guild_id})
                await interaction.edit_original_response(
                    content=(
                        "Archive replay completed, but redistributing stat points failed. "
                        "XP and metrics were still updated."
                    )
                )
                return

        lines = [
            f"Archive replay complete. Processed **{processed:,}** message(s).",
        ]
        if reset_metrics:
            lines.append("Activity metrics were reset before replaying.")
        if reset_xp:
            lines.append(f"Cleared **{cleared_rows:,}** RPG progress row(s) before replay.")
        if respec_stats:
            lines.append(f"Redistributed stat points for **{redistributed:,}** member(s).")

        await interaction.edit_original_response(content="\n".join(lines))


async def setup(bot: commands.Bot):
    await bot.add_cog(MaintActivityCog(bot))
    log.info("Loaded MaintActivityCog")
