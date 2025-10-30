# cogs/cleanup.py
from __future__ import annotations
import csv, io, json, logging, os
from pathlib import Path
from typing import Iterable, Optional, Tuple
import discord
from discord import app_commands
from discord.ext import commands
from ..db import connect
from ..strings import S

log = logging.getLogger(__name__)
MU_STATE_FILE = Path("./data/mu_watch.json")
OWNER_USER_ID = 49670556760408064  # your Discord user ID


async def _owner_only(inter: discord.Interaction) -> bool:
    try:
        return await inter.client.is_owner(inter.user)
    except Exception:
        return False


async def _is_authorized(inter: discord.Interaction) -> bool:
    return inter.user.id == OWNER_USER_ID or await _owner_only(inter)


class CleanupCog(commands.Cog):
    """Admin maintenance utilities"""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Admin maintenance utilities")

    # --- helpers ---
    def _has_table(self, con, table: str) -> bool:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None

    def _table_cols(self, con, table: str) -> set[str]:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}

    def _guess_legacy(self, con):
        month_candidates = ["member_activity_month", "member_activity_monthly"]
        total_candidates = ["member_activity_total"]
        def find(pool):
            for t in pool:
                if not self._has_table(con, t): continue
                cols = self._table_cols(con, t)
                if {"guild_id", "user_id"} <= cols: return t
            return ""
        return find(month_candidates), find(total_candidates)

    # ------------------------
    # export_legacy_activity
    # ------------------------
    @group.command(name="export_legacy_activity", description="Export legacy activity CSV for a guild.")
    @app_commands.describe(target_guild_id="Guild ID to export (as string)")
    @app_commands.check(_is_authorized)
    async def export_legacy_activity(
        self,
        interaction: discord.Interaction,
        target_guild_id: str,
    ):
        await interaction.response.defer(ephemeral=True)
        gid = int(target_guild_id)
        import sqlite3

        con = connect()
        mon_table, tot_table = self._guess_legacy(con)
        files = []

        if mon_table:
            cur = con.execute(
                f"SELECT guild_id, month, user_id, count as messages FROM {mon_table} "
                f"WHERE guild_id=? ORDER BY month, messages DESC", (gid,)
            )
            rows = cur.fetchall()
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["guild_id", "month", "user_id", "messages"])
            w.writerows(rows)
            files.append(discord.File(io.BytesIO(buf.getvalue().encode()), f"legacy-month-{gid}.csv"))

        if tot_table:
            cur = con.execute(
                f"SELECT guild_id, user_id, count as messages FROM {tot_table} "
                f"WHERE guild_id=? ORDER BY messages DESC", (gid,)
            )
            rows = cur.fetchall()
            buf = io.StringIO()
            w = csv.writer(buf)
            w.writerow(["guild_id", "user_id", "messages"])
            w.writerows(rows)
            files.append(discord.File(io.BytesIO(buf.getvalue().encode()), f"legacy-total-{gid}.csv"))

        con.close()
        if files:
            await interaction.followup.send(files=files, ephemeral=True)
        else:
            await interaction.followup.send("No legacy data found.", ephemeral=True)

    # ------------------------
    # migrate_activity_db
    # ------------------------
    @group.command(name="migrate_activity_db", description="Migrate legacy data into current tables.")
    @app_commands.describe(target_guild_id="Guild ID to migrate (as string)", dry_run="Only show what would be done")
    @app_commands.check(_is_authorized)
    async def migrate_activity_db(self, interaction: discord.Interaction, target_guild_id: str, dry_run: bool = True):
        await interaction.response.defer(ephemeral=True)
        gid = int(target_guild_id)
        import sqlite3
        from .. import models

        con = connect()
        cur = con.cursor()
        mon_table, tot_table = self._guess_legacy(con)

        ops = []

        # Migrate monthly
        if mon_table:
            cur.execute(f"SELECT user_id, month, count FROM {mon_table} WHERE guild_id=?", (gid,))
            month_rows = cur.fetchall()
            ops.append(f"{mon_table}: {len(month_rows)} month rows")
            if not dry_run:
                for uid, mon, cnt in month_rows:
                    models.upsert_member_messages_month(gid, int(uid), mon, int(cnt))

        # Migrate totals
        if tot_table:
            cur.execute(f"SELECT user_id, count FROM {tot_table} WHERE guild_id=?", (gid,))
            total_rows = cur.fetchall()
            ops.append(f"{tot_table}: {len(total_rows)} total rows")
            if not dry_run:
                for uid, cnt in total_rows:
                    models.upsert_member_messages_total(gid, int(uid), int(cnt))

        con.close()
        head = "**Dry-run**:" if dry_run else "**Migration complete:**"
        body = "\n".join(ops) or "(no data found)"
        await interaction.followup.send(f"{head}\n{body}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
