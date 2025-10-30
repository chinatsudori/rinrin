# cogs/cleanup.py
from __future__ import annotations

import csv
import io
import json
import logging
import os
from pathlib import Path
from typing import Iterable, Optional, Tuple, Dict

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from ..strings import S  # only used for "guild only" text
from .. import models

log = logging.getLogger(__name__)

MU_STATE_FILE = Path("./data/mu_watch.json")

# ---------- CONFIG ----------
# Hard-gate admin commands to *you* as well as the bot owner:
OWNER_USER_ID = 49670556760408064  # Summer

# ---------- helpers ----------
def _delete_counts(cur, table: str, where_sql: str, params: Tuple) -> int:
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    (n,) = cur.fetchone() or (0,)
    if n:
        cur.execute(f"DELETE FROM {table} WHERE {where_sql}", params)
    return int(n or 0)

def _tables_for_guild_scoped_delete() -> Iterable[Tuple[str, str]]:
    return [
        ("schedule_sections", "series_id IN (SELECT id FROM series WHERE guild_id=?)"),
        ("submissions", "guild_id=?"),
        ("collections", "guild_id=?"),
        ("poll_votes", "poll_id IN (SELECT id FROM polls WHERE guild_id=?)"),
        ("poll_options", "poll_id IN (SELECT id FROM polls WHERE guild_id=?)"),
        ("polls", "guild_id=?"),
        ("movie_events", "guild_id=?"),
        ("emoji_usage_monthly", "guild_id=?"),
        ("sticker_usage_monthly", "guild_id=?"),
        ("member_activity_monthly", "guild_id=?"),
        ("member_activity_total", "guild_id=?"),
        ("mod_actions", "guild_id=?"),
        ("series", "guild_id=?"),
        ("guild_settings", "guild_id=?"),
        ("guild_config", "guild_id=?"),
        ("clubs", "guild_id=?"),
    ]

def _tables_for_prune_unknown() -> Iterable[Tuple[str, str]]:
    return [
        ("schedule_sections", "series_id IN (SELECT id FROM series WHERE guild_id NOT IN ({ids}))"),
        ("submissions", "guild_id NOT IN ({ids})"),
        ("collections", "guild_id NOT IN ({ids})"),
        ("poll_votes", "poll_id IN (SELECT id FROM polls WHERE guild_id NOT IN ({ids}))"),
        ("poll_options", "poll_id IN (SELECT id FROM polls WHERE guild_id NOT IN ({ids}))"),
        ("polls", "guild_id NOT IN ({ids})"),
        ("movie_events", "guild_id NOT IN ({ids})"),
        ("emoji_usage_monthly", "guild_id NOT IN ({ids})"),
        ("sticker_usage_monthly", "guild_id NOT IN ({ids})"),
        ("member_activity_monthly", "guild_id NOT IN ({ids})"),
        ("member_activity_total", "guild_id NOT IN ({ids})"),
        ("mod_actions", "guild_id NOT IN ({ids})"),
        ("series", "guild_id NOT IN ({ids})"),
        ("guild_settings", "guild_id NOT IN ({ids})"),
        ("guild_config", "guild_id NOT IN ({ids})"),
        ("clubs", "guild_id NOT IN ({ids})"),
    ]

async def _owner_only(inter: discord.Interaction) -> bool:
    # allow bot owner OR Summer
    try:
        if inter.user and int(inter.user.id) == OWNER_USER_ID:
            return True
    except Exception:
        pass
    try:
        return await inter.client.is_owner(inter.user)  # type: ignore[attr-defined]
    except Exception:
        return False

# -----------------------
# Cog
# -----------------------
class CleanupCog(commands.Cog):
    """Admin maintenance utilities (DB pruning, MU state, command sync, and activity migration/import)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Admin maintenance utilities")

    # ---- schema helpers ----
    def _has_table(self, con, table: str) -> bool:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None

    def _table_cols(self, con, table: str) -> set[str]:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}

    def _guess_legacy(self, con) -> tuple[tuple[str, dict], tuple[str, dict]]:
        month_candidates = [
            "member_activity_month", "messages_month", "activity_month", "member_messages_month",
            "member_activity_monthly",
        ]
        total_candidates = [
            "member_activity_total", "messages_total", "activity_total", "member_messages_total",
        ]

        def find(cols_need: list[str], also_accept: dict[str, list[str]], pool: list[str]):
            for t in pool:
                if not self._has_table(con, t):
                    continue
                cols = self._table_cols(con, t)
                if not all(c in cols for c in cols_need):
                    continue
                cmap = {"guild": "guild_id", "user": "user_id"}
                ok = True
                for want, choices in also_accept.items():
                    hit = next((c for c in choices if c in cols), None)
                    if not hit:
                        ok = False
                        break
                    cmap[want] = hit
                if ok:
                    return (t, cmap)
            return ("", {})

        mon = find(
            cols_need=["guild_id", "user_id"],
            also_accept={"month": ["month", "mon"], "count": ["count", "messages"]},
            pool=month_candidates,
        )
        tot = find(
            cols_need=["guild_id", "user_id"],
            also_accept={"count": ["count", "messages"]},
            pool=total_candidates,
        )
        return mon, tot

    # ------------------------
    # /cleanup purge_here
    # ------------------------
    @group.command(
        name="purge_here",
        description="Delete all stored data for THIS server (optional dry-run).",
    )
    @app_commands.describe(
        dry_run="If true, only report counts; no delete.",
        vacuum="Run VACUUM after deletion.",
    )
    @app_commands.check(_owner_only)
    async def purge_here(
        self,
        interaction: discord.Interaction,
        dry_run: bool = True,
        vacuum: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if callable(S) else "This command can only be used in a server.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True)
        gid = int(interaction.guild_id)

        import sqlite3
        deleted_total = 0
        lines = []

        try:
            con = connect()
            cur = con.cursor()
            cur.execute("BEGIN")
            for table, where_sql in _tables_for_guild_scoped_delete():
                if dry_run:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", (gid,))
                    (n,) = cur.fetchone() or (0,)
                else:
                    n = _delete_counts(cur, table, where_sql, (gid,))
                if n:
                    lines.append(f"• {table}: {n}")
                    deleted_total += n

            if dry_run:
                cur.execute("ROLLBACK")
            else:
                con.commit()

            if not dry_run and vacuum:
                con.execute("VACUUM")

            con.close()
        except sqlite3.Error as e:
            log.exception("purge_here failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)

        head = f"**Dry-run**: would delete {deleted_total} rows." if dry_run else f"Deleted **{deleted_total}** rows."
        detail = "\n".join(lines) if lines else "(nothing to delete)"
        await interaction.followup.send(f"{head}\n{detail}", ephemeral=True)

    # ------------------------
    # /cleanup prune_unknown_guilds
    # ------------------------
    @group.command(
        name="prune_unknown_guilds",
        description="Owner-only: delete rows for guilds the bot is not in.",
    )
    @app_commands.describe(
        dry_run="If true, only report counts; no delete.",
        vacuum="Run VACUUM after deletion.",
    )
    @app_commands.check(_owner_only)
    async def prune_unknown_guilds(
        self,
        interaction: discord.Interaction,
        dry_run: bool = True,
        vacuum: bool = False,
    ):
        await interaction.response.defer(ephemeral=True)

        live_ids = {g.id for g in self.bot.guilds}
        ids_csv = ",".join(str(i) for i in sorted(live_ids)) or "0"

        import sqlite3
        deleted_total = 0
        lines = []

        try:
            con = connect()
            cur = con.cursor()
            cur.execute("BEGIN")
            for table, where_tpl in _tables_for_prune_unknown():
                where_sql = where_tpl.format(ids=ids_csv)
                cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}")
                (n,) = cur.fetchone() or (0,)
                if not dry_run and n:
                    cur.execute(f"DELETE FROM {table} WHERE {where_sql}")
                if n:
                    lines.append(f"• {table}: {int(n or 0)}")
                    deleted_total += int(n or 0)

            if dry_run:
                cur.execute("ROLLBACK")
            else:
                con.commit()

            if not dry_run and vacuum:
                con.execute("VACUUM")

            con.close()
        except sqlite3.Error as e:
            log.exception("prune_unknown_guilds failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)

        head = (
            f"**Dry-run**: would delete {deleted_total} rows (keeping {len(live_ids)} guilds)."
            if dry_run else
            f"Deleted **{deleted_total}** rows (kept {len(live_ids)} guilds)."
        )
        detail = "\n".join(lines) if lines else "(nothing to prune)"
        await interaction.followup.send(f"{head}\n{detail}", ephemeral=True)

    # ------------------------
    # /cleanup mu_purge_here
    # ------------------------
    @group.command(
        name="mu_purge_here",
        description="Remove MangaUpdates watcher state for THIS server.",
    )
    @app_commands.check(_owner_only)
    async def mu_purge_here(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if callable(S) else "This command can only be used in a server.",
                ephemeral=True,
            )
        await interaction.response.defer(ephemeral=True)

        gid = str(interaction.guild_id)
        if not MU_STATE_FILE.exists():
            return await interaction.followup.send("No MU state file found.", ephemeral=True)

        try:
            data = json.loads(MU_STATE_FILE.read_text("utf-8"))
        except Exception:
            return await interaction.followup.send("Failed to read MU state file.", ephemeral=True)

        if gid in data:
            data.pop(gid, None)
            try:
                MU_STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            except Exception:
                return await interaction.followup.send("Failed to write MU state file.", ephemeral=True)
            return await interaction.followup.send("Removed MU watcher state for this server.", ephemeral=True)

        await interaction.followup.send("No MU watcher state for this server.", ephemeral=True)

    # ------------------------
    # /cleanup vacuum
    # ------------------------
    @group.command(
        name="vacuum",
        description="Owner-only: VACUUM the SQLite DB.",
    )
    @app_commands.check(_owner_only)
    async def vacuum_db(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        import sqlite3
        try:
            con = connect()
            con.execute("VACUUM")
            con.close()
        except sqlite3.Error as e:
            log.exception("VACUUM failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)
        await interaction.followup.send("VACUUM complete.", ephemeral=True)

    # ------------------------
    # /cleanup sync_commands and /cleanup sync_here
    # ------------------------
    @group.command(name="sync_commands", description="Owner-only: force sync of slash commands.")
    @app_commands.check(_owner_only)
    async def sync_commands(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            dev_gid = os.getenv("DEV_GUILD_ID")
            if dev_gid:
                guild = discord.Object(id=int(dev_gid))
                interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
                synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
                await interaction.followup.send(f"Synced **{len(synced)}** commands to dev guild {dev_gid}.", ephemeral=True)
            else:
                synced = await interaction.client.tree.sync()  # type: ignore[attr-defined]
                await interaction.followup.send(f"Globally synced **{len(synced)}** commands.", ephemeral=True)
        except Exception as e:
            log.exception("sync_commands failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    @group.command(name="sync_here", description="Owner-only: sync commands to THIS guild now.")
    @app_commands.check(_owner_only)
    async def sync_here(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        try:
            gid = interaction.guild_id
            guild = discord.Object(id=int(gid))
            interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
            synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
            await interaction.followup.send(f"Synced **{len(synced)}** commands to this guild.", ephemeral=True)
        except Exception as e:
            log.exception("sync_here failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    # ------------------------
    # /cleanup export_legacy_activity  (unchanged)
    # ------------------------
    @group.command(name="export_legacy_activity", description="Export legacy activity CSV for THIS guild.")
    @app_commands.describe(
        month_table="Legacy month table (optional)",
        month_col_month="Legacy month column (optional)",
        month_col_count="Legacy month count column (optional)",
        total_table="Legacy total table (optional)",
        total_col_count="Legacy total count column (optional)",
    )
    @app_commands.check(_owner_only)
    async def export_legacy_activity(
        self,
        interaction: discord.Interaction,
        month_table: Optional[str] = None,
        month_col_month: Optional[str] = None,
        month_col_count: Optional[str] = None,
        total_table: Optional[str] = None,
        total_col_count: Optional[str] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        gid = int(interaction.guild_id)
        import sqlite3

        try:
            con = connect()
            mon_guess, tot_guess = self._guess_legacy(con)

            mon_table = month_table or (mon_guess[0] or "")
            tot_table = total_table or (tot_guess[0] or "")

            files: list[discord.File] = []

            if mon_table:
                m_month = month_col_month or mon_guess[1].get("month")
                m_count = month_col_count or mon_guess[1].get("count")
                cols = self._table_cols(con, mon_table)
                need = {"guild_id", "user_id", m_month, m_count}
                if need <= cols:
                    cur = con.execute(
                        f"SELECT guild_id, {m_month} as month, user_id, {m_count} as messages "
                        f"FROM {mon_table} WHERE guild_id=? ORDER BY month, messages DESC",
                        (gid,),
                    )
                    rows = cur.fetchall()
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    w.writerow(["guild_id", "month", "user_id", "messages"])
                    for r in rows:
                        w.writerow(r)
                    files.append(discord.File(fp=io.BytesIO(buf.getvalue().encode("utf-8")), filename=f"legacy-month-{gid}.csv"))

            if tot_table:
                t_count = total_col_count or tot_guess[1].get("count")
                cols = self._table_cols(con, tot_table)
                need = {"guild_id", "user_id", t_count}
                if need <= cols:
                    cur = con.execute(
                        f"SELECT guild_id, user_id, {t_count} as messages FROM {tot_table} "
                        f"WHERE guild_id=? ORDER BY messages DESC",
                        (gid,),
                    )
                    rows = cur.fetchall()
                    buf = io.StringIO()
                    w = csv.writer(buf)
                    w.writerow(["guild_id", "user_id", "messages"])
                    for r in rows:
                        w.writerow(r)
                    files.append(discord.File(fp=io.BytesIO(buf.getvalue().encode("utf-8")), filename=f"legacy-total-{gid}.csv"))

            con.close()

            if files:
                await interaction.followup.send(files=files, ephemeral=True)
            else:
                await interaction.followup.send("No legacy tables/rows found for this guild.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("export_legacy_activity failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)

    # ------------------------
    # /cleanup import_activity_csv  **NEW**
    # ------------------------
    @group.command(
        name="import_activity_csv",
        description="Import a month of message activity from a CSV attachment (guild_id,month,user_id,messages).",
    )
    @app_commands.describe(
        file="CSV attachment (headers: guild_id,month,user_id,messages)",
        target_guild_id="Target guild ID (defaults to current guild).",
        month="YYYY-MM month to import (must match CSV).",
        mode="replace = overwrite month, add = merge with existing (default: add).",
    )
    @app_commands.choices(mode=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="replace", value="replace"),
    ])
    @app_commands.check(_owner_only)
    async def import_activity_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        target_guild_id: Optional[str] = None,
        month: Optional[str] = None,
        mode: app_commands.Choice[str] | None = None,
    ):
        # simple regex, reuse from ActivityCog if you prefer
        import re
        MONTH_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")

        await interaction.response.defer(ephemeral=True)

        # Resolve target guild
        gid = int(target_guild_id) if target_guild_id else int(interaction.guild_id or 0)
        if gid <= 0:
            return await interaction.followup.send("Need a valid target guild id.", ephemeral=True)

        # Month sanity
        if not month or not MONTH_RE.match(month):
            return await interaction.followup.send("Provide a valid month (YYYY-MM).", ephemeral=True)

        mode_val = (mode.value if mode else "add")

        # Read the CSV
        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        required = {"guild_id", "month", "user_id", "messages"}
        if set(reader.fieldnames or []) < required:
            return await interaction.followup.send("CSV missing required headers.", ephemeral=True)

        # Aggregate by user (filtering to guild+month)
        incoming: Dict[int, int] = {}
        for row in reader:
            try:
                if str(row["guild_id"]).strip() != str(gid):
                    continue
                if str(row["month"]).strip() != month:
                    continue
                uid = int(str(row["user_id"]).strip())
                cnt = int(str(row["messages"]).strip())
            except Exception:
                continue
            incoming[uid] = incoming.get(uid, 0) + cnt

        if not incoming:
            return await interaction.followup.send("No matching rows for that guild/month.", ephemeral=True)

        # If replace: wipe that month first using models API if available
        if mode_val == "replace":
            if hasattr(models, "reset_member_activity"):
                try:
                    models.reset_member_activity(gid, scope="month", key=month)
                except Exception:
                    log.exception("import.reset_month_failed", extra={"guild_id": gid, "month": month})

        # Base existing totals (for add-mode exact upserts)
        existing_month: Dict[int, int] = {}
        existing_total: Dict[int, int] = {}
        try:
            rows_m = models.top_members_messages_period(gid, scope="month", key=month, limit=100_000)
            existing_month = dict(rows_m or [])
        except Exception:
            existing_month = {}
        try:
            rows_t = models.top_members_messages_total(gid, limit=100_000)
            existing_total = dict(rows_t or [])
        except Exception:
            existing_total = {}

        # Upsert via models.* so ActivityCog sees it
        upserted = 0
        total_messages = 0
        for uid, add_cnt in incoming.items():
            if hasattr(models, "upsert_member_messages_month"):
                try:
                    if mode_val == "add":
                        new_month_val = existing_month.get(uid, 0) + add_cnt
                    else:
                        new_month_val = add_cnt
                    models.upsert_member_messages_month(gid, int(uid), month, int(new_month_val))
                    upserted += 1
                    total_messages += add_cnt
                except Exception:
                    log.exception("import.upsert_month_failed", extra={"guild_id": gid, "user_id": uid})

            # update totals
            if hasattr(models, "upsert_member_messages_total"):
                try:
                    if mode_val == "add":
                        new_total_val = existing_total.get(uid, 0) + add_cnt
                    else:
                        # replace = set total to (existing_total_without_month + add_cnt)
                        # We can't easily subtract the old month portion; safest is add.
                        new_total_val = existing_total.get(uid, 0) + add_cnt
                    models.upsert_member_messages_total(gid, int(uid), int(new_total_val))
                except Exception:
                    log.exception("import.upsert_total_failed", extra={"guild_id": gid, "user_id": uid})

        await interaction.followup.send(
            f"Import complete for guild `{gid}`, month `{month}`.\n"
            f"• rows imported: {upserted} (unique users: {len(incoming)})\n"
            f"• total messages: {total_messages}\n"
            f"• mode: {mode_val}",
            ephemeral=True,
        )

    # ------------------------
    # /cleanup purge_legacy_table
    # ------------------------
    @group.command(
        name="purge_legacy_table",
        description="Remove a legacy activity table or this guild’s rows.",
    )
    @app_commands.describe(
        table="Legacy table name to purge",
        scope="Choose 'guild' to delete only this guild's rows or 'drop' to DROP the table.",
        dry_run="If true, only report; no changes.",
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="guild", value="guild"),
        app_commands.Choice(name="drop", value="drop"),
    ])
    @app_commands.check(_owner_only)
    async def purge_legacy_table(
        self,
        interaction: discord.Interaction,
        table: str,
        scope: app_commands.Choice[str],
        dry_run: bool = True,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        gid = int(interaction.guild_id)
        import sqlite3

        try:
            con = connect()
            cur = con.cursor()

            if not self._has_table(con, table):
                con.close()
                return await interaction.followup.send(f"Table `{table}` does not exist.", ephemeral=True)

            if scope.value == "drop":
                if not (await _owner_only(interaction)):
                    con.close()
                    return await interaction.followup.send("Only the bot owner can DROP tables.", ephemeral=True)
                if dry_run:
                    con.close()
                    return await interaction.followup.send(f"**Dry-run**: would DROP `{table}`.", ephemeral=True)
                cur.execute(f"DROP TABLE {table}")
                con.commit()
                con.close()
                return await interaction.followup.send(f"Dropped `{table}`.", ephemeral=True)

            cols = self._table_cols(con, table)
            if "guild_id" not in cols:
                con.close()
                return await interaction.followup.send(f"`{table}` has no `guild_id`; can't guild-prune.", ephemeral=True)

            cur.execute(f"SELECT COUNT(*) FROM {table} WHERE guild_id=?", (gid,))
            (n_rows,) = cur.fetchone() or (0,)
            if dry_run:
                con.close()
                return await interaction.followup.send(f"**Dry-run**: would delete {n_rows} rows from `{table}`.", ephemeral=True)

            cur.execute("BEGIN")
            cur.execute(f"DELETE FROM {table} WHERE guild_id=?", (gid,))
            con.commit()
            con.close()
            await interaction.followup.send(f"Deleted {n_rows} rows from `{table}`.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("purge_legacy_table failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
