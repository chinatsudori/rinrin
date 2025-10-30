from __future__ import annotations

import csv
import io
import json
import logging
import os
from pathlib import Path
from typing import Iterable, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from ..strings import S  # only used for "guild only" if present

log = logging.getLogger(__name__)

MU_STATE_FILE = Path("./data/mu_watch.json")

# The only user allowed to operate on arbitrary guild IDs.
OWNER_USER_ID = 49670556760408064


# -----------------------
# Internal helpers
# -----------------------
def _delete_counts(cur, table: str, where_sql: str, params: Tuple) -> int:
    """Return number of rows to delete and perform the deletion."""
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    (n,) = cur.fetchone() or (0,)
    if n:
        cur.execute(f"DELETE FROM {table} WHERE {where_sql}", params)
    return int(n or 0)


def _tables_for_guild_scoped_delete() -> Iterable[Tuple[str, str]]:
    """Children first, then parents (adjust to your schema)."""
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
    try:
        if inter.user and inter.user.id == OWNER_USER_ID:
            return True
        return await inter.client.is_owner(inter.user)  # type: ignore[attr-defined]
    except Exception:
        return False


def _safe_guild_only_msg() -> str:
    return S("common.guild_only") if callable(S) else "This command can only be used in a server."


# -----------------------
# Cog
# -----------------------
class CleanupCog(commands.Cog):
    """Admin maintenance utilities (DB pruning, MU state, command sync, legacy activity)."""

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
        """
        Guess legacy month + total sources.
        Returns: ((month_table, colmap), (total_table, colmap))
        colmap keys: guild,user,month,count
        """
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

    # Resolve the target guild id (string → int), with permissions.
    def _resolve_gid(self, interaction: discord.Interaction, target_guild_id: Optional[str]) -> int:
        """
        If target_guild_id is provided, only OWNER_USER_ID may use it.
        Otherwise, use the current interaction guild id.
        """
        if target_guild_id:
            if interaction.user.id != OWNER_USER_ID:
                raise PermissionError("Only the bot owner can target another guild.")
            try:
                return int(target_guild_id)
            except Exception:
                raise ValueError("target_guild_id must be a numeric string.")
        # default: current guild
        if not interaction.guild_id:
            raise RuntimeError("No current guild context.")
        return int(interaction.guild_id)

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
    @app_commands.default_permissions(manage_guild=True)
    async def purge_here(
        self,
        interaction: discord.Interaction,
        dry_run: bool = True,
        vacuum: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(_safe_guild_only_msg(), ephemeral=True)

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
    @app_commands.default_permissions(manage_guild=True)
    async def mu_purge_here(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(_safe_guild_only_msg(), ephemeral=True)
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
    # /cleanup sync_commands (global/dev) and /cleanup sync_here (this guild)
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
    # /cleanup probe_legacy_activity
    # ------------------------
    @group.command(name="probe_legacy_activity", description="Find likely legacy activity tables.")
    @app_commands.default_permissions(manage_guild=True)
    async def probe_legacy_activity(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        con = connect()
        mon, tot = self._guess_legacy(con)
        con.close()

        lines = []
        if mon[0]:
            lines.append(f"Month candidate: **{mon[0]}** (cols: guild_id, user_id, {mon[1]['month']}, {mon[1]['count']})")
        else:
            lines.append("Month candidate: (none found)")
        if tot[0]:
            lines.append(f"Total candidate: **{tot[0]}** (cols: guild_id, user_id, {tot[1]['count']})")
        else:
            lines.append("Total candidate: (none found)")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    # ------------------------
    # /cleanup export_legacy_activity
    # ------------------------
    @group.command(name="export_legacy_activity", description="Export legacy activity CSV for a guild.")
    @app_commands.describe(
        target_guild_id="Guild ID to export (string). Omit to use current guild.",
        month_table="Legacy month table (optional)",
        month_col_month="Legacy month column (optional)",
        month_col_count="Legacy month count column (optional)",
        total_table="Legacy total table (optional)",
        total_col_count="Legacy total count column (optional)",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def export_legacy_activity(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[str] = None,
        month_table: Optional[str] = None,
        month_col_month: Optional[str] = None,
        month_col_count: Optional[str] = None,
        total_table: Optional[str] = None,
        total_col_count: Optional[str] = None,
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            gid = self._resolve_gid(interaction, target_guild_id)
        except Exception as e:
            return await interaction.followup.send(str(e), ephemeral=True)

        import sqlite3
        try:
            con = connect()
            mon_guess, tot_guess = self._guess_legacy(con)

            mon_table = month_table or (mon_guess[0] or "")
            tot_table = total_table or (tot_guess[0] or "")

            files: list[discord.File] = []

            # Month CSV
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

            # Total CSV
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
                await interaction.followup.send("No legacy tables/rows found for that guild.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("export_legacy_activity failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)

    # ------------------------
    # /cleanup migrate_activity_db
    # ------------------------
    @group.command(
        name="migrate_activity_db",
        description="ADD legacy month/total MESSAGE counts into new tables for a guild.",
    )
    @app_commands.describe(
        target_guild_id="Guild ID to migrate (string). Omit to use current guild.",
        src_month_table="Legacy month table (optional)",
        src_month_col_month="Legacy month column (optional)",
        src_month_col_count="Legacy month count column (optional)",
        src_total_table="Legacy total table (optional)",
        src_total_col_count="Legacy total count column (optional)",
        dry_run="Only report; no writes.",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def migrate_activity_db(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[str] = None,
        src_month_table: Optional[str] = None,
        src_month_col_month: Optional[str] = None,
        src_month_col_count: Optional[str] = None,
        src_total_table: Optional[str] = None,
        src_total_col_count: Optional[str] = None,
        dry_run: bool = True,
    ):
        await interaction.response.defer(ephemeral=True)

        try:
            gid = self._resolve_gid(interaction, target_guild_id)
        except Exception as e:
            return await interaction.followup.send(str(e), ephemeral=True)

        import sqlite3
        try:
            con = connect()
            cur = con.cursor()

            # verify destinations
            required_dests = [
                ("member_activity_monthly", {"guild_id", "month", "user_id", "count"}),
                ("member_activity_total", {"guild_id", "user_id", "count"}),
            ]
            for t, need in required_dests:
                if not self._has_table(con, t):
                    raise RuntimeError(f"Destination table missing: {t}")
                have = self._table_cols(con, t)
                missing = need - have
                if missing:
                    raise RuntimeError(f"Destination table {t} missing columns: {missing}")

            # detect sources
            mon_guess, tot_guess = self._guess_legacy(con)
            mon_table = src_month_table or (mon_guess[0] or "")
            tot_table = src_total_table or (tot_guess[0] or "")

            mon_cmap = {}
            if mon_table:
                mon_cmap = {
                    "guild": "guild_id",
                    "user": "user_id",
                    "month": src_month_col_month or mon_guess[1].get("month"),
                    "count": src_month_col_count or mon_guess[1].get("count"),
                }
                if None in mon_cmap.values():
                    raise RuntimeError(f"{mon_table}: could not infer month/count columns.")

            tot_cmap = {}
            if tot_table:
                tot_cmap = {
                    "guild": "guild_id",
                    "user": "user_id",
                    "count": src_total_col_count or tot_guess[1].get("count"),
                }
                if None in tot_cmap.values():
                    raise RuntimeError(f"{tot_table}: could not infer count column.")

            ops = []

            # MONTH add-merge
            if mon_table:
                cols = self._table_cols(con, mon_table)
                need = {mon_cmap["guild"], mon_cmap["user"], mon_cmap["month"], mon_cmap["count"]}
                missing = need - cols
                if missing:
                    raise RuntimeError(f"{mon_table} missing columns: {missing}")

                cur.execute(f"SELECT COUNT(*) FROM {mon_table} WHERE {mon_cmap['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{mon_table}: {n_rows} month rows for this guild")

                if not dry_run and n_rows:
                    cur.execute("BEGIN")
                    cur.execute(f"""
                        INSERT INTO member_activity_monthly (guild_id, month, user_id, count)
                        SELECT {mon_cmap['guild']}, {mon_cmap['month']}, {mon_cmap['user']}, {mon_cmap['count']}
                        FROM {mon_table}
                        WHERE {mon_cmap['guild']}=?
                        ON CONFLICT(guild_id, month, user_id)
                        DO UPDATE SET count = member_activity_monthly.count + excluded.count
                    """, (gid,))
                    con.commit()

            # TOTAL add-merge
            if tot_table:
                cols = self._table_cols(con, tot_table)
                need = {tot_cmap["guild"], tot_cmap["user"], tot_cmap["count"]}
                missing = need - cols
                if missing:
                    raise RuntimeError(f"{tot_table} missing columns: {missing}")

                cur.execute(f"SELECT COUNT(*) FROM {tot_table} WHERE {tot_cmap['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{tot_table}: {n_rows} total rows for this guild")

                if not dry_run and n_rows:
                    cur.execute("BEGIN")
                    cur.execute(f"""
                        INSERT INTO member_activity_total (guild_id, user_id, count)
                        SELECT {tot_cmap['guild']}, {tot_cmap['user']}, {tot_cmap['count']}
                        FROM {tot_table}
                        WHERE {tot_cmap['guild']}=?
                        ON CONFLICT(guild_id, user_id)
                        DO UPDATE SET count = member_activity_total.count + excluded.count
                    """, (gid,))
                    con.commit()

            con.close()

            if not mon_table and not tot_table:
                return await interaction.followup.send(
                    "No legacy tables detected. Run `/cleanup probe_legacy_activity` or pass table names.",
                    ephemeral=True,
                )

            head = "**Dry-run** results:" if dry_run else "**Migration complete**:"
            body = "\n".join(ops) if ops else "(nothing found for that guild)"
            await interaction.followup.send(f"{head}\n{body}", ephemeral=True)

        except Exception as e:
            log.exception("cleanup.migrate_activity_db.failed", extra={"user_id": interaction.user.id})
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # ------------------------
    # /cleanup purge_legacy_table
    # ------------------------
    @group.command(
        name="purge_legacy_table",
        description="Remove a legacy activity table or a guild’s rows.",
    )
    @app_commands.describe(
        target_guild_id="Guild ID to operate on (string). Omit to use current guild. Needed for scope=guild.",
        table="Legacy table name to purge",
        scope="Choose 'guild' to delete only that guild's rows or 'drop' to DROP the table.",
        dry_run="If true, only report; no changes.",
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="guild", value="guild"),
        app_commands.Choice(name="drop", value="drop"),
    ])
    async def purge_legacy_table(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[str],
        table: str,
        scope: app_commands.Choice[str],
        dry_run: bool = True,
    ):
        await interaction.response.defer(ephemeral=True)

        # For DROP we don't need a guild id; for guild scope we do.
        try:
            gid = None
            if scope.value == "guild":
                gid = self._resolve_gid(interaction, target_guild_id)
        except Exception as e:
            return await interaction.followup.send(str(e), ephemeral=True)

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

            # scope=guild
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
