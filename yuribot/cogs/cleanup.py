from __future__ import annotations

import json
import logging
import os
from io import StringIO, BytesIO
from pathlib import Path
from typing import Iterable, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from ..strings import S  # optional, only for "guild only"

log = logging.getLogger(__name__)
MU_STATE_FILE = Path("./data/mu_watch.json")


# -----------------------
# DB helpers
# -----------------------
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
    try:
        return await inter.client.is_owner(inter.user)  # type: ignore[attr-defined]
    except Exception:
        return False


# -----------------------
# Cog
# -----------------------
class CleanupCog(commands.Cog):
    """Admin DB tools, MU state, command sync, and activity migration/exports."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Admin DB utilities")

    # ---- schema helpers ----
    def _has_table(self, con, table: str) -> bool:
        cur = con.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,))
        return cur.fetchone() is not None

    def _table_cols(self, con, table: str) -> set[str]:
        cur = con.execute(f"PRAGMA table_info({table})")
        return {r[1] for r in cur.fetchall()}

    def _guess_legacy(self, con) -> tuple[tuple[str, dict], tuple[str, dict]]:
        month_candidates = [
            "member_activity_month", "messages_month", "activity_month",
            "member_messages_month", "member_activity_monthly",
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

    # ---------- Sync ----------
    @group.command(name="sync_commands", description="Owner: force sync (global; also dev if set).")
    @app_commands.check(_owner_only)
    async def sync_commands(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            # Always sync globals
            global_synced = await interaction.client.tree.sync()  # type: ignore[attr-defined]
            msg = [f"Global: {len(global_synced)}"]

            # If DEV_GUILD_ID present, also sync there
            dev_gid = os.getenv("DEV_GUILD_ID")
            if dev_gid:
                guild = discord.Object(id=int(dev_gid))
                interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
                dev_synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
                msg.append(f"Dev {dev_gid}: {len(dev_synced)}")

            await interaction.followup.send("Synced → " + " | ".join(msg), ephemeral=True)
        except Exception as e:
            log.exception("cleanup.sync_commands.failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    # ---------- Purge here ----------
    @group.command(name="purge_here", description="Delete rows for THIS server (dry-run optional).")
    @app_commands.describe(dry_run="Only report; no delete.", vacuum="Run VACUUM after delete.")
    @app_commands.default_permissions(manage_guild=True)
    async def purge_here(
        self, interaction: discord.Interaction, dry_run: bool = True, vacuum: bool = False
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if callable(S) else "Server only.", ephemeral=True
            )
        await interaction.response.defer(ephemeral=True)
        gid = int(interaction.guild_id)
        import sqlite3

        total, lines = 0, []
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
                    total += n
            if dry_run:
                cur.execute("ROLLBACK")
            else:
                con.commit()
                if vacuum:
                    con.execute("VACUUM")
            con.close()
        except sqlite3.Error as e:
            log.exception("cleanup.purge_here.failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)

        head = f"**Dry-run**: would delete {total} rows." if dry_run else f"Deleted **{total}** rows."
        await interaction.followup.send(head + ("\n" + "\n".join(lines) if lines else "\n(nothing)"), ephemeral=True)

    # ---------- Prune unknown ----------
    @group.command(name="prune_unknown_guilds", description="Owner: delete rows for guilds not in bot.")
    @app_commands.describe(dry_run="Only report; no delete.", vacuum="Run VACUUM after delete.")
    @app_commands.check(_owner_only)
    async def prune_unknown_guilds(
        self, interaction: discord.Interaction, dry_run: bool = True, vacuum: bool = False
    ):
        await interaction.response.defer(ephemeral=True)

        live_ids = {g.id for g in self.bot.guilds}
        ids_csv = ",".join(str(i) for i in sorted(live_ids)) or "0"
        import sqlite3

        total, lines = 0, []
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
                    total += int(n or 0)
            if dry_run:
                cur.execute("ROLLBACK")
            else:
                con.commit()
                if vacuum:
                    con.execute("VACUUM")
            con.close()
        except sqlite3.Error as e:
            log.exception("cleanup.prune_unknown_guilds.failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)

        head = (
            f"**Dry-run**: would delete {total} rows (keeping {len(live_ids)} guilds)."
            if dry_run else
            f"Deleted **{total}** rows (kept {len(live_ids)} guilds)."
        )
        await interaction.followup.send(head + ("\n" + "\n".join(lines) if lines else "\n(nothing)"), ephemeral=True)

    # ---------- MU purge ----------
    @group.command(name="mu_purge_here", description="Delete MU watcher state for THIS server.")
    @app_commands.default_permissions(manage_guild=True)
    async def mu_purge_here(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if callable(S) else "Server only.", ephemeral=True
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

    # ---------- VACUUM ----------
    @group.command(name="vacuum", description="Owner: VACUUM the SQLite DB.")
    @app_commands.check(_owner_only)
    async def vacuum_db(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        import sqlite3
        try:
            con = connect()
            con.execute("VACUUM")
            con.close()
        except sqlite3.Error as e:
            log.exception("cleanup.vacuum.failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)
        await interaction.followup.send("VACUUM complete.", ephemeral=True)

    # ---------- Probe legacy ----------
    @group.command(name="probe_legacy_activity", description="Show likely legacy activity tables.")
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
            lines.append(f"Month: **{mon[0]}** (cols: guild_id,user_id,{mon[1]['month']},{mon[1]['count']})")
        else:
            lines.append("Month: (none)")
        if tot[0]:
            lines.append(f"Total: **{tot[0]}** (cols: guild_id,user_id,{tot[1]['count']})")
        else:
            lines.append("Total: (none)")

        await interaction.followup.send("\n".join(lines), ephemeral=True)

    # ---------- Migrate (additive) ----------
    @group.command(name="migrate_activity_db", description="ADD legacy message counts into new tables.")
    @app_commands.describe(
        src_month_table="Legacy month table (auto if blank)",
        src_month_col_month="Legacy month col (auto if blank)",
        src_month_col_count="Legacy month count col (auto if blank)",
        src_total_table="Legacy total table (auto if blank)",
        src_total_col_count="Legacy total count col (auto if blank)",
        dry_run="Only report; no writes."
    )
    @app_commands.default_permissions(manage_guild=True)
    async def migrate_activity_db(
        self,
        interaction: discord.Interaction,
        src_month_table: Optional[str] = None,
        src_month_col_month: Optional[str] = None,
        src_month_col_count: Optional[str] = None,
        src_total_table: Optional[str] = None,
        src_total_col_count: Optional[str] = None,
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

            # dest checks (names based on your new schema)
            dest_month = "member_activity_monthly"
            dest_total = "member_activity_total"
            for t in (dest_month, dest_total):
                if not self._has_table(con, t):
                    raise RuntimeError(f"Destination table missing: {t}")

            # source detection
            mon_guess, tot_guess = self._guess_legacy(con)
            mon_table = src_month_table or (mon_guess[0] or "")
            tot_table = src_total_table or (tot_guess[0] or "")

            mon_c = {}
            if mon_table:
                mon_c = {
                    "guild": "guild_id",
                    "user": "user_id",
                    "month": src_month_col_month or mon_guess[1].get("month"),
                    "count": src_month_col_count or mon_guess[1].get("count"),
                }
                if None in mon_c.values():
                    raise RuntimeError(f"{mon_table}: need columns; pass src_month_col_month/src_month_col_count.")

            tot_c = {}
            if tot_table:
                tot_c = {
                    "guild": "guild_id",
                    "user": "user_id",
                    "count": src_total_col_count or tot_guess[1].get("count"),
                }
                if None in tot_c.values():
                    raise RuntimeError(f"{tot_table}: need src_total_col_count.")

            ops = []

            # month add-merge
            if mon_table:
                cols = self._table_cols(con, mon_table)
                missing = {mon_c["guild"], mon_c["user"], mon_c["month"], mon_c["count"]} - cols
                if missing:
                    raise RuntimeError(f"{mon_table} missing columns: {missing}")
                cur.execute(f"SELECT COUNT(*) FROM {mon_table} WHERE {mon_c['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{mon_table}: {n_rows} month rows for this guild")
                if not dry_run and n_rows:
                    cur.execute("BEGIN")
                    cur.execute(f"""
                        INSERT INTO {dest_month} (guild_id, month, user_id, count)
                        SELECT {mon_c['guild']}, {mon_c['month']}, {mon_c['user']}, {mon_c['count']}
                        FROM {mon_table}
                        WHERE {mon_c['guild']}=?
                        ON CONFLICT(guild_id, month, user_id)
                        DO UPDATE SET count = {dest_month}.count + excluded.count
                    """, (gid,))
                    con.commit()

            # total add-merge
            if tot_table:
                cols = self._table_cols(con, tot_table)
                missing = {tot_c["guild"], tot_c["user"], tot_c["count"]} - cols
                if missing:
                    raise RuntimeError(f"{tot_table} missing columns: {missing}")
                cur.execute(f"SELECT COUNT(*) FROM {tot_table} WHERE {tot_c['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{tot_table}: {n_rows} total rows for this guild")
                if not dry_run and n_rows:
                    cur.execute("BEGIN")
                    cur.execute(f"""
                        INSERT INTO {dest_total} (guild_id, user_id, count)
                        SELECT {tot_c['guild']}, {tot_c['user']}, {tot_c['count']}
                        FROM {tot_table}
                        WHERE {tot_c['guild']}=?
                        ON CONFLICT(guild_id, user_id)
                        DO UPDATE SET count = {dest_total}.count + excluded.count
                    """, (gid,))
                    con.commit()

            con.close()
            if not mon_table and not tot_table:
                return await interaction.followup.send(
                    "No legacy tables found. Try /cleanup probe_legacy_activity or pass names.",
                    ephemeral=True,
                )

            head = "**Dry-run**:" if dry_run else "**Done**:"
            body = "\n".join(ops) if ops else "(nothing found)"
            await interaction.followup.send(f"{head}\n{body}", ephemeral=True)

        except Exception as e:
            log.exception("cleanup.migrate_activity_db.failed", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # ---------- Export legacy (CSV) ----------
    @group.command(name="export_legacy_activity", description="Export legacy month/total tables as CSV.")
    @app_commands.describe(
        scope="What to export",
        src_month_table="Legacy month table (auto if blank)",
        src_month_col_month="Legacy month col (auto if blank)",
        src_month_col_count="Legacy month count col (auto if blank)",
        src_total_table="Legacy total table (auto if blank)",
        src_total_col_count="Legacy total count col (auto if blank)"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="month", value="month"),
        app_commands.Choice(name="total", value="total"),
        app_commands.Choice(name="both", value="both"),
    ])
    @app_commands.default_permissions(manage_guild=True)
    async def export_legacy_activity(
        self,
        interaction: discord.Interaction,
        scope: app_commands.Choice[str],
        src_month_table: Optional[str] = None,
        src_month_col_month: Optional[str] = None,
        src_month_col_count: Optional[str] = None,
        src_total_table: Optional[str] = None,
        src_total_col_count: Optional[str] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Server only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        gid = int(interaction.guild_id)
        import csv, sqlite3

        try:
            con = connect()
            cur = con.cursor()
            mon_guess, tot_guess = self._guess_legacy(con)

            files: list[discord.File] = []

            if scope.value in ("month", "both"):
                mon_table = src_month_table or (mon_guess[0] or "")
                if not mon_table:
                    return await interaction.followup.send("No legacy month table found.", ephemeral=True)
                mon_mon = src_month_col_month or mon_guess[1].get("month")
                mon_cnt = src_month_col_count or mon_guess[1].get("count")
                if not mon_mon or not mon_cnt:
                    return await interaction.followup.send("Specify month/count columns for legacy month table.", ephemeral=True)

                buf = StringIO(); w = csv.writer(buf)
                w.writerow(["guild_id", "month", "user_id", "messages"])
                for gid2, mon, uid, cnt in cur.execute(
                    f"SELECT guild_id, {mon_mon}, user_id, {mon_cnt} FROM {mon_table} WHERE guild_id=?", (gid,)
                ):
                    w.writerow([gid2, mon, uid, cnt])
                data = buf.getvalue().encode("utf-8")
                files.append(discord.File(BytesIO(data), filename=f"legacy-month-{gid}.csv"))

            if scope.value in ("total", "both"):
                tot_table = src_total_table or (tot_guess[0] or "")
                if not tot_table:
                    return await interaction.followup.send("No legacy total table found.", ephemeral=True)
                tot_cnt = src_total_col_count or tot_guess[1].get("count")
                if not tot_cnt:
                    return await interaction.followup.send("Specify count column for legacy total table.", ephemeral=True)

                buf = StringIO(); w = csv.writer(buf)
                w.writerow(["guild_id", "user_id", "messages"])
                for gid2, uid, cnt in cur.execute(
                    f"SELECT guild_id, user_id, {tot_cnt} FROM {tot_table} WHERE guild_id=?", (gid,)
                ):
                    w.writerow([gid2, uid, cnt])
                data = buf.getvalue().encode("utf-8")
                files.append(discord.File(BytesIO(data), filename=f"legacy-total-{gid}.csv"))

            con.close()

            if not files:
                return await interaction.followup.send("Nothing to export.", ephemeral=True)
            await interaction.followup.send(files=files, ephemeral=True)

        except Exception as e:
            log.exception("cleanup.export_legacy_activity.failed")
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    # ---------- Purge legacy table ----------
    @group.command(name="purge_legacy_table", description="Drop a legacy table or delete this guild's rows.")
    @app_commands.describe(
        table="Legacy table name",
        scope="Delete this guild's rows or DROP the table",
        dry_run="Only report; no changes"
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="guild", value="guild"),
        app_commands.Choice(name="drop", value="drop"),
    ])
    async def purge_legacy_table(
        self, interaction: discord.Interaction, table: str, scope: app_commands.Choice[str], dry_run: bool = True
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
                con.commit(); con.close()
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
                return await interaction.followup.send(f"**Dry-run**: would delete {n_rows} row(s) from `{table}`.", ephemeral=True)

            cur.execute("BEGIN")
            cur.execute(f"DELETE FROM {table} WHERE guild_id=?", (gid,))
            con.commit(); con.close()
            await interaction.followup.send(f"Deleted {n_rows} row(s) from `{table}`.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("cleanup.purge_legacy_table.failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
