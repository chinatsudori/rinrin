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

# ---- HARD GATE: only this user can run cleanup commands ----
ALLOWED_USER_IDS: set[int] = {49670556760408064}

async def _summer_only(inter: discord.Interaction) -> bool:
    try:
        return inter.user is not None and inter.user.id in ALLOWED_USER_IDS
    except Exception:
        return False


# -----------------------
# Internal DB helpers
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


class CleanupCog(commands.Cog):
    """Maintenance utilities: DB pruning, MU state, sync, legacy activity migration."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Maintenance utilities")

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

    # ------------------------
    # helpers
    # ------------------------
    @staticmethod
    def _resolve_gid(inter: discord.Interaction, target_guild_id: Optional[int]) -> int:
        """Use provided guild id or fall back to the interaction guild."""
        gid = int(target_guild_id or inter.guild_id or 0)
        if gid <= 0:
            raise RuntimeError("No guild id available.")
        return gid

    # ------------------------
    # /cleanup purge_here  (now: purge)
    # ------------------------
    @group.command(
        name="purge",
        description="Delete all stored data for a guild (dry-run by default).",
    )
    @app_commands.check(_summer_only)
    @app_commands.describe(
        target_guild_id="Guild ID to purge (default: current guild)",
        dry_run="If true, only report counts; no delete.",
        vacuum="Run VACUUM after deletion.",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def purge(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[int] = None,
        dry_run: bool = True,
        vacuum: bool = False,
    ):
        gid = self._resolve_gid(interaction, target_guild_id)
        await interaction.response.defer(ephemeral=True)

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
            log.exception("purge failed")
            return await interaction.followup.send(f"DB error: {e}", ephemeral=True)

        head = f"**Dry-run**: would delete {deleted_total} rows." if dry_run else f"Deleted **{deleted_total}** rows."
        detail = "\n".join(lines) if lines else "(nothing to delete)"
        await interaction.followup.send(f"Guild {gid}\n{head}\n{detail}", ephemeral=True)

    # ------------------------
    # /cleanup prune_unknown_guilds
    # ------------------------
    @group.command(
        name="prune_unknown_guilds",
        description="Delete rows for guilds the bot is not in.",
    )
    @app_commands.check(_summer_only)
    @app_commands.describe(
        dry_run="If true, only report counts; no delete.",
        vacuum="Run VACUUM after deletion.",
    )
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
    # /cleanup mu_purge
    # ------------------------
    @group.command(
        name="mu_purge",
        description="Remove MangaUpdates watcher state for a guild.",
    )
    @app_commands.check(_summer_only)
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(target_guild_id="Guild ID (default: current guild)")
    async def mu_purge(self, interaction: discord.Interaction, target_guild_id: Optional[int] = None):
        gid = self._resolve_gid(interaction, target_guild_id)
        await interaction.response.defer(ephemeral=True)

        if not MU_STATE_FILE.exists():
            return await interaction.followup.send("No MU state file found.", ephemeral=True)

        try:
            data = json.loads(MU_STATE_FILE.read_text("utf-8"))
        except Exception:
            return await interaction.followup.send("Failed to read MU state file.", ephemeral=True)

        key = str(gid)
        if key in data:
            data.pop(key, None)
            try:
                MU_STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            except Exception:
                return await interaction.followup.send("Failed to write MU state file.", ephemeral=True)
            return await interaction.followup.send(f"Removed MU watcher state for guild {gid}.", ephemeral=True)

        await interaction.followup.send(f"No MU watcher state for guild {gid}.", ephemeral=True)

    # ------------------------
    # /cleanup vacuum
    # ------------------------
    @group.command(
        name="vacuum",
        description="VACUUM the SQLite DB.",
    )
    @app_commands.check(_summer_only)
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
    # Sync helpers
    # ------------------------
    @group.command(name="sync_commands", description="Force global/dev sync.")
    @app_commands.check(_summer_only)
    async def sync_commands(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            dev_gid = os.getenv("DEV_GUILD_ID")
            if dev_gid:
                guild = discord.Object(id=int(dev_gid))
                interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
                synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
                await interaction.followup.send(f"Synced {len(synced)} commands to dev guild {dev_gid}.", ephemeral=True)
            else:
                synced = await interaction.client.tree.sync()  # type: ignore[attr-defined]
                await interaction.followup.send(f"Globally synced {len(synced)} commands.", ephemeral=True)
        except Exception as e:
            log.exception("sync_commands failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    @group.command(name="sync_here", description="Sync to THIS guild now.")
    @app_commands.check(_summer_only)
    async def sync_here(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            gid = int(interaction.guild_id)
            guild = discord.Object(id=gid)
            interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
            synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
            await interaction.followup.send(f"Synced {len(synced)} commands to this guild.", ephemeral=True)
        except Exception as e:
            log.exception("sync_here failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    @group.command(name="sync_guild", description="Sync to a specific guild id.")
    @app_commands.check(_summer_only)
    @app_commands.describe(guild_id="Guild ID to sync")
    async def sync_guild(self, interaction: discord.Interaction, guild_id: int):
        await interaction.response.defer(ephemeral=True)
        try:
            guild = discord.Object(id=int(guild_id))
            interaction.client.tree.copy_global_to(guild=guild)  # type: ignore[attr-defined]
            synced = await interaction.client.tree.sync(guild=guild)  # type: ignore[attr-defined]
            await interaction.followup.send(f"Synced {len(synced)} commands to guild {guild_id}.", ephemeral=True)
        except Exception as e:
            log.exception("sync_guild failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)

    # ------------------------
    # Legacy activity tools
    # ------------------------
    @group.command(name="probe_legacy_activity", description="Find legacy activity tables.")
    @app_commands.check(_summer_only)
    @app_commands.default_permissions(manage_guild=True)
    async def probe_legacy_activity(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        con = connect()
        mon, tot = self._guess_legacy(con)
        con.close()
        lines = []
        if mon[0]:
            lines.append(f"Month candidate: **{mon[0]}** (month={mon[1].get('month')}, count={mon[1].get('count')})")
        else:
            lines.append("Month candidate: (none)")
        if tot[0]:
            lines.append(f"Total candidate: **{tot[0]}** (count={tot[1].get('count')})")
        else:
            lines.append("Total candidate: (none)")
        await interaction.followup.send("\n".join(lines), ephemeral=True)

    @group.command(name="export_legacy_activity", description="Export legacy CSV for a guild.")
    @app_commands.check(_summer_only)
    @app_commands.describe(
        target_guild_id="Guild ID (default: current)",
        month_table="Legacy month table",
        month_col_month="Legacy month column",
        month_col_count="Legacy month count column",
        total_table="Legacy total table",
        total_col_count="Legacy total count column",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def export_legacy_activity(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[int] = None,
        month_table: Optional[str] = None,
        month_col_month: Optional[str] = None,
        month_col_count: Optional[str] = None,
        total_table: Optional[str] = None,
        total_col_count: Optional[str] = None,
    ):
        gid = self._resolve_gid(interaction, target_guild_id)
        await interaction.response.defer(ephemeral=True)

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
                await interaction.followup.send(f"No legacy tables/rows for guild {gid}.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("export_legacy_activity failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)

    @group.command(
        name="migrate_activity_db",
        description="ADD legacy message counts into new tables for a guild.",
    )
    @app_commands.check(_summer_only)
    @app_commands.describe(
        target_guild_id="Guild ID (default: current)",
        src_month_table="Legacy month table",
        src_month_col_month="Legacy month column",
        src_month_col_count="Legacy month count column",
        src_total_table="Legacy total table",
        src_total_col_count="Legacy total count column",
        dry_run="Only report; no writes.",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def migrate_activity_db(
        self,
        interaction: discord.Interaction,
        target_guild_id: Optional[int] = None,
        src_month_table: Optional[str] = None,
        src_month_col_month: Optional[str] = None,
        src_month_col_count: Optional[str] = None,
        src_total_table: Optional[str] = None,
        src_total_col_count: Optional[str] = None,
        dry_run: bool = True,
    ):
        gid = self._resolve_gid(interaction, target_guild_id)
        await interaction.response.defer(ephemeral=True)

        import sqlite3
        try:
            con = connect()
            cur = con.cursor()

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
                    raise RuntimeError(f"{mon_table}: need month/count columns.")

            tot_cmap = {}
            if tot_table:
                tot_cmap = {
                    "guild": "guild_id",
                    "user": "user_id",
                    "count": src_total_col_count or tot_guess[1].get("count"),
                }
                if None in tot_cmap.values():
                    raise RuntimeError(f"{tot_table}: need count column.")

            ops = []

            if mon_table:
                cols = self._table_cols(con, mon_table)
                need = {mon_cmap["guild"], mon_cmap["user"], mon_cmap["month"], mon_cmap["count"]}
                missing = need - cols
                if missing:
                    raise RuntimeError(f"{mon_table} missing columns: {missing}")

                cur.execute(f"SELECT COUNT(*) FROM {mon_table} WHERE {mon_cmap['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{mon_table}: {n_rows} month rows for guild {gid}")

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

            if tot_table:
                cols = self._table_cols(con, tot_table)
                need = {tot_cmap["guild"], tot_cmap["user"], tot_cmap["count"]}
                missing = need - cols
                if missing:
                    raise RuntimeError(f"{tot_table} missing columns: {missing}")

                cur.execute(f"SELECT COUNT(*) FROM {tot_table} WHERE {tot_cmap['guild']}=?", (gid,))
                (n_rows,) = cur.fetchone() or (0,)
                ops.append(f"{tot_table}: {n_rows} total rows for guild {gid}")

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
                    "No legacy tables detected. Use `/cleanup probe_legacy_activity` or pass table names.",
                    ephemeral=True,
                )

            head = "**Dry-run**" if dry_run else "**Migration complete**"
            body = "\n".join(ops) if ops else "(nothing found)"
            await interaction.followup.send(f"{head}\n{body}", ephemeral=True)

        except Exception as e:
            log.exception("cleanup.migrate_activity_db.failed", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send(f"Error: {e}", ephemeral=True)

    @group.command(
        name="purge_legacy_table",
        description="Drop a legacy table or delete a guild’s rows.",
    )
    @app_commands.check(_summer_only)
    @app_commands.describe(
        table="Legacy table name",
        scope="Use 'guild' to delete rows or 'drop' to DROP TABLE",
        target_guild_id="Guild ID for scope=guild (default: current)",
        dry_run="If true, only report; no changes.",
    )
    @app_commands.choices(scope=[
        app_commands.Choice(name="guild", value="guild"),
        app_commands.Choice(name="drop", value="drop"),
    ])
    async def purge_legacy_table(
        self,
        interaction: discord.Interaction,
        table: str,
        scope: app_commands.Choice[str],
        target_guild_id: Optional[int] = None,
        dry_run: bool = True,
    ):
        gid = self._resolve_gid(interaction, target_guild_id)
        await interaction.response.defer(ephemeral=True)

        import sqlite3
        try:
            con = connect()
            cur = con.cursor()

            if not self._has_table(con, table):
                con.close()
                return await interaction.followup.send(f"Table `{table}` does not exist.", ephemeral=True)

            if scope.value == "drop":
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
                return await interaction.followup.send(f"**Dry-run**: would delete {n_rows} row(s) from `{table}` for guild {gid}.", ephemeral=True)

            cur.execute("BEGIN")
            cur.execute(f"DELETE FROM {table} WHERE guild_id=?", (gid,))
            con.commit()
            con.close()
            await interaction.followup.send(f"Deleted {n_rows} row(s) from `{table}` for guild {gid}.", ephemeral=True)

        except sqlite3.Error as e:
            log.exception("purge_legacy_table failed")
            await interaction.followup.send(f"DB error: {e}", ephemeral=True)

    # Unauthorized -> tidy error
    @commands.Cog.listener()
    async def on_app_command_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        if isinstance(error, app_commands.CheckFailure) and not interaction.response.is_done():
            await interaction.response.send_message("You’re not authorized to use this command.", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
