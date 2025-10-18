from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Iterable, Tuple

import discord
from discord.ext import commands
from discord import app_commands

from ..db import connect
from ..strings import S  # optional, only used for "guild only" fallback

log = logging.getLogger(__name__)

MU_STATE_FILE = Path("./data/mu_watch.json")


def _delete_counts(cur, table: str, where_sql: str, params: Tuple) -> int:
    """Return number of rows that WOULD be deleted (approx) and perform deletion."""
    # Rowcount behavior differs by sqlite/python; we’ll do a COUNT first for precise reporting.
    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}", params)
    (n,) = cur.fetchone() or (0,)
    if n:
        cur.execute(f"DELETE FROM {table} WHERE {where_sql}", params)
    return int(n or 0)


def _tables_for_guild_scoped_delete() -> Iterable[Tuple[str, str]]:
    """
    Table list for 'purge this guild' operations.
    Order matters for FKs (we delete children first, then parents).
    (If you didn’t add FKs, order is still safe.)
    """
    return [
        # Children first
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
    """Same as above, but using NOT IN (...) predicate for global pruning."""
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
    # For global prune/VACUUM we require bot owner.
    try:
        return await inter.client.is_owner(inter.user)  # type: ignore[attr-defined]
    except Exception:
        return False


class CleanupCog(commands.Cog):
    """
    Admin maintenance commands for pruning old/stale data.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="cleanup", description="Admin maintenance utilities")

    # ------------------------
    # /cleanup purge_here
    # ------------------------
    @group.command(
        name="purge_here",
        description="Delete all stored data for THIS server from the database (with optional dry-run).",
    )
    @app_commands.describe(
        dry_run="If true, only report counts; do not delete.",
        vacuum="Run VACUUM after deletion (may lock DB for a bit).",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def purge_here(
        self,
        interaction: discord.Interaction,
        dry_run: bool = True,
        vacuum: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if "common.guild_only" in S.__globals__.get("_STRINGS", {}) else "This command can only be used in a server.",
                ephemeral=True,
            )

        await interaction.response.defer(ephemeral=True)
        gid = int(interaction.guild_id)

        # Collect counts first
        deleted_total = 0
        lines = []
        import sqlite3

        try:
            con = connect()
            cur = con.cursor()
            # Wrap in a single tx for speed/atomicity
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
                # VACUUM must be run outside a transaction in SQLite
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
        description="Owner-only: delete rows for guilds the bot is not currently in.",
    )
    @app_commands.describe(
        dry_run="If true, only report counts; do not delete.",
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
                if dry_run:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}")
                    (n,) = cur.fetchone() or (0,)
                else:
                    # Can't use parameterized list in IN; we already string-subbed ids_csv safely from integers.
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {where_sql}")
                    (n,) = cur.fetchone() or (0,)
                    if n:
                        cur.execute(f"DELETE FROM {table} WHERE {where_sql}")
                if n:
                    lines.append(f"• {table}: {n}")
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
            f"**Dry-run**: would delete {deleted_total} rows (keeping {len(live_ids)} guild(s))."
            if dry_run else
            f"Deleted **{deleted_total}** rows (kept {len(live_ids)} guild(s))."
        )
        detail = "\n".join(lines) if lines else "(nothing to prune)"
        await interaction.followup.send(f"{head}\n{detail}", ephemeral=True)

    # ------------------------
    # /cleanup mu_purge_here
    # ------------------------
    @group.command(
        name="mu_purge_here",
        description="Remove MangaUpdates watcher state for THIS server (mu_watch.json).",
    )
    @app_commands.default_permissions(manage_guild=True)
    async def mu_purge_here(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only") if "common.guild_only" in S.__globals__.get("_STRINGS", {}) else "This command can only be used in a server.",
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

        removed = gid in data
        if removed:
            data.pop(gid, None)
            try:
                MU_STATE_FILE.write_text(json.dumps(data, indent=2), encoding="utf-8")
            except Exception:
                return await interaction.followup.send("Failed to write MU state file.", ephemeral=True)

        await interaction.followup.send(
            "Removed MU watcher state for this server." if removed else "No MU watcher state for this server.",
            ephemeral=True,
        )

    # ------------------------
    # /cleanup vacuum
    # ------------------------
    @group.command(
        name="vacuum",
        description="Owner-only: VACUUM the SQLite DB to reclaim space.",
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


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
