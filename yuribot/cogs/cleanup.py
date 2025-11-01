from __future__ import annotations

import csv
import io
import logging
import sqlite3
from typing import Dict, List, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect

log = logging.getLogger(__name__)


def owner_only():
    async def predicate(interaction: discord.Interaction):
        app_owner = (
            interaction.client.application.owner
            if hasattr(interaction.client.application, "owner")
            else None
        )
        if interaction.user.id == getattr(app_owner, "id", None):
            return True
        await interaction.response.send_message("Owner only.", ephemeral=True)
        return False
    return app_commands.check(predicate)


class CleanupCog(commands.Cog):
    """Maintenance and data import/export utilities (guild-scoped)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._debug_flags: Dict[int, bool] = {}

    group = app_commands.Group(
        name="cleanup",
        description="Maintenance and data tools."
    )

    # ----------------------
    # DEBUG TOGGLE
    # ----------------------
    @group.command(name="debug", description="Toggle verbose debug for cleanup operations (guild-scoped).")
    @app_commands.describe(state="on/off")
    @app_commands.choices(state=[
        app_commands.Choice(name="on", value="on"),
        app_commands.Choice(name="off", value="off"),
    ])
    @owner_only()
    async def toggle_debug(self, interaction: discord.Interaction, state: app_commands.Choice[str]):
        if not interaction.guild_id:
            return await interaction.response.send_message("Guild only.", ephemeral=True)
        on = (state.value == "on")
        self._debug_flags[int(interaction.guild_id)] = on
        await interaction.response.send_message(
            f"Cleanup debug is now {'ON' if on else 'OFF'} for this server.",
            ephemeral=True
        )

    def _is_debug(self, guild_id: int | None) -> bool:
        return bool(guild_id and self._debug_flags.get(int(guild_id), False))

    # ----------------------
    # NUKE SWITCH — purge all activity rows for this guild
    # ----------------------
    @group.command(
        name="purge_activity_data",
        description="HARD RESET: remove all rows for this guild from every table that contains a guild_id column."
    )
    @app_commands.describe(
        really_type_it="Type the exact word: YES",
        dry_run="If true, show what would be deleted but do not write.",
        export_report="Attach a CSV of table-by-table delete counts."
    )
    @owner_only()
    async def purge_activity_data(
        self,
        interaction: discord.Interaction,
        really_type_it: str,
        dry_run: bool = True,
        export_report: bool = True,
    ):
        """
        Strategy:
          1) Discover all user tables from sqlite_master (type='table', not sqlite_*, not internal).
          2) Keep tables that have a 'guild_id' column (PRAGMA table_info).
          3) DELETE FROM <table> WHERE guild_id = this_guild in one transaction.
          4) Count affected rows via changes() delta.
        Safe for schema evolution; no hard-coded table names.
        """
        if not interaction.guild_id:
            return await interaction.response.send_message("Guild only.", ephemeral=True)

        if really_type_it.strip() != "YES":
            return await interaction.response.send_message(
                "Confirmation failed. Type **YES** to proceed.",
                ephemeral=True
            )

        gid = int(interaction.guild_id)
        await interaction.response.defer(ephemeral=True)

        con: sqlite3.Connection | None = None
        try:
            con = connect()
            con.isolation_level = None  # we'll manage BEGIN/COMMIT manually
            cur = con.cursor()

            # 1) discover candidate tables
            rows = cur.execute(
                """
                SELECT name
                FROM sqlite_master
                WHERE type='table'
                  AND name NOT LIKE 'sqlite_%'
                  AND name NOT LIKE 'pragma_%'
                ORDER BY name
                """
            ).fetchall()
            table_names: List[str] = [r[0] for r in rows]

            # 2) filter to tables with a guild_id column
            target_tables: List[str] = []
            for t in table_names:
                try:
                    cols = cur.execute(f"PRAGMA table_info({t})").fetchall()
                    col_names = {c[1] for c in cols}  # (cid, name, type, notnull, dflt, pk)
                    if "guild_id" in col_names:
                        target_tables.append(t)
                except Exception:
                    # don't die if a PRAGMA fails on some virtual table
                    continue

            if not target_tables:
                return await interaction.followup.send(
                    "No tables with a `guild_id` column were found. Nothing to purge.",
                    ephemeral=True
                )

            debug = self._is_debug(gid)
            if debug:
                await interaction.followup.send(
                    "Purge diagnostics:\n```\n"
                    f"Guild: {gid}\n"
                    f"Candidate tables: {len(table_names)}\n"
                    f"Target tables with guild_id: {len(target_tables)}\n"
                    f"{target_tables}\n"
                    f"Dry-run: {dry_run}\n"
                    "```",
                    ephemeral=True
                )

            # 3) nuke in one transaction, counting rows
            report: List[Tuple[str, int]] = []
            cur.execute("BEGIN IMMEDIATE")
            try:
                for t in target_tables:
                    # Count before
                    before = cur.execute(
                        f"SELECT COUNT(1) FROM {t} WHERE guild_id=?",
                        (gid,)
                    ).fetchone()[0]

                    deleted = 0
                    if before > 0 and not dry_run:
                        cur.execute(f"DELETE FROM {t} WHERE guild_id=?", (gid,))
                        # sqlite doesn't give per statement rowcount reliably; trust 'before'
                        deleted = before
                    else:
                        deleted = before  # what would be deleted

                    report.append((t, int(deleted)))

                if dry_run:
                    cur.execute("ROLLBACK")
                else:
                    cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise

            # 4) respond with summary (and optional CSV)
            total = sum(d for _, d in report)
            lines = [f"- {t}: {d}" for (t, d) in report if d > 0]
            details = "(none)" if not lines else "Details:\n" + "\n".join(lines)

            msg = (
                f"{'DRY RUN — ' if dry_run else ''}Purged activity rows for guild `{gid}`.\n"
                f"Tables touched: {len(report)}\n"
                f"Total rows {'to be ' if dry_run else ''}deleted: **{total}**\n"
                f"{details}"
            )


            # Optional CSV export
            if export_report:
                buf = io.StringIO()
                w = csv.writer(buf)
                w.writerow(["table", "rows_deleted"])
                w.writerows(report)
                data = buf.getvalue().encode("utf-8")
                file = discord.File(io.BytesIO(data), filename=f"purge-report-{gid}.csv")
                await interaction.followup.send(content=msg, file=file, ephemeral=True)
            else:
                await interaction.followup.send(content=msg, ephemeral=True)

            log.warning(
                "cleanup.purge_activity_data",
                extra={
                    "guild_id": gid,
                    "dry_run": dry_run,
                    "tables": len(report),
                    "rows": total
                },
            )

        except sqlite3.Error as e:
            log.exception("purge_activity_data.db_error", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send(f"Database error: {e}", ephemeral=True)
        except Exception as e:
            log.exception("purge_activity_data.failed", extra={"guild_id": interaction.guild_id})
            await interaction.followup.send(f"Error: {e}", ephemeral=True)
        finally:
            try:
                if con:
                    con.close()
            except Exception:
                pass


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
