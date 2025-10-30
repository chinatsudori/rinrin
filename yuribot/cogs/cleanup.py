import csv
import io
import logging
import sqlite3
from typing import Dict, Tuple, List

import discord
from discord import app_commands
from discord.ext import commands

from ..db import connect
from .. import models  # noqa: F401

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
    """Maintenance and data import/export utilities with a debug toggle."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # per-guild debug switch
        self._debug_flags: Dict[int, bool] = {}

    group = app_commands.Group(
        name="cleanup",
        description="Maintenance and import tools."
    )

    # ----------------------
    # DEBUG TOGGLE
    # ----------------------
    @group.command(name="debug", description="Toggle verbose debug for imports (guild-scoped).")
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
    # IMPORT ACTIVITY CSV
    # ----------------------
    @group.command(
        name="import_activity_csv",
        description="Import activity counts from a CSV file into the leaderboard's backing tables."
    )
    @app_commands.describe(
        file="CSV attachment. For monthly: guild_id,month,user_id,messages. For metrics_daily: guild_id,day,user_id,messages",
        target_guild_id="Target guild ID (default: current guild)",
        month="Month in YYYY-MM (required for monthly path; ignored for metrics_daily)",
        mode="replace=overwrite or add=merge existing data",
        to="Where to write: auto, activity_monthly, or metrics_daily",
        dry_run="If true, validate and show what would change without writing"
    )
    @app_commands.choices(mode=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="replace", value="replace"),
    ])
    @app_commands.choices(to=[
        app_commands.Choice(name="auto", value="auto"),
        app_commands.Choice(name="activity_monthly", value="activity_monthly"),
        app_commands.Choice(name="metrics_daily", value="metrics_daily"),
    ])
    @owner_only()
    async def import_activity_csv(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        target_guild_id: str | None = None,
        month: str | None = None,
        mode: app_commands.Choice[str] | None = None,
        to: app_commands.Choice[str] | None = None,
        dry_run: bool = False,
    ):
        await interaction.response.defer(ephemeral=True)

        gid = int(target_guild_id or interaction.guild_id or 0)
        if gid <= 0:
            return await interaction.followup.send("Invalid guild ID.", ephemeral=True)

        mode_val = (mode.value if mode else "add")
        target_val = (to.value if to else "auto")
        debug = self._is_debug(interaction.guild_id)

        # Read CSV
        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        headers = set(reader.fieldnames or [])

        # Decide target if auto
        decided_target = target_val
        if target_val == "auto":
            if {"guild_id", "month", "user_id", "messages"}.issubset(headers):
                decided_target = "activity_monthly"
            elif {"guild_id", "day", "user_id", "messages"}.issubset(headers):
                decided_target = "metrics_daily"
            else:
                return await interaction.followup.send(
                    "CSV headers must match either monthly (guild_id,month,user_id,messages) "
                    "or metrics_daily (guild_id,day,user_id,messages).",
                    ephemeral=True
                )

        # ----------------------
        # PATH A: member_activity_monthly
        # ----------------------
        if decided_target == "activity_monthly":
            if not {"guild_id", "month", "user_id", "messages"}.issubset(headers):
                return await interaction.followup.send(
                    "Monthly import requires CSV headers: guild_id,month,user_id,messages.",
                    ephemeral=True
                )
            if not month or not (len(month) == 7 and month[4] == "-"):
                return await interaction.followup.send("Invalid month format. Use YYYY-MM.", ephemeral=True)

            incoming: Dict[int, int] = {}
            parsed_rows = 0
            for row in reader:
                parsed_rows += 1
                try:
                    if str(row["guild_id"]).strip() != str(gid):
                        continue
                    if str(row["month"]).strip() != month:
                        continue
                    uid = int(str(row["user_id"]).strip())
                    cnt = int(str(row["messages"]).strip())
                    if cnt < 0:
                        continue
                    incoming[uid] = incoming.get(uid, 0) + cnt
                except Exception:
                    continue

            if not incoming:
                return await interaction.followup.send("No rows matched the target guild/month.", ephemeral=True)

            con = None
            try:
                con = connect()
                cur = con.cursor()

                before_month_sum = 0
                if debug:
                    try:
                        cols = cur.execute("PRAGMA table_info(member_activity_monthly)").fetchall()
                        pk_cols = [(c[1], c[5]) for c in cols]
                        pk_cols_sorted = [name for (name, pk) in sorted(pk_cols, key=lambda t: t[1]) if pk]
                        before_month_sum = cur.execute(
                            "SELECT COALESCE(SUM(count),0) FROM member_activity_monthly WHERE guild_id=? AND month=?",
                            (gid, month)
                        ).fetchone()[0]
                        await interaction.followup.send(
                            "Debug import diagnostics:\n```\n"
                            f"PK(member_activity_monthly) = {pk_cols_sorted or 'UNKNOWN'}\n"
                            f"Before month sum = {before_month_sum}\n"
                            f"Incoming unique users = {len(incoming)}\n"
                            f"Parsed CSV rows = {parsed_rows}\n"
                            f"Sample = {list(incoming.items())[:3]}\n"
                            f"Target = member_activity_monthly\n"
                            "```",
                            ephemeral=True
                        )
                    except Exception:
                        pass

                cur.execute("BEGIN IMMEDIATE")

                if mode_val == "replace" and not dry_run:
                    cur.execute(
                        "DELETE FROM member_activity_monthly WHERE guild_id=? AND month=?",
                        (gid, month)
                    )

                rows = [(gid, uid, month, cnt, mode_val) for uid, cnt in incoming.items()]
                if not dry_run:
                    cur.executemany(
                        """
                        INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(guild_id, user_id, month)
                        DO UPDATE SET count =
                            CASE WHEN ?='add'
                                 THEN member_activity_monthly.count + excluded.count
                                 ELSE excluded.count
                            END
                        """,
                        rows,
                    )

                # Refresh totals for affected users (from monthly)
                uids = tuple(incoming.keys())
                totals: List[Tuple[int, int]] = []
                if uids:
                    q_marks = ",".join("?" for _ in uids)
                    sql = (
                        "SELECT user_id, COALESCE(SUM(count),0) "
                        "FROM member_activity_monthly "
                        f"WHERE guild_id=? AND user_id IN ({q_marks}) "
                        "GROUP BY user_id"
                    )
                    totals = cur.execute(sql, (gid, *uids)).fetchall()

                if not dry_run and totals:
                    cur.executemany(
                        """
                        INSERT INTO member_activity_total (guild_id, user_id, count)
                        VALUES (?, ?, ?)
                        ON CONFLICT(guild_id, user_id)
                        DO UPDATE SET count = excluded.count
                        """,
                        [(gid, uid, total) for (uid, total) in totals],
                    )

                if dry_run:
                    con.rollback()
                else:
                    con.commit()

                if debug:
                    try:
                        after_month_sum = cur.execute(
                            "SELECT COALESCE(SUM(count),0) FROM member_activity_monthly WHERE guild_id=? AND month=?",
                            (gid, month)
                        ).fetchone()[0]
                        delta = after_month_sum - before_month_sum
                        any_uid = next(iter(incoming.keys()))
                        now_row = cur.execute(
                            "SELECT COALESCE(count,0) FROM member_activity_monthly WHERE guild_id=? AND user_id=? AND month=?",
                            (gid, any_uid, month)
                        ).fetchone()
                        now_val = int(now_row[0]) if now_row else 0
                        await interaction.followup.send(
                            "Debug import diagnostics (post):\n```\n"
                            f"After month sum = {after_month_sum}\n"
                            f"Delta = {delta}\n"
                            f"Sample user {any_uid} month count = {now_val}\n"
                            f"Mode = {mode_val}; Dry-run = {dry_run}\n"
                            "```",
                            ephemeral=True
                        )
                    except Exception:
                        pass

                total_msgs = sum(incoming.values())
                return await interaction.followup.send(
                    f"{'DRY RUN - ' if dry_run else ''}Import complete (target=member_activity_monthly) for guild `{gid}`, month `{month}`.\n"
                    f"- rows imported: {len(incoming)} (unique users)\n"
                    f"- total messages in file: {total_msgs}\n"
                    f"- mode: {mode_val}",
                    ephemeral=True
                )

            except sqlite3.Error as e:
                try:
                    if con:
                        con.rollback()
                except Exception:
                    pass
                log.exception("import_activity_csv.db_error", extra={"guild_id": gid})
                return await interaction.followup.send(f"Database error: {e}", ephemeral=True)
            except Exception as e:
                try:
                    if con:
                        con.rollback()
                except Exception:
                    pass
                log.exception("import_activity_csv.failed", extra={"guild_id": gid})
                return await interaction.followup.send(f"Error: {e}", ephemeral=True)
            finally:
                try:
                    if con:
                        con.close()
                except Exception:
                    pass

        # ----------------------
        # PATH B: member_metrics_daily
        # ----------------------
        if decided_target == "metrics_daily":
            if not {"guild_id", "day", "user_id", "messages"}.issubset(headers):
                return await interaction.followup.send(
                    "metrics_daily import needs CSV headers: guild_id,day,user_id,messages (day must be YYYY-MM-DD).",
                    ephemeral=True
                )

            incoming_rows: List[Tuple[int, int, str, int]] = []
            parsed_rows = 0
            for row in reader:
                parsed_rows += 1
                try:
                    if str(row["guild_id"]).strip() != str(gid):
                        continue
                    day = str(row["day"]).strip()
                    if len(day) != 10 or day[4] != "-" or day[7] != "-":
                        continue
                    uid = int(str(row["user_id"]).strip())
                    cnt = int(str(row["messages"]).strip())
                    if cnt < 0:
                        continue
                    incoming_rows.append((gid, uid, day, cnt))
                except Exception:
                    continue

            if not incoming_rows:
                return await interaction.followup.send("No valid rows for metrics_daily.", ephemeral=True)

            con = None
            try:
                con = connect()
                cur = con.cursor()

                if debug:
                    await interaction.followup.send(
                        "Debug import diagnostics:\n```\n"
                        "Target = member_metrics_daily\n"
                        f"Parsed rows = {parsed_rows}; will upsert = {len(incoming_rows)}\n"
                        f"Sample = {incoming_rows[:3]}\n"
                        "```",
                        ephemeral=True
                    )

                cur.execute("BEGIN IMMEDIATE")

                if not dry_run:
                    # PK assumed: (guild_id, user_id, day)
                    cur.executemany(
                        """
                        INSERT INTO member_metrics_daily (guild_id, user_id, day, messages)
                        VALUES (?, ?, ?, ?)
                        ON CONFLICT(guild_id, user_id, day)
                        DO UPDATE SET messages =
                            CASE WHEN ?='add'
                                 THEN member_metrics_daily.messages + excluded.messages
                                 ELSE excluded.messages
                            END
                        """,
                        [(gid, uid, day, cnt, mode_val) for (gid, uid, day, cnt) in incoming_rows],
                    )

                # Refresh totals from metrics_daily
                if not dry_run:
                    uids = tuple({uid for (_gid, uid, _day, _cnt) in incoming_rows})
                    if uids:
                        q_marks = ",".join("?" for _ in uids)
                        totals = cur.execute(
                            f"""
                            SELECT user_id, COALESCE(SUM(messages),0)
                            FROM member_metrics_daily
                            WHERE guild_id=? AND user_id IN ({q_marks})
                            GROUP BY user_id
                            """,
                            (gid, *uids),
                        ).fetchall()
                        cur.executemany(
                            """
                            INSERT INTO member_activity_total (guild_id, user_id, count)
                            VALUES (?, ?, ?)
                            ON CONFLICT(guild_id, user_id)
                            DO UPDATE SET count = excluded.count
                            """,
                            [(gid, uid, total) for (uid, total) in totals],
                        )

                if dry_run:
                    con.rollback()
                else:
                    con.commit()

                return await interaction.followup.send(
                    f"{'DRY RUN - ' if dry_run else ''}Import complete (target=member_metrics_daily) for guild `{gid}`.\n"
                    f"- rows imported: {len(incoming_rows)}\n"
                    f"- mode: {mode_val}",
                    ephemeral=True
                )

            except sqlite3.Error as e:
                try:
                    if con:
                        con.rollback()
                except Exception:
                    pass
                log.exception("import_activity_csv.db_error", extra={"guild_id": gid})
                return await interaction.followup.send(f"Database error: {e}", ephemeral=True)
            except Exception as e:
                try:
                    if con:
                        con.rollback()
                except Exception:
                    pass
                log.exception("import_activity_csv.failed", extra={"guild_id": gid})
                return await interaction.followup.send(f"Error: {e}", ephemeral=True)
            finally:
                try:
                    if con:
                        con.close()
                except Exception:
                    pass

        # Safety: unknown target (shouldn't happen)
        return await interaction.followup.send("Unknown target.", ephemeral=True)

    # ----------------------
    # PURGE OLD TABLE
    # ----------------------
    @group.command(
        name="purge_old_activity_table",
        description="Drop the legacy activity table if it still exists."
    )
    @owner_only()
    async def purge_old_activity_table(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            con = connect()
            cur = con.cursor()
            cur.execute("DROP TABLE IF EXISTS member_activity;")
            con.commit()
            await interaction.followup.send("Old `member_activity` table dropped.", ephemeral=True)
        except Exception as e:
            log.exception("purge_old_activity_table.failed")
            await interaction.followup.send(f"Error: {e}", ephemeral=True)
        finally:
            try:
                con.close()
            except Exception:
                pass

    # ----------------------
    # FORCE SYNC COMMANDS
    # ----------------------
    @group.command(name="force_sync", description="Force re-sync of all slash commands.")
    @owner_only()
    async def force_sync(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        try:
            synced = await self.bot.tree.sync()
            await interaction.followup.send(
                f"Force-synced {len(synced)} command(s).", ephemeral=True
            )
        except Exception as e:
            log.exception("force_sync.failed")
            await interaction.followup.send(f"Sync failed: {e}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(CleanupCog(bot))
