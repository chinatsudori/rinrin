from __future__ import annotations

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


# ---------------------------------------------------------------------------
# Checks
# ---------------------------------------------------------------------------
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


# ---------------------------------------------------------------------------
# Cog
# ---------------------------------------------------------------------------
class CleanupCog(commands.Cog):
    """Maintenance and data import/export utilities (owner only)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
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
    # SCHEMA PATCH (messages column & triggers)
    # ----------------------
    @group.command(
        name="patch_metrics_daily_compat",
        description="Add `messages` column to member_metrics_daily and create sync triggers."
    )
    @owner_only()
    async def patch_metrics_daily_compat(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True)
        sql = """
        -- Add a legacy-compatible column so old writes stop crashing
        ALTER TABLE member_metrics_daily ADD COLUMN messages INTEGER;

        -- Backfill once so reads don’t look empty
        UPDATE member_metrics_daily
        SET messages = count
        WHERE messages IS NULL;

        -- Keep both in sync while you transition the code
        CREATE TRIGGER IF NOT EXISTS trg_mmd_sync_to_messages
        AFTER UPDATE OF count ON member_metrics_daily
        FOR EACH ROW
        WHEN NEW.messages IS NOT NEW.count
        BEGIN
          UPDATE member_metrics_daily
          SET messages = NEW.count
          WHERE guild_id = NEW.guild_id
            AND user_id = NEW.user_id
            AND day = NEW.day;
        END;

        CREATE TRIGGER IF NOT EXISTS trg_mmd_sync_to_count
        AFTER UPDATE OF messages ON member_metrics_daily
        FOR EACH ROW
        WHEN NEW.count IS NOT NEW.messages
        BEGIN
          UPDATE member_metrics_daily
          SET count = NEW.messages
          WHERE guild_id = NEW.guild_id
            AND user_id = NEW.user_id
            AND day = NEW.day;
        END;
        """
        try:
            con = connect()
            cur = con.cursor()
            cur.executescript(sql)
            con.commit()
            await interaction.followup.send("Patched `member_metrics_daily`: added `messages` and sync triggers.", ephemeral=True)
        except Exception as e:
            log.exception("patch_metrics_daily_compat.failed")
            await interaction.followup.send(f"Patch failed: {e}", ephemeral=True)
        finally:
            try:
                con.close()
            except Exception:
                pass

    # ----------------------------------------------------------------------
    # NEW: MONTHLY CSV -> DAILY TABLE (YYYY-MM becomes YYYY-MM-01)
    # ----------------------------------------------------------------------
    @group.command(
        name="import_monthly_as_daily",
        description="Import monthly CSV (guild_id,month,user_id,messages) as daily entries on the 1st of each month."
    )
    @app_commands.describe(
        file="CSV attachment with headers: guild_id,month,user_id,messages",
        target_guild_id="Target guild ID (default: current guild)",
        mode="replace=overwrite or add=merge existing data",
        update_monthly_rollup="Also upsert member_activity_monthly for that month (recommended)"
    )
    @app_commands.choices(mode=[
        app_commands.Choice(name="add", value="add"),
        app_commands.Choice(name="replace", value="replace"),
    ])
    @owner_only()
    async def import_monthly_as_daily(
        self,
        interaction: discord.Interaction,
        file: discord.Attachment,
        target_guild_id: str | None = None,
        mode: app_commands.Choice[str] | None = None,
        update_monthly_rollup: bool = True,
    ):
        await interaction.response.defer(ephemeral=True)

        gid = int(target_guild_id or interaction.guild_id or 0)
        if gid <= 0:
            return await interaction.followup.send("Invalid guild ID.", ephemeral=True)

        mode_val = (mode.value if mode else "add")
        debug = self._is_debug(interaction.guild_id)

        # Read CSV
        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        headers = set(reader.fieldnames or [])

        required = {"guild_id", "month", "user_id", "messages"}
        if not required.issubset(headers):
            return await interaction.followup.send(
                "CSV must contain headers: guild_id,month,user_id,messages.",
                ephemeral=True
            )

        # Parse -> list of (gid, uid, day, messages, month)
        rows: List[Tuple[int, int, str, int, str]] = []
        parsed = 0
        for r in reader:
            parsed += 1
            try:
                if str(r["guild_id"]).strip() != str(gid):
                    continue
                m = str(r["month"]).strip()
                # Very light validation: YYYY-MM
                if len(m) != 7 or m[4] != "-":
                    continue
                day = f"{m}-01"
                uid = int(str(r["user_id"]).strip())
                cnt = int(str(r["messages"]).strip())
                if cnt < 0:
                    continue
                rows.append((gid, uid, day, cnt, m))
            except Exception:
                continue

        if not rows:
            return await interaction.followup.send("No valid rows for this guild.", ephemeral=True)

        # Upsert into member_metrics_daily(messages), then refresh totals and monthly rollup
        con = None
        try:
            con = connect()
            cur = con.cursor()
            cur.execute("BEGIN IMMEDIATE")

            # Ensure messages column exists
            try:
                cols = [c[1] for c in cur.execute("PRAGMA table_info(member_metrics_daily)").fetchall()]
                if "messages" not in cols:
                    raise RuntimeError("member_metrics_daily is missing `messages`. Run /cleanup patch_metrics_daily_compat first.")
            except Exception:
                raise

            # Upsert daily entries
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
                [(g, u, d, c, mode_val) for (g, u, d, c, _m) in rows],
            )

            # Refresh totals from metrics_daily
            uids = tuple({u for (_g, u, _d, _c, _m) in rows})
            if uids:
                q = ",".join("?" for _ in uids)
                totals = cur.execute(
                    f"""
                    SELECT user_id, COALESCE(SUM(messages),0)
                    FROM member_metrics_daily
                    WHERE guild_id=? AND user_id IN ({q})
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

            # Optional: update monthly rollup for the same month(s)
            if update_monthly_rollup:
                months = tuple({m for (_g, _u, _d, _c, m) in rows})
                for m in months:
                    # Pull per-user sum for that month (summing messages in metrics_daily over YYYY-MM-%)
                    per_user = cur.execute(
                        """
                        SELECT user_id, COALESCE(SUM(messages),0)
                        FROM member_metrics_daily
                        WHERE guild_id=? AND substr(day,1,7)=?
                        GROUP BY user_id
                        """,
                        (gid, m),
                    ).fetchall()
                    if per_user:
                        cur.executemany(
                            """
                            INSERT INTO member_activity_monthly (guild_id, user_id, month, count)
                            VALUES (?, ?, ?, ?)
                            ON CONFLICT(guild_id, user_id, month)
                            DO UPDATE SET count=excluded.count
                            """,
                            [(gid, uid, m, cnt) for (uid, cnt) in per_user],
                        )

            con.commit()

            if debug:
                await interaction.followup.send(
                    f"Debug: parsed={parsed}, imported={len(rows)}, unique_uids={len(uids)}",
                    ephemeral=True
                )

            return await interaction.followup.send(
                f"Imported **{len(rows)}** row(s) into `member_metrics_daily` (stored on day `YYYY-MM-01`, mode={mode_val}).\n"
                f"Totals {'and monthly rollups ' if update_monthly_rollup else ''}updated.",
                ephemeral=True
            )

        except Exception as e:
            try:
                if con:
                    con.rollback()
            except Exception:
                pass
            log.exception("import_monthly_as_daily.failed", extra={"guild_id": gid})
            return await interaction.followup.send(f"Error: {e}", ephemeral=True)
        finally:
            try:
                if con:
                    con.close()
            except Exception:
                pass

    # ----------------------------------------------------------------------
    # Generic CSV importer (monthly OR metrics_daily) – unchanged behavior
    # ----------------------------------------------------------------------
    @group.command(
        name="import_activity_csv",
        description="Import activity counts from CSV into monthly or daily tables."
    )
    @app_commands.describe(
        file="CSV attachment. Monthly: guild_id,month,user_id,messages. Daily: guild_id,day,user_id,messages",
        target_guild_id="Target guild ID (default: current guild)",
        month="Month in YYYY-MM (required for monthly path; ignored for daily)",
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
        # (Same robust importer you already had; omitted here for brevity in comments)
        # This block is identical to your working version – I’ve kept the logic intact.
        await interaction.response.defer(ephemeral=True)

        gid = int(target_guild_id or interaction.guild_id or 0)
        if gid <= 0:
            return await interaction.followup.send("Invalid guild ID.", ephemeral=True)

        mode_val = (mode.value if mode else "add")
        target_val = (to.value if to else "auto")
        debug = self._is_debug(interaction.guild_id)

        content = await file.read()
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        headers = set(reader.fieldnames or [])

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

        # Path A: member_activity_monthly
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

                # Refresh totals from monthly
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

        # Path B: member_metrics_daily
        if decided_target == "metrics_daily":
            if not {"guild_id", "day", "user_id", "messages"}.issubset(headers):
                return await interaction.followup.send(
                    "metrics_daily import needs CSV headers: guild_id,day,user_id,messages (day must be YYYY-MM-DD).",
                    ephemeral=True
                )

            incoming_rows: List[Tuple[int, int, str, int]] = []
            for row in reader:
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
                cur.execute("BEGIN IMMEDIATE")

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

                con.commit()
                return await interaction.followup.send(
                    f"Import complete (target=member_metrics_daily) for guild `{gid}`.\n"
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

        return await interaction.followup.send("Unknown target.", ephemeral=True)

    # ----------------------
    # PURGE OLD TABLE
    # ----------------------
    @group.command(
        name="purge_old_activity_table",
        description="Drop the legacy `member_activity` table if it still exists."
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
