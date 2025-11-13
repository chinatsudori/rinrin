from __future__ import annotations

import asyncio
import datetime as dt
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import commands

from ..models import activity_metrics as am

# Server opened on this date; default stats window uses days since this date.
OPEN_DATE = dt.date(2025, 9, 16)


class ActivityMetricsCog(commands.Cog):
    """Live metrics updater, purge, stats, and rebuild-from-history with progress + error logging."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        am.ensure_tables()

    async def cog_load(self) -> None:  # discord.py ≥ 2.4
        am.ensure_tables()

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message) -> None:
        try:
            am.upsert_from_message(message, include_bots=False)
        except Exception as e:
            self._log(f"[activity_metrics] upsert error: {e}", error=True)

    def _log(self, msg: str, *, error: bool = False) -> None:
        logger = getattr(self.bot, "logger", None)
        if logger:
            (logger.error if error else logger.info)(msg)
        else:
            print(("[ERR] " if error else "") + msg)

    # ---------------------------
    # /activity_rebuild (non-ephemeral, progress) — uses normal message (no webhook expiry)
    # ---------------------------
    @app_commands.command(
        name="activity_rebuild",
        description="Rebuild live activity metrics from history (text/news/forum/threads).",
    )
    @app_commands.describe(
        days="Look back this many days (default 30).",
        channel="Optionally limit to one text or forum channel.",
        include_bots="Include bot-authored messages.",
    )
    async def activity_rebuild(
        self,
        inter: discord.Interaction,
        days: Optional[int] = 30,
        channel: Optional[discord.abc.GuildChannel] = None,
        include_bots: Optional[bool] = False,
    ) -> None:
        if inter.guild is None:
            await inter.response.send_message("Run this in a server.", ephemeral=True)
            return
        if not inter.user.guild_permissions.manage_guild:
            await inter.response.send_message("You need Manage Server.", ephemeral=True)
            return

        # defer (public)
        await inter.response.defer()

        guild = inter.guild
        since = None
        if days and days > 0:
            since = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=days)

        progress = {
            "phase": "starting",
            "channel_index": 0,
            "channel_total": 0,
            "channel_name": "",
            "msgs_this_channel": 0,
            "msgs_total": 0,
            "errors_count": 0,
            "last_errors": [],  # type: List[str]
        }

        # --- create a NORMAL channel message (bot token), not a followup webhook ---
        progress_msg: discord.Message | None = None
        try:
            progress_msg = await inter.channel.send(  # type: ignore[arg-type]
                "⏳ Preparing activity rebuild…",
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except discord.Forbidden:
            progress_msg = None  # fall back to editing the interaction response

        stop = False
        used_interaction_edit = False

        async def _render_text() -> str:
            phase = progress["phase"]
            ch_i = progress["channel_index"]
            ch_n = progress["channel_total"]
            ch_name = progress["channel_name"]
            msgs_ch = progress["msgs_this_channel"]
            msgs_all = progress["msgs_total"]
            errs = progress["errors_count"]
            last_errs = progress["last_errors"][-3:]

            lines = []
            if phase == "starting":
                lines.append("⏳ Preparing activity rebuild…")
            elif phase == "scanning":
                lines.append(f"🔎 Scanning channels ({ch_i}/{ch_n})")
                lines.append(f"• Current: #{ch_name} — {msgs_ch} msgs")
                lines.append(f"• Total processed: {msgs_all}")
            elif phase == "error":
                lines.append("❌ Rebuild encountered an error. See logs.")
            elif phase == "done":
                lines.append("✅ Activity metrics rebuild complete.")
                lines.append(f"• Channels: {ch_n}")
                lines.append(f"• Messages processed: {msgs_all}")

            if errs:
                lines.append(f"⚠️ Errors so far: {errs}")
                if last_errs:
                    lines.append("Latest:")
                    for e in last_errs:
                        snippet = e if len(e) < 160 else (e[:157] + "…")
                        lines.append(f"  • {snippet}")
            return "\n".join(lines)

        async def render_progress_once():
            text = await _render_text()
            try:
                if progress_msg is not None:
                    await progress_msg.edit(
                        content=text, allowed_mentions=discord.AllowedMentions.none()
                    )
                else:
                    nonlocal used_interaction_edit
                    used_interaction_edit = True
                    await inter.edit_original_response(content=text)
            except discord.HTTPException as e:
                # 50027 = Invalid Webhook Token (interaction token expired)
                if getattr(e, "code", None) == 50027 or "Invalid Webhook Token" in str(
                    e
                ):
                    self._log(
                        "[activity_rebuild] progress update switched off: webhook token expired"
                    )
                    raise
                self._log(f"[activity_rebuild] progress update failed: {e}", error=True)

        async def progress_loop():
            while not stop:
                try:
                    await render_progress_once()
                except Exception:
                    if progress_msg is None:
                        break
                await asyncio.sleep(1.5)

        progress_task = asyncio.create_task(progress_loop())

        # ---- channel enumeration (includes text “inside” voice, news, forum) ----
        def _list_channels() -> list[discord.abc.GuildChannel]:
            if channel:
                return [channel]
            chans: list[discord.abc.GuildChannel] = []
            for ch in guild.channels:
                if isinstance(ch, discord.TextChannel):
                    chans.append(ch)  # includes voice-attached text areas
            chans.extend(getattr(guild, "news_channels", []))
            chans.extend(getattr(guild, "forums", []))
            return chans

        channels = _list_channels()
        progress["channel_total"] = len(channels)

        async def _scan_textlike(ch: discord.TextChannel):
            if not ch.permissions_for(guild.me).read_message_history:
                msg_ = f"Skipping #{ch.name}: missing Read Message History"
                progress["last_errors"].append(msg_)
                progress["errors_count"] += 1
                self._log(f"[activity_rebuild] {msg_}")
                return

            progress["channel_name"] = ch.name
            progress["msgs_this_channel"] = 0

            async for m in ch.history(limit=None, oldest_first=True, after=since):

                def _work():
                    try:
                        am.upsert_from_message(m, include_bots=bool(include_bots))
                    except Exception as e:
                        err = f"{ch.name} / msg {m.id}: {e}"
                        progress["last_errors"].append(err)
                        if len(progress["last_errors"]) > 1000:
                            del progress["last_errors"][:500]
                        progress["errors_count"] += 1
                        self._log(f"[activity_rebuild] {err}", error=True)

                await asyncio.to_thread(_work)
                progress["msgs_this_channel"] += 1
                progress["msgs_total"] += 1
                if (progress["msgs_this_channel"] % 250) == 0:
                    await asyncio.sleep(0)

            # Threads under this channel (active + archived)
            try:
                for th in ch.threads:
                    await _scan_thread(th)
                async for th in ch.archived_threads(limit=None):
                    await _scan_thread(th)
            except Exception:
                pass

        async def _scan_forum(forum: discord.ForumChannel):
            progress["channel_name"] = forum.name
            progress["msgs_this_channel"] = 0
            for th in forum.threads:
                await _scan_thread(th)
            async for th in forum.archived_threads(limit=None):
                await _scan_thread(th)

        async def _scan_thread(th: discord.Thread):
            progress["channel_name"] = (
                f"{th.parent.name} → {th.name}" if th.parent else th.name
            )
            async for m in th.history(limit=None, oldest_first=True, after=since):

                def _work():
                    try:
                        am.upsert_from_message(m, include_bots=bool(include_bots))
                    except Exception as e:
                        err = f"{th.name} / msg {m.id}: {e}"
                        progress["last_errors"].append(err)
                        if len(progress["last_errors"]) > 1000:
                            del progress["last_errors"][:500]
                        progress["errors_count"] += 1
                        self._log(f"[activity_rebuild] {err}", error=True)

                await asyncio.to_thread(_work)
                progress["msgs_this_channel"] += 1
                progress["msgs_total"] += 1
                if (progress["msgs_this_channel"] % 250) == 0:
                    await asyncio.sleep(0)

        try:
            progress["phase"] = "scanning"
            for idx, ch in enumerate(channels, start=1):
                progress["channel_index"] = idx
                if isinstance(ch, discord.StageChannel):
                    continue
                if isinstance(ch, discord.TextChannel):
                    await _scan_textlike(ch)
                elif isinstance(ch, discord.ForumChannel):
                    await _scan_forum(ch)
            progress["phase"] = "done"
        except Exception as e:
            progress["phase"] = "error"
            progress["last_errors"].append(f"Top-level rebuild error: {e}")
            progress["errors_count"] += 1
            self._log(f"[activity_rebuild] Top-level rebuild error: {e}", error=True)
        finally:
            stop = True
            try:
                await progress_task
            except Exception:
                pass
            # Final render
            text = await _render_text()
            try:
                if progress_msg is not None:
                    await progress_msg.edit(
                        content=text, allowed_mentions=discord.AllowedMentions.none()
                    )
                else:
                    if not used_interaction_edit:
                        await inter.edit_original_response(content=text)
            except Exception:
                pass

    # ---------------------------
    # /activity_stats (default to days since OPEN_DATE; optional public post)
    # ---------------------------
    @app_commands.command(
        name="activity_stats",
        description="Show totals captured (overall + by channel) for a window.",
    )
    @app_commands.describe(
        days="Look back this many days. Default is days since 2025-09-16 (server open).",
        post="Post result publicly in this channel (default: false).",
    )
    async def activity_stats(
        self,
        inter: discord.Interaction,
        days: Optional[int] = None,
        post: Optional[bool] = False,
    ) -> None:
        if inter.guild is None:
            await inter.response.send_message("Run this in a server.", ephemeral=True)
            return

        # Default window = days since OPEN_DATE
        now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)
        default_days = max((now.date() - OPEN_DATE).days, 1)
        window_days = days if (days and days > 0) else default_days

        # Defer according to 'post'
        if post:
            await inter.response.defer()  # public
        else:
            await inter.response.defer(ephemeral=True)

        start_day = (now.date() - dt.timedelta(days=window_days)).isoformat()
        end_day = now.date().isoformat()

        totals = am.get_totals(inter.guild.id, start_day, end_day)
        by_channel = am.get_totals_by_channel(inter.guild.id, start_day, end_day)

        # format
        lines = []
        lines.append(f"**Activity totals (last {window_days} days)**")
        lines.append(f"- Messages: {totals['messages']:,}")
        lines.append(f"- Words: {totals['words']:,}")
        lines.append("")
        lines.append("**Top channels (by messages)**")
        if not by_channel:
            lines.append(
                "_No per-channel data yet. Run `/activity_rebuild` to backfill._"
            )
        else:
            top = sorted(by_channel.items(), key=lambda kv: kv[1], reverse=True)[:15]
            for cid, count in top:
                mention = f"<#{cid}>"
                lines.append(f"- {mention}: {count:,}")

        content = "\n".join(lines)
        await inter.edit_original_response(
            content=content,
            allowed_mentions=discord.AllowedMentions.none(),
        )

    # ---------------------------
    # /activity_purge (drop rows; scoped or full; guarded)
    # ---------------------------
    @app_commands.command(
        name="activity_purge",
        description="PURGE captured metrics for this server. Use with care.",
    )
    @app_commands.describe(
        days="If set, purge only the last N days; omit to purge ALL data.",
        include_index="Also clear dedupe index (message_index). Default: true.",
        really="Safety flag. Must be true to actually purge.",
        post="Post the result publicly (default: false).",
    )
    async def activity_purge(
        self,
        inter: discord.Interaction,
        days: Optional[int] = None,
        include_index: Optional[bool] = True,
        really: Optional[bool] = False,
        post: Optional[bool] = False,
    ) -> None:
        if inter.guild is None:
            await inter.response.send_message("Run this in a server.", ephemeral=True)
            return
        if not inter.user.guild_permissions.manage_guild:
            await inter.response.send_message("You need Manage Server.", ephemeral=True)
            return

        scope_str = f"last {days} day(s)" if days and days > 0 else "ALL data"
        if not really:
            await inter.response.send_message(
                f"⚠️ This will purge **{scope_str}** of activity metrics for this server."
                f"\nRe-run with `really=true` to confirm."
                f"\nUse `include_index=true` to also clear the dedupe table.",
                ephemeral=True,
            )
            return

        if post:
            await inter.response.defer()
        else:
            await inter.response.defer(ephemeral=True)

        gid = int(inter.guild.id)
        now = dt.datetime.utcnow().replace(minute=0, second=0, microsecond=0)

        start_day: Optional[str] = None
        end_day: Optional[str] = None
        if days and days > 0:
            start_day = (now.date() - dt.timedelta(days=days)).isoformat()
            end_day = now.date().isoformat()

        def _purge() -> int:
            con = am.connect()
            try:
                cur = con.cursor()
                before = con.total_changes
                # Daily tables
                daily_tables = [
                    "message_metrics_daily",
                    "message_metrics_channel_daily",
                    "reaction_hist_daily",
                    "latency_hist_daily",
                    "user_token_daily",
                    "sentiment_daily",
                ]
                if start_day and end_day:
                    for t in daily_tables:
                        cur.execute(
                            f"DELETE FROM {t} WHERE guild_id=? AND day BETWEEN ? AND ?",
                            (gid, start_day, end_day),
                        )
                else:
                    for t in daily_tables:
                        cur.execute(
                            f"DELETE FROM {t} WHERE guild_id=?",
                            (gid,),
                        )

                # Hourly — stored as 'YYYY-MM-DDTHH' → compare by day prefix
                if start_day and end_day:
                    cur.execute(
                        "DELETE FROM message_metrics_hourly "
                        "WHERE guild_id=? AND substr(hour,1,10) BETWEEN ? AND ?",
                        (gid, start_day, end_day),
                    )
                else:
                    cur.execute(
                        "DELETE FROM message_metrics_hourly WHERE guild_id=?",
                        (gid,),
                    )

                # Channel last msg watermark
                cur.execute("DELETE FROM channel_last_msg WHERE guild_id=?", (gid,))

                # Optional dedupe index
                if include_index:
                    cur.execute("DELETE FROM message_index WHERE guild_id=?", (gid,))

                con.commit()
                after = con.total_changes
                return max(0, after - before)
            finally:
                try:
                    con.close()
                except Exception:
                    pass

        try:
            deleted = await asyncio.to_thread(_purge)
            msg = (
                f"🧹 Purge complete.\n"
                f"• Scope: **{scope_str}**\n"
                f"• Guild: `{gid}`\n"
                f"• Rows deleted (approx): **{deleted}**\n"
                f"{'• Dedupe index cleared.' if include_index else ''}"
            )
            await inter.edit_original_response(
                content=msg,
                allowed_mentions=discord.AllowedMentions.none(),
            )
        except Exception as e:
            self._log(f"[activity_purge] error: {e}", error=True)
            await inter.edit_original_response(
                content=f"❌ Purge failed: {e}",
                allowed_mentions=discord.AllowedMentions.none(),
            )


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityMetricsCog(bot))
