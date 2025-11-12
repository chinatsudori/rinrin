from __future__ import annotations

import asyncio
import datetime as dt
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import commands

from ..models import activity_metrics as am


class ActivityMetricsCog(commands.Cog):
    """Live metrics updater & on-demand rebuild from history with progress + error logging."""

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

    @app_commands.command(
        name="activity_rebuild",
        description="Rebuild live activity metrics from history.",
    )
    @app_commands.describe(
        days="Look back this many days (default 30).",
        channel="Optionally limit to one text channel.",
        include_bots="Include bot-authored messages.",
    )
    async def activity_rebuild(
        self,
        inter: discord.Interaction,
        days: Optional[int] = 30,
        channel: Optional[discord.TextChannel] = None,
        include_bots: Optional[bool] = False,
    ) -> None:
        if inter.guild is None:
            await inter.response.send_message("Run this in a server.", ephemeral=True)
            return
        if not inter.user.guild_permissions.manage_guild:
            await inter.response.send_message("You need Manage Server.", ephemeral=True)
            return

        await inter.response.defer(ephemeral=True)

        guild = inter.guild
        since = None
        if days and days > 0:
            since = dt.datetime.now(tz=dt.timezone.utc) - dt.timedelta(days=days)

        # ---- Progress state shared with progress loop ----
        progress = {
            "phase": "starting",  # "starting" | "scanning" | "done" | "error"
            "channel_index": 0,
            "channel_total": 0,
            "channel_name": "",
            "msgs_this_channel": 0,
            "msgs_total": 0,
            "errors_count": 0,
            "last_errors": [],  # type: List[str]
        }
        stop_event = asyncio.Event()

        async def render_progress() -> None:
            """Edits the original ephemeral response every ~1.5s with live counters."""
            while not stop_event.is_set():
                try:
                    phase = progress["phase"]
                    ch_i = progress["channel_index"]
                    ch_n = progress["channel_total"]
                    ch_name = progress["channel_name"]
                    msgs_ch = progress["msgs_this_channel"]
                    msgs_all = progress["msgs_total"]
                    errs = progress["errors_count"]
                    last_errs = progress["last_errors"][-3:]  # show last 3

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
                                # keep it brief to avoid hitting message length limits
                                snippet = e if len(e) < 160 else (e[:157] + "…")
                                lines.append(f"  • {snippet}")

                    await inter.edit_original_response(content="\n".join(lines))
                except Exception as e:
                    # don't crash the updater on transient edit errors
                    self._log(
                        f"[activity_rebuild] progress update failed: {e}", error=True
                    )
                await asyncio.sleep(1.5)

        # Start progress loop
        progress["phase"] = "starting"
        progress_task = asyncio.create_task(render_progress())

        # Prepare channel list and set totals
        if channel:
            channels = [channel]
        else:
            channels = [c for c in guild.text_channels]
            channels.sort(
                key=lambda c: (
                    c.category.position if c.category else -1,
                    c.position,
                    c.id,
                )
            )
        progress["channel_total"] = len(channels)

        async def scan_channel(ch: discord.TextChannel):
            if not ch.permissions_for(guild.me).read_message_history:
                msg = f"Skipping #{ch.name}: missing Read Message History"
                self._log(f"[activity_rebuild] {msg}")
                progress["last_errors"].append(msg)
                progress["errors_count"] += 1
                return

            progress["channel_name"] = ch.name
            progress["msgs_this_channel"] = 0

            async for m in ch.history(limit=None, oldest_first=True, after=since):
                # Offload SQLite writes off the event loop to avoid heartbeat blocks.
                def _upsert_wrapper():
                    try:
                        am.upsert_from_message(m, include_bots=bool(include_bots))
                    except Exception as e:
                        # capture & log error but keep going
                        err = f"{ch.name} / msg {m.id}: {e}"
                        progress["last_errors"].append(err)
                        # cap the error buffer to avoid memory blow-up
                        if len(progress["last_errors"]) > 1000:
                            del progress["last_errors"][:500]
                        progress["errors_count"] += 1
                        self._log(f"[activity_rebuild] {err}", error=True)

                await asyncio.to_thread(_upsert_wrapper)
                progress["msgs_this_channel"] += 1
                progress["msgs_total"] += 1

                # Yield periodically to keep gateway healthy
                if progress["msgs_this_channel"] % 250 == 0:
                    await asyncio.sleep(0)

        try:
            progress["phase"] = "scanning"
            for idx, ch in enumerate(channels, start=1):
                progress["channel_index"] = idx
                await scan_channel(ch)

            progress["phase"] = "done"
        except Exception as e:
            progress["phase"] = "error"
            err = f"Top-level rebuild error: {e}"
            progress["last_errors"].append(err)
            progress["errors_count"] += 1
            self._log(f"[activity_rebuild] {err}", error=True)
        finally:
            # Stop updater and do one last render
            stop_event.set()
            try:
                await progress_task
            except Exception:
                pass

            # Final, explicit message (keeps ephemeral)
            ch_n = progress["channel_total"]
            msgs_all = progress["msgs_total"]
            errs = progress["errors_count"]
            summary = f"✅ Activity metrics rebuild complete.\n• Channels: {ch_n}\n• Messages processed: {msgs_all}"
            if errs:
                summary += f"\n⚠️ Errors encountered: {errs} (see logs for details)"
            await inter.edit_original_response(content=summary)


async def setup(bot: commands.Bot):
    await bot.add_cog(ActivityMetricsCog(bot))
