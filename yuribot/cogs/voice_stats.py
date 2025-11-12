# yuribot/cogs/voice_stats.py
from __future__ import annotations

import datetime as dt
from typing import Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands


class VoiceStatsCog(commands.Cog):
    """
    Tracks live voice participation. Stores in-memory sessions keyed by (guild_id, member_id)
    and emits a single 'session' record on disconnect or channel switch.

    Wire _persist_session() to your storage (e.g., message_archive / voice_metrics table)
    if you want to save session rows.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        # (guild_id, member_id) -> session info
        self._sessions: Dict[Tuple[int, int], Dict[str, object]] = {}

    # ──────────────── Lifecycle ────────────────

    @commands.Cog.listener()
    async def on_ready(self) -> None:
        # Seed current voice states for all guilds the bot is connected to
        for guild in self.bot.guilds:
            for member in guild.members:
                vs: Optional[discord.VoiceState] = getattr(member, "voice", None)
                if vs and vs.channel:
                    self._seed_voice_state(guild, member, vs)
        self._log("Primed voice sessions for all connected guilds.")

    # ──────────────── Voice events ────────────────

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ) -> None:
        """
        Handles join/leave/move/mute changes. Uses `member` (authoritative) rather than `before.member/after.member`.
        """
        gid = member.guild.id
        key = (gid, member.id)

        before_channel = getattr(before, "channel", None)
        after_channel = getattr(after, "channel", None)

        # If member left a channel (or moved), close previous session
        prev = self._sessions.get(key)
        if prev and (
            before_channel is None
            or (after_channel and prev.get("channel_id") != after_channel.id)
        ):
            await self._close_session(gid, member, prev)

        # If member is currently in a channel, (re)open session (join or move target)
        if after_channel:
            self._seed_voice_state(member.guild, member, after)

    # ──────────────── Commands (optional) ────────────────

    @app_commands.command(
        name="voice_stats_backfill",
        description="Backfill voice sessions by scanning current voice channels.",
    )
    async def voice_stats_backfill(self, inter: discord.Interaction) -> None:
        """Minimal backfill: snapshot current members in voice as 'joined now' sessions."""
        if inter.guild is None:
            await inter.response.send_message("Run this in a server.", ephemeral=True)
            return
        await inter.response.defer(ephemeral=True)
        seeded = 0
        for ch in inter.guild.voice_channels:
            for member in ch.members:
                vs = member.voice
                if vs and vs.channel:
                    self._seed_voice_state(inter.guild, member, vs)
                    seeded += 1
        await inter.followup.send(
            f"Seeded {seeded} live voice sessions.", ephemeral=True
        )

    # ──────────────── Internals ────────────────

    def _seed_voice_state(
        self, guild: discord.Guild, member: discord.Member, vs: discord.VoiceState
    ) -> None:
        """Create/refresh the in-memory session for a member currently in a voice channel."""
        if not vs or not vs.channel:
            return
        key = (guild.id, member.id)
        now = dt.datetime.now(tz=dt.timezone.utc)
        self._sessions[key] = {
            "guild_id": guild.id,
            "guild_name": guild.name,
            "member_id": member.id,
            "member_name": str(member),
            "channel_id": vs.channel.id,
            "channel_name": vs.channel.name,
            "joined_at": now,  # we use 'now' as start point for live seed
            "muted": bool(vs.mute or vs.self_mute),
            "deafened": bool(vs.deaf or vs.self_deaf),
        }
        self._log(
            f"Seeded voice session: g={guild.id} m={member.id} ch={vs.channel.id}"
        )

    async def _close_session(
        self, guild_id: int, member: discord.Member, session: Dict[str, object]
    ) -> None:
        """Close an in-memory session and persist it via _persist_session()."""
        key = (guild_id, member.id)
        started: dt.datetime = session.get("joined_at")  # type: ignore
        ended = dt.datetime.now(tz=dt.timezone.utc)
        duration = (
            (ended - started).total_seconds()
            if isinstance(started, dt.datetime)
            else 0.0
        )

        record = {
            "guild_id": guild_id,
            "guild_name": session.get("guild_name"),
            "member_id": member.id,
            "member_name": session.get("member_name") or str(member),
            "channel_id": session.get("channel_id"),
            "channel_name": session.get("channel_name"),
            "joined_at": (
                started.isoformat() if isinstance(started, dt.datetime) else None
            ),
            "left_at": ended.isoformat(),
            "duration_s": int(duration),
            "final_muted": bool(session.get("muted", False)),
            "final_deafened": bool(session.get("deafened", False)),
        }

        # Remove from active cache first to avoid double-closing
        self._sessions.pop(key, None)

        try:
            await self._persist_session(record)
            self._log(
                f"Closed voice session: g={guild_id} m={member.id} ch={record['channel_id']} dur={record['duration_s']}s"
            )
        except Exception as e:
            self._log(f"persist error for voice session {record}: {e}", error=True)

    async def _persist_session(self, record: Dict[str, object]) -> None:
        """
        Hook to store a finished voice session. Replace this body with your DB write.

        Example if you have a DAO:
            await self.bot.loop.run_in_executor(
                None, lambda: voice_metrics.insert_session(record)
            )
        """
        # No-op placeholder to avoid crashes; log so you can wire it up.
        self._log(f"(noop persist) {record}")

    def _log(self, msg: str, *, error: bool = False) -> None:
        logger = getattr(self.bot, "logger", None)
        if logger:
            (logger.error if error else logger.info)(f"voice_stats: {msg}")
        else:
            print(f"[voice_stats]{'[ERR]' if error else ''} {msg}")


async def setup(bot: commands.Bot):
    await bot.add_cog(VoiceStatsCog(bot))
