from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..models import settings, voice_sessions
from ..strings import S
from ..utils.voice import ParsedVoiceEvent, parse_voice_log_embed

log = logging.getLogger(__name__)

# (guild_id, user_id) -> (session_id, join_time, channel_id)
LiveSessionCache = Dict[Tuple[int, int], Tuple[int, datetime, int]]

# State machine cache for backfill: user_id -> (join_time, join_msg_id, channel_id)
BackfillState = Dict[int, Tuple[datetime, int, int]]


class VoiceStatsCog(commands.Cog):
    """
    Calculates and stores voice session durations, both live and via backfill.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._is_running_backfill: Set[int] = set()  # guild_id
        self._live_sessions: LiveSessionCache = {}

    async def cog_load(self):
        """Ensures the database table exists when the cog is loaded."""
        voice_sessions.ensure_table()

    @commands.Cog.listener()
    async def on_ready(self):
        """Primes the live session cache with users already in voice."""
        log.info("Priming live voice session cache...")
        now = datetime.now(timezone.utc)
        for guild in self.bot.guilds:
            for member in guild.members:
                vs = getattr(member, "voice", None)
                if not vs or not vs.channel:
                    continue
                # Use the known Member object; VoiceState.member may be None.
                key = (guild.id, member.id)
                self._seed_voice_state(guild, member, vs)
            key = (guild.id, member.id)
            if key not in self._live_sessions:
                try:
                    session_id = voice_sessions.open_live_session(
                        guild.id, member.id, vs.channel.id, now
                    )
                    self._live_sessions[key] = (session_id, now, vs.channel.id)
                except Exception as e:
                    log.error(f"Failed to prime session for {member.id}: {e}")
        log.info(f"Live session cache primed with {len(self._live_sessions)} users.")

    # --- Live Listener ---

    @commands.Cog.listener()
    async def on_voice_state_update(
        self,
        member: discord.Member,
        before: discord.VoiceState,
        after: discord.VoiceState,
    ):
        if member.bot:
            return  # Ignore bots

        now = datetime.now(timezone.utc)
        guild_id = member.guild.id
        user_id = member.id
        cache_key = (guild_id, user_id)

        current_session = self._live_sessions.pop(cache_key, None)

        # --- Handle LEAVE part ---
        # User left a channel (before.channel exists)
        # This triggers on a simple LEAVE (after.channel is None)
        # or a MOVE (after.channel is different)
        if before.channel and current_session:
            (session_id, join_time, join_channel_id) = current_session

            # Only close the session if they are leaving the *channel we tracked*
            if join_channel_id == before.channel.id:
                duration = int((now - join_time).total_seconds())

                # Don't log tiny "channel hopping" sessions
                if duration > 10:  # 10 seconds threshold
                    try:
                        voice_sessions.close_live_session(session_id, now, duration)
                    except Exception as e:
                        log.error(f"Failed to close live session {session_id}: {e}")

                # This session is now closed, so `current_session` is cleared
                current_session = None
            else:
                # State mismatch (e.g., bot restart). Put it back.
                self._live_sessions[cache_key] = current_session

        # --- Handle JOIN part ---
        # User joined a channel (after.channel exists)
        # This triggers on a simple JOIN (before.channel is None)
        # or a MOVE (before.channel is different)
        if after.channel:
            # If the user was already in a session (e.g., a state mismatch),
            # that session is now dangling. We discard it and start the new one.
            try:
                session_id = voice_sessions.open_live_session(
                    guild_id, user_id, after.channel.id, now
                )
                self._live_sessions[cache_key] = (session_id, now, after.channel.id)
            except Exception as e:
                log.error(f"Failed to open live session for {user_id}: {e}")

    # --- Backfill Command ---

    @app_commands.command(
        name="voice_stats_backfill",
        description="Process bot logs to calculate voice session durations.",
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def voice_stats_backfill(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        guild = interaction.guild
        if guild.id in self._is_running_backfill:
            return await interaction.response.send_message(
                S("archive.backfill.already_running"), ephemeral=True  # Re-use string
            )

        log_channel_id = settings.get_bot_logs_channel(guild.id)
        if not log_channel_id:
            return await interaction.response.send_message(
                S("voice_stats.err.no_log_channel"), ephemeral=True
            )

        log_channel = guild.get_channel(log_channel_id)
        if not isinstance(log_channel, discord.TextChannel):
            return await interaction.response.send_message(
                S("voice_stats.err.bad_log_channel"), ephemeral=True
            )

        if not log_channel.permissions_for(guild.me).read_message_history:
            return await interaction.response.send_message(
                S("voice_stats.err.no_log_perms", channel=log_channel.mention),
                ephemeral=True,
                allowed_mentions=discord.AllowedMentions.none(),
            )

        await interaction.response.send_message(
            S("voice_stats.starting", channel=log_channel.mention),
            ephemeral=True,
            allowed_mentions=discord.AllowedMentions.none(),
        )
        self._is_running_backfill.add(guild.id)

        try:
            # Find the last log message we processed to resume from there
            last_id = voice_sessions.get_last_processed_log_id(guild.id)
            after_obj = discord.Object(id=last_id) if last_id else None

            log.info(
                f"Starting voice session backfill for GID {guild.id} after msg {last_id}"
            )

            events: List[ParsedVoiceEvent] = []
            async for msg in log_channel.history(
                limit=None, after=after_obj, oldest_first=True
            ):
                # Only process embeds from ourself (the bot)
                if (
                    not msg.author.bot
                    or msg.author.id != self.bot.user.id
                    or not msg.embeds
                ):
                    continue

                event = parse_voice_log_embed(msg)
                if event:
                    events.append(event)

            if not events:
                await interaction.followup.send(
                    S("voice_stats.no_new_logs"), ephemeral=True
                )
                self._is_running_backfill.remove(guild.id)
                return

            # History is already sorted oldest-to-newest.
            # process these events with a state machine.
            open_sessions: BackfillState = {}
            sessions_created = 0

            for ts, msg_id, user_id, kind, from_ch, to_ch in events:

                current_session = open_sessions.pop(user_id, None)

                # --- Handle the "leave" part of the event ---
                if (kind == "leave" or kind == "move") and current_session:
                    (join_ts, join_msg_id, join_ch_id) = current_session
                    event_from_channel = from_ch

                    if event_from_channel == join_ch_id:
                        # This event closes the session
                        duration = int((ts - join_ts).total_seconds())
                        if duration > 0:
                            voice_sessions.upsert_backfilled_session(
                                guild_id=guild.id,
                                user_id=user_id,
                                channel_id=join_ch_id,
                                join_message_id=join_msg_id,
                                join_time=join_ts.isoformat(),
                                leave_message_id=msg_id,
                                leave_time=ts.isoformat(),
                                duration_seconds=duration,
                            )
                            sessions_created += 1
                        current_session = None  # Mark as closed
                    else:
                        open_sessions[user_id] = current_session

                # --- Handle the "join" part of the event ---
                if kind == "join" or kind == "move":
                    new_channel_id = to_ch
                    if new_channel_id:
                        open_sessions[user_id] = (ts, msg_id, new_channel_id)

            await interaction.followup.send(
                S("voice_stats.complete", count=sessions_created, events=len(events)),
                ephemeral=True,
            )

        except Exception as e:
            log.exception(f"Voice stats backfill failed for GID {guild.id}")
            await interaction.followup.send(
                S("voice_stats.error", err=str(e)), ephemeral=True
            )
        finally:
            if guild.id in self._is_running_backfill:
                self._is_running_backfill.remove(guild.id)


async def setup(bot: commands.Bot):
    await bot.add_cog(VoiceStatsCog(bot))
