from __future__ import annotations

import asyncio
import logging
from typing import Dict, Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S
from ..ui.music import format_queue, format_track
from ..utils.music import GuildMusicState, extract_track

log = logging.getLogger(__name__)


class MusicCog(commands.Cog):
    """Lightweight music playback via yt-dlp and FFmpeg."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._states: Dict[int, GuildMusicState] = {}

    def _state_for(self, guild: discord.Guild) -> GuildMusicState:
        state = self._states.get(guild.id)
        if not state:
            state = GuildMusicState(self.bot, guild)
            self._states[guild.id] = state
        return state

    music = app_commands.Group(name="music", description="Music playback commands")

    async def _user_voice_channel(self, interaction: discord.Interaction) -> Optional[discord.VoiceChannel]:
        member = interaction.user if isinstance(interaction.user, discord.Member) else interaction.guild.get_member(interaction.user.id)  # type: ignore[arg-type]
        if isinstance(member, discord.Member) and member.voice and isinstance(member.voice.channel, discord.VoiceChannel):
            return member.voice.channel
        return None

    async def _ensure_voice(self, interaction: discord.Interaction) -> Optional[discord.VoiceChannel]:
        if not interaction.guild:
            return None
        channel = await self._user_voice_channel(interaction)
        if channel is None:
            if not interaction.response.is_done():
                await interaction.response.send_message(S("music.error.join_voice") if hasattr(S, "__call__") else "Join a voice channel first.", ephemeral=True)
            else:
                await interaction.followup.send("Join a voice channel first.", ephemeral=True)
            return None
        state = self._state_for(interaction.guild)
        await state.ensure_voice(channel)
        return channel

    @music.command(name="play", description="Queue a song or playlist URL/search result")
    @app_commands.describe(query="URL or search query")
    async def play(self, interaction: discord.Interaction, query: str):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        channel = await self._ensure_voice(interaction)
        if channel is None:
            return
        await interaction.response.defer(ephemeral=True)
        try:
            track = await extract_track(query, requester_id=interaction.user.id)
        except Exception as exc:
            log.exception("music.extract.failed", exc_info=exc)
            return await interaction.followup.send("Could not find audio for that query.", ephemeral=True)

        state = self._state_for(interaction.guild)
        await state.enqueue(track)
        await interaction.followup.send(f"Queued {format_track(track)}", ephemeral=True)

    @music.command(name="skip", description="Skip the current track")
    async def skip(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        state = self._state_for(interaction.guild)
        if not state.voice or not state.voice.is_connected():
            return await interaction.response.send_message("Not connected to voice.", ephemeral=True)
        state.skip()
        await interaction.response.send_message("Skipped.", ephemeral=True)

    @music.command(name="stop", description="Stop playback and clear the queue")
    async def stop(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        state = self._state_for(interaction.guild)
        if not state.voice or not state.voice.is_connected():
            return await interaction.response.send_message("Not connected to voice.", ephemeral=True)
        await state.stop()
        await interaction.response.send_message("Playback stopped and queue cleared.", ephemeral=True)

    @music.command(name="leave", description="Disconnect the bot from voice")
    async def leave(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        state = self._state_for(interaction.guild)
        await state.disconnect()
        await interaction.response.send_message("Left voice channel.", ephemeral=True)

    @music.command(name="queue", description="Show the current queue")
    async def queue(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        state = self._state_for(interaction.guild)
        now, pending = state.snapshot()
        parts = []
        if now:
            parts.append(f"Now playing: {format_track(now)}")
        if pending:
            parts.append("Up next:\n" + format_queue(pending))
        if not parts:
            parts.append("Queue is empty.")
        await interaction.response.send_message("\n".join(parts), ephemeral=True)

    @music.command(name="now", description="Show the currently playing track")
    async def now(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        state = self._state_for(interaction.guild)
        now_playing, _ = state.snapshot()
        if not now_playing:
            await interaction.response.send_message("Nothing is playing.", ephemeral=True)
        else:
            await interaction.response.send_message(f"Now playing: {format_track(now_playing)}", ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MusicCog(bot))
