from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Union

import discord
import yt_dlp
from discord.ext import commands

FFMPEG_BEFORE = (
    "-nostdin -loglevel warning "
    "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
    "-probesize 100M -analyzeduration 100M"
)

FFMPEG_OPTIONS = "-vn -ar 48000 -ac 2"

YTDL_OPTIONS = {
    "format": (
        "bestaudio[abr>=320]/"
        "bestaudio[abr>=256]/"
        "bestaudio[abr>=192]/"
        "bestaudio[abr>=160]/"
        "bestaudio/best"
    ),
    "quiet": True,
    "noprogress": True,
    "no_warnings": True,
    "cachedir": False,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "encoding": "utf-8",
    "socket_timeout": 20,
}

log = logging.getLogger(__name__)

GuildTextish = Union[discord.TextChannel, discord.Thread]


def _get_ytdl() -> yt_dlp.YoutubeDL:
    return yt_dlp.YoutubeDL(dict(YTDL_OPTIONS))


@dataclass
class MusicTrack:
    title: str
    stream_url: str
    webpage_url: str
    duration: Optional[int]
    requester_id: int
    headers: Optional[Dict[str, str]] = None


async def extract_track(query: str, *, requester_id: int) -> MusicTrack:
    loop = asyncio.get_running_loop()

    def _extract() -> Dict:
        with _get_ytdl() as ytdl:
            info = ytdl.extract_info(query, download=False)
            if "entries" in info:
                for entry in info["entries"]:
                    if entry:
                        info = entry
                        break
            return info

    info = await loop.run_in_executor(None, _extract)
    stream = info.get("url")
    if not stream:
        raise RuntimeError("Could not extract audio URL")
    webpage = info.get("webpage_url") or info.get("original_url") or query
    headers = info.get("http_headers")
    return MusicTrack(
        title=info.get("title") or query,
        stream_url=stream,
        webpage_url=webpage,
        duration=info.get("duration"),
        requester_id=requester_id,
        headers=headers,
    )


def format_duration(seconds: Optional[int]) -> str:
    if not seconds or seconds <= 0:
        return 'live'
    minutes, secs = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours:d}:{minutes:02d}:{secs:02d}"
    return f"{minutes:d}:{secs:02d}"


class GuildMusicState:
    def __init__(self, bot: commands.Bot, guild: discord.Guild):

        self.bot = bot
        self.guild = guild
        self.voice: Optional[discord.VoiceClient] = None
        self.queue: asyncio.Queue[MusicTrack] = asyncio.Queue()
        self.current: Optional[MusicTrack] = None
        self.volume: float = 0.95
        self._player_task: Optional[asyncio.Task] = None
        self._play_next = asyncio.Event()

    async def ensure_voice(self, channel: discord.VoiceChannel):
        if self.voice and self.voice.is_connected():
            if self.voice.channel and self.voice.channel.id != channel.id:
                await self.voice.move_to(channel)
            return
        self.voice = await channel.connect()

    def _ensure_task(self):
        if self._player_task is None or self._player_task.done():
            self._player_task = asyncio.create_task(self.player_loop(), name=f"music-player-{self.guild.id}")

    async def enqueue(self, track: MusicTrack):
        await self.queue.put(track)
        self._ensure_task()

    async def player_loop(self):
        while True:
            track = await self.queue.get()
            self.current = track
            if not self.voice or not self.voice.is_connected():
                self.current = None
                continue

            before_options = FFMPEG_BEFORE
            if track.headers:
                header_block = "".join(f"{k}: {v}\r\n" for k, v in track.headers.items())
                before_options += f' -headers "{header_block}"'

            source = discord.FFmpegPCMAudio(
                track.stream_url,
                before_options=before_options,
                options=FFMPEG_OPTIONS,
            )
            pcm = discord.PCMVolumeTransformer(source, volume=self.volume)
            self._play_next.clear()

            def _after_play(error: Optional[Exception]):
                if error:
                    log.warning("music.playback.error", exc_info=error)
                loop = self.bot.loop
                loop.call_soon_threadsafe(self._play_next.set)

            self.voice.play(pcm, after=_after_play)
            await self._play_next.wait()
            self.current = None
            if self.voice and not self.voice.is_playing() and self.queue.empty():
                break

    def skip(self):
        if self.voice and self.voice.is_playing():
            self.voice.stop()

    async def stop(self):
        while not self.queue.empty():
            try:
                self.queue.get_nowait()
            except asyncio.QueueEmpty:
                break
        if self.voice and self.voice.is_playing():
            self.voice.stop()
        self.current = None

    async def disconnect(self):
        await self.stop()
        if self.voice and self.voice.is_connected():
            await self.voice.disconnect(force=True)
        self.voice = None

    def snapshot(self) -> Tuple[Optional[MusicTrack], List[MusicTrack]]:
        pending = list(self.queue._queue)  # type: ignore[attr-defined]
        return self.current, pending
