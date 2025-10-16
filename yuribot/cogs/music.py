from __future__ import annotations
import asyncio
import os
from dataclasses import dataclass
from datetime import timedelta
from typing import Optional, Dict, Deque, List
from collections import deque
from urllib.parse import urlparse, urlsplit, urlunsplit, quote

import discord
from discord.ext import commands
from discord import app_commands
from ..strings import S

import yt_dlp
import json, urllib.request

SPOTIFY_CLIENT_ID = os.getenv("SPOTIFY_CLIENT_ID")
SPOTIFY_CLIENT_SECRET = os.getenv("SPOTIFY_CLIENT_SECRET")
SPOTIFY_ENABLED = bool(SPOTIFY_CLIENT_ID and SPOTIFY_CLIENT_SECRET)

_sp = None
if SPOTIFY_ENABLED:
    try:
        import spotipy
        from spotipy.oauth2 import SpotifyClientCredentials

        _sp = spotipy.Spotify(
            auth_manager=SpotifyClientCredentials(
                client_id=SPOTIFY_CLIENT_ID,
                client_secret=SPOTIFY_CLIENT_SECRET,
            )
        )
    except Exception:
        _sp = None
        SPOTIFY_ENABLED = False

FFMPEG_BEFORE = (
    "-nostdin -loglevel warning "
    "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5"
)

FFMPEG_OPTS_BASE = {"before_options": FFMPEG_BEFORE, "options": "-vn"}

YTDL_BASE = {
    "format": (
        "bestaudio[acodec=opus][abr>=160]/"
        "bestaudio[abr>=192]/"
        "bestaudio[abr>=128]/"
        "bestaudio/best"
    ),
    "quiet": True,
    "noprogress": True,
    "no_warnings": True,
    "cachedir": False,
    "default_search": "ytsearch",
    "source_address": "0.0.0.0",
    "encoding": "utf-8",

    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
}

def _iri_to_uri(s: str) -> str:
    """Percent-encode non-ASCII URL parts (e.g., CJK paths) so yt-dlp/ffmpeg handle them."""
    try:
        parts = urlsplit(s)
        scheme = parts.scheme
        netloc = parts.netloc.encode("idna").decode("ascii")
        path = quote(parts.path or "", safe="/%:@")
        query = quote(parts.query or "", safe="=&;%+/?:@")
        fragment = quote(parts.fragment or "", safe="")
        return urlunsplit((scheme, netloc, path, query, fragment))
    except Exception:
        return s

def _ytdl(noplaylist: bool = True) -> yt_dlp.YoutubeDL:
    opts = dict(YTDL_BASE)
    opts["noplaylist"] = noplaylist
    return yt_dlp.YoutubeDL(opts)

def fmt_duration(seconds: Optional[int]) -> str:
    if not seconds or seconds <= 0:
        return S("music.duration.live")
    td = timedelta(seconds=int(seconds))
    h, r = divmod(td.seconds, 3600)
    m, s = divmod(r, 60)
    h += td.days * 24
    return f"{h:d}:{m:02d}:{s:02d}" if h else f"{m:d}:{s:02d}"

@dataclass
class Track:
    title: str
    url: str                 
    webpage_url: str         
    duration: Optional[int]
    requester_id: int
    headers: Optional[Dict[str, str]] = None 
def _ffmpeg_opts_for(track: Track, *, volume: float | None) -> dict:
    """
    Merge base options with per-track headers.
    If volume is provided (e.g., 1.25 for +25%), add a volume filter.
    """
    opts = dict(FFMPEG_OPTS_BASE)
    if volume and abs(volume - 1.0) > 1e-3:
        vol = max(0.0, min(volume, 3.0))  
        opts["options"] = opts["options"] + f' -filter:a "volume={vol}"'
    if track.headers:
        hdr_blob = "".join(f"{k}: {v}\r\n" for k, v in track.headers.items())
        opts["before_options"] = FFMPEG_BEFORE + f' -headers "{hdr_blob}"'
    return opts



class GuildPlayer:
    def __init__(self, bot: commands.Bot, guild: discord.Guild):
        self.bot = bot
        self.guild = guild
        self.vc: Optional[discord.VoiceClient] = None
        self.queue: Deque[Track] = deque()
        self.now: Optional[Track] = None
        self._play_next = asyncio.Event()
        self._worker_task: Optional[asyncio.Task] = None
        self._stop_flag = False
        self.volume: float = 1.0 

    def _ensure_task(self):
        if self._worker_task is None or self._worker_task.done():
            self._stop_flag = False
            self._worker_task = asyncio.create_task(self._worker(), name=f"music-worker-{self.guild.id}")

    async def connect(self, channel: discord.VoiceChannel):
        if self.vc and self.vc.channel and self.vc.channel.id == channel.id:
            return
        if self.vc and self.vc.is_connected():
            await self.vc.move_to(channel)
        else:
            self.vc = await channel.connect()

    async def _worker(self):
        while not self._stop_flag:
            if not self.queue:
                try:
                    await asyncio.wait_for(self._play_next.wait(), timeout=300.0)
                except asyncio.TimeoutError:
                    if self.vc and self.vc.is_connected():
                        try:
                            await self.vc.disconnect(force=True)
                        except Exception:
                            pass
                        self.vc = None
                    self._play_next.clear()
                    continue
                self._play_next.clear()
                if not self.queue:
                    continue

            self.now = self.queue.popleft()
            if not self.vc or not self.vc.is_connected():
                self.now = None
                continue

            try:
                source = await discord.FFmpegOpusAudio.from_probe(
                    self.now.url, **_ffmpeg_opts_for(self.now, volume=self.volume)
                )
            except Exception:
                try:
                    refetched = await MusicCog.fetch_one(self.now.webpage_url, self.now.requester_id)
                    self.now = refetched
                    source = await discord.FFmpegOpusAudio.from_probe(
                        self.now.url, **_ffmpeg_opts_for(self.now, volume=self.volume)
                    )
                except Exception:
                    self.now = None
                    continue

            done = asyncio.Event()

            def _after(err: Exception | None):
                try:
                    if err:
                        print(f"[music] playback error: {err}")
                finally:
                    self.bot.loop.call_soon_threadsafe(done.set)

            self.vc.play(source, after=_after)
            await done.wait()
            try:
                source.cleanup()
            except Exception:
                pass
            self.now = None

    def enqueue(self, track: Track):
        self.queue.append(track)
        self._ensure_task()
        self._play_next.set()

    def extend(self, tracks: List[Track]):
        if tracks:
            self.queue.extend(tracks)
            self._ensure_task()
            self._play_next.set()

    def skip(self):
        if self.vc and (self.vc.is_playing() or self.vc.is_paused()):
            self.vc.stop()

    def stop(self):
        self._stop_flag = True
        if self.vc and self.vc.is_connected():
            try:
                self.vc.stop()
            except Exception:
                pass
        self.queue.clear()
        self.now = None


class MusicCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.players: Dict[int, GuildPlayer] = {}

    def _player(self, guild: discord.Guild) -> GuildPlayer:
        gp = self.players.get(guild.id)
        if gp is None:
            gp = GuildPlayer(self.bot, guild)
            self.players[guild.id] = gp
        return gp

    @staticmethod
    def _is_url(s: str) -> bool:
        try:
            return bool(urlparse(s).scheme)
        except Exception:
            return False

    @staticmethod
    def _domain(s: str) -> str:
        try:
            return urlparse(s).netloc.lower()
        except Exception:
            return ""

    @staticmethod
    async def _ytdl_extract(query_or_url: str, allow_playlist: bool, requester_id: int) -> List[Track]:
        """
        Run yt-dlp in a thread, returning Track objects with the best audio-only URL + headers.
        We still manually select the best audio stream to avoid yt-dlp occasionally
        returning a low-bitrate fallback.
        """
        def _extract():
            y = _ytdl(noplaylist=not allow_playlist)
            info = y.extract_info(query_or_url, download=False)
            out: List[Track] = []

            def _pick(e: dict):
                chosen_url = e.get("url")
                chosen_headers = e.get("http_headers") or info.get("http_headers")
                if e.get("formats"):
                    afmts = [
                        f for f in e["formats"]
                        if f.get("vcodec") == "none" and f.get("acodec") != "none"
                    ]
                    def _score(f):
                        is_opus = 1 if (f.get("acodec") or "").lower().startswith("opus") else 0
                        abr = f.get("abr") or f.get("tbr") or 0
                        return (is_opus, abr)
                    afmts.sort(key=_score, reverse=True)
                    if afmts:
                        chosen_url = afmts[0].get("url") or chosen_url
                        chosen_headers = (afmts[0].get("http_headers") or
                                          e.get("http_headers") or info.get("http_headers"))

                title = e.get("title") or "Unknown"
                page = e.get("webpage_url") or e.get("original_url") or query_or_url
                duration = e.get("duration")
                if chosen_url:
                    out.append(
                        Track(
                            title=title,
                            url=chosen_url,
                            webpage_url=page,
                            duration=duration,
                            requester_id=requester_id,
                            headers=chosen_headers,
                        )
                    )

            if "entries" in info:
                for e in (e for e in info["entries"] if e):
                    _pick(e)
            else:
                _pick(info)
            return out

        return await asyncio.to_thread(_extract)

    @staticmethod
    def _spotify_oembed_title(url: str) -> Optional[str]:
        try:
            with urllib.request.urlopen(
                f"https://open.spotify.com/oembed?url={urllib.parse.quote(url, safe=':/?=&')}"
            ) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return data.get("title")
        except Exception:
            return None

    @staticmethod
    def _search_query_from_spotify(title:
