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
    "-reconnect 1 -reconnect_streamed 1 -reconnect_delay_max 5 "
    "-probesize 100M -analyzeduration 100M"
)

FFMPEG_OPTS_BASE = {
    "before_options": FFMPEG_BEFORE,
    # force 48kHz stereo, clean opus at 128k VBR, longer frames (smoother), audio profile
    "options": (
        "-vn -ar 48000 -ac 2 "
        "-c:a libopus -b:a 128k -vbr on -compression_level 10 "
        "-frame_duration 60 -application audio"
    ),
}
# Prefer highest-ABR audio-only; don’t force Opus if M4A/AAC has higher bitrate.
YTDL_BASE = {
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
    # Use multiple clients to dodge occasional quirks
    "extractor_args": {"youtube": {"player_client": ["android", "web"]}},
    "socket_timeout": 20,
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
    url: str                 # direct audio stream URL for ffmpeg
    webpage_url: str         # canonical page
    duration: Optional[int]
    requester_id: int
    headers: Optional[Dict[str, str]] = None  # HTTP headers for ffmpeg (avoid 403)

def _ffmpeg_opts_for(track: Track, *, volume: float | None) -> dict:
    """
    Merge base options with per-track headers.
    If volume is provided (e.g., 1.25 for +25%), add a volume filter.
    """
    opts = dict(FFMPEG_OPTS_BASE)
    if volume and abs(volume - 1.0) > 1e-3:
        vol = max(0.0, min(volume, 3.0))  # clamp 0–300%
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
        self.volume: float = 1.0  # 1.0 = 100%

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
                # Refetch (signed URL could have expired)
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
        Now prioritizes highest bitrate first (then Opus as a tiebreaker).
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
                    # Score: prefer higher abr/tbr first; use Opus as a tiebreaker; then higher sample rate.
                    def _score(f):
                        abr = f.get("abr") or f.get("tbr") or 0
                        is_opus = 1 if (f.get("acodec") or "").lower().startswith("opus") else 0
                        asr = f.get("asr") or 0
                        return (abr, is_opus, asr)

                    afmts.sort(key=_score, reverse=True)
                    if afmts:
                        top = afmts[0]
                        chosen_url = top.get("url") or chosen_url
                        chosen_headers = top.get("http_headers") or e.get("http_headers") or chosen_headers

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
    def _search_query_from_spotify(title: Optional[str], url: str) -> str:
        if title and " - " in title:
            return title
        parsed = urlparse(url)
        parts = [p for p in parsed.path.split("/") if p]
        return f"spotify {parts[0] if parts else ''} {parts[1] if len(parts) > 1 else ''}".strip()

    @staticmethod
    async def fetch_one(query_or_url: str, requester_id: int) -> Track:
        tracks = await MusicCog.fetch_many(query_or_url, requester_id, limit=1)
        if not tracks:
            raise RuntimeError("No audio found")
        return tracks[0]

    @staticmethod
    async def fetch_many(query_or_url: str, requester_id: int, limit: int = 100) -> List[Track]:
        is_url = MusicCog._is_url(query_or_url)
        if is_url:
            query_or_url = _iri_to_uri(query_or_url)
        domain = MusicCog._domain(query_or_url) if is_url else ""

        # SoundCloud
        if "soundcloud.com" in domain:
            items = await MusicCog._ytdl_extract(query_or_url, allow_playlist=True, requester_id=requester_id)
            return items[:limit]

        # Spotify
        if "open.spotify.com" in domain or "spotify.link" in domain:
            if SPOTIFY_ENABLED and _sp:
                try:
                    path = urlparse(query_or_url).path.strip("/")
                    head = path.split("/")[0]
                    out: List[Track] = []
                    if head == "track":
                        tid = path.split("/")[1].split("?")[0]
                        t = _sp.track(tid)
                        title = f"{t['artists'][0]['name']} - {t['name']}"
                        query = f"{title} audio"
                        out = await MusicCog._ytdl_extract(query, allow_playlist=False, requester_id=requester_id)
                    elif head in ("album", "playlist"):
                        ids = []
                        if head == "album":
                            aid = path.split("/")[1].split("?")[0]
                            res = _sp.album_tracks(aid, limit=50, offset=0)
                            ids.extend([it["id"] for it in res["items"] if it.get("id")])
                            while res.get("next"):
                                res = _sp.next(res)
                                ids.extend([it["id"] for it in res["items"] if it.get("id")])
                        else:  # playlist
                            pid = path.split("/")[1].split("?")[0]
                            res = _sp.playlist_items(pid, limit=50)
                            def _items(r): return [it["track"] for it in r["items"] if it.get("track")]
                            tracks_meta = _items(res)
                            while res.get("next"):
                                res = _sp.next(res)
                                tracks_meta.extend(_items(res))
                            ids = [t["id"] for t in tracks_meta if t and t.get("id")]
                        results: List[Track] = []
                        for i in range(0, len(ids), 50):
                            batch = ids[i:i+50]
                            meta = _sp.tracks(batch)["tracks"]
                            for t in meta:
                                if not t: continue
                                title = f"{t['artists'][0]['name']} - {t['name']}"
                                query = f"{title} audio"
                                got = await MusicCog._ytdl_extract(query, allow_playlist=False, requester_id=requester_id)
                                if got:
                                    results.append(got[0])
                                    if len(results) >= limit:
                                        break
                            if len(results) >= limit:
                                break
                        return results[:limit]
                except Exception:
                    pass  # fall through

            title = MusicCog._spotify_oembed_title(query_or_url)
            query = MusicCog._search_query_from_spotify(title, query_or_url)
            got = await MusicCog._ytdl_extract(query, allow_playlist=False, requester_id=requester_id)
            return got[:1]

        # YouTube / everything else
        allow_playlist = is_url and ("youtube.com/playlist" in query_or_url or "list=" in query_or_url)
        items = await MusicCog._ytdl_extract(query_or_url, allow_playlist=allow_playlist, requester_id=requester_id)
        return items[:limit]

    async def _require_voice(self, interaction: discord.Interaction, channel: Optional[discord.VoiceChannel] = None) -> Optional[GuildPlayer]:
        if not interaction.guild:
            await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
            return None
        gp = self._player(interaction.guild)
        target = channel
        if target is None:
            mem = interaction.guild.get_member(interaction.user.id)
            if mem and mem.voice and isinstance(mem.voice.channel, discord.VoiceChannel):
                target = mem.voice.channel
        if target is None:
            await interaction.response.send_message(S("music.error.join_first"), ephemeral=True)
            return None
        if not interaction.response.is_done():
            await interaction.response.defer(ephemeral=True, thinking=True)
        await gp.connect(target)
        return gp

    @app_commands.command(name="join", description="Summon the bot to a voice channel.")
    @app_commands.describe(channel="Voice channel (optional; defaults to your current one)")
    async def join(self, interaction: discord.Interaction, channel: Optional[discord.VoiceChannel] = None):
        gp = await self._require_voice(interaction, channel)
        if not gp:
            return
        ch = gp.vc.channel if gp.vc else channel
        await interaction.followup.send(S("music.joined", name=(ch.name if ch else "voice")), ephemeral=True)

    @app_commands.command(name="leave", description="Disconnect from voice and clear the queue.")
    async def leave(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        gp = self._player(interaction.guild)
        gp.stop()
        if gp.vc and gp.vc.is_connected():
            await gp.vc.disconnect(force=True)
            gp.vc = None
        await interaction.response.send_message(S("music.left"), ephemeral=True)

    @app_commands.command(name="play", description="Play a track by URL or search query (YouTube, Spotify, SoundCloud).")
    @app_commands.describe(query="URL or search query (Spotify/SC links supported)")
    async def play(self, interaction: discord.Interaction, query: str):
        gp = await self._require_voice(interaction)
        if not gp:
            return
        try:
            tracks = await self.fetch_many(query, interaction.user.id, limit=100)
        except Exception as e:
            return await interaction.followup.send(S("music.error.resolve", error=str(e)), ephemeral=True)

        if not tracks:
            return await interaction.followup.send(S("music.error.no_audio"), ephemeral=True)

        if len(tracks) == 1:
            gp.enqueue(tracks[0])
            where = (S("music.queue.where_now") if gp.now is None and gp.vc and not gp.vc.is_playing()
                     else S("music.queue.where_pos", pos=len(gp.queue)))
            t = tracks[0]
            return await interaction.followup.send(
                S(
                    "music.queued.single",
                    title=discord.utils.escape_markdown(t.title),
                    duration=fmt_duration(t.duration),
                    where=where),
                ephemeral=True,
            )

        MAX_BULK = 50
        bundle = tracks[:MAX_BULK]
        gp.extend(bundle)
        more = max(0, len(tracks) - len(bundle))
        await interaction.followup.send(
            S("music.queued.bulk", count=len(bundle), more=more),
            ephemeral=True,
        )

    @app_commands.command(name="skip", description="Skip the current track.")
    async def skip(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        gp = self._player(interaction.guild)
        if not gp.vc or not (gp.vc.is_playing() or gp.vc.is_paused()):
            return await interaction.response.send_message(S("music.error.nothing_playing"), ephemeral=True)
        gp.skip()
        await interaction.response.send_message(S("music.skipped"), ephemeral=True)

    @app_commands.command(name="pause", description="Pause playback.")
    async def pause(self, interaction: discord.Interaction):
        gp = self._player(interaction.guild)
        if not gp.vc or not gp.vc.is_playing():
            return await interaction.response.send_message(S("music.error.nothing_playing"), ephemeral=True)
        gp.vc.pause()
        await interaction.response.send_message(S("music.paused"), ephemeral=True)

    @app_commands.command(name="resume", description="Resume playback.")
    async def resume(self, interaction: discord.Interaction):
        gp = self._player(interaction.guild)
        if not gp.vc or not gp.vc.is_paused():
            return await interaction.response.send_message(S("music.error.nothing_to_resume"), ephemeral=True)
        gp.vc.resume()
        await interaction.response.send_message(S("music.resumed"), ephemeral=True)

    @app_commands.command(name="stop", description="Stop playback and clear the queue.")
    async def stop(self, interaction: discord.Interaction):
        gp = self._player(interaction.guild)
        if not gp.vc:
            return await interaction.response.send_message(S("music.error.not_connected"), ephemeral=True)
        gp.stop()
        await interaction.response.send_message(S("music.stopped"), ephemeral=True)

    @app_commands.command(name="now", description="Show the currently playing track.")
    async def now(self, interaction: discord.Interaction):
        gp = self._player(interaction.guild)
        if not gp.now:
            return await interaction.response.send_message(S("music.error.nothing_playing"), ephemeral=True)
        t = gp.now
        await interaction.response.send_message(
            S("music.now",
              title=discord.utils.escape_markdown(t.title),
              duration=fmt_duration(t.duration),
              url=t.webpage_url),
            ephemeral=True,
        )

    @app_commands.command(name="queue", description="Show the next tracks in the queue (up to 10).")
    async def queue_cmd(self, interaction: discord.Interaction):
        gp = self._player(interaction.guild)
        if not gp.queue:
            return await interaction.response.send_message(S("music.queue.empty"), ephemeral=True)
        items = list(gp.queue)[:10]
        lines = [
            S("music.queue.line", idx=i + 1,
              title=discord.utils.escape_markdown(t.title),
              duration=fmt_duration(t.duration))
            for i, t in enumerate(items)
        ]
        more = len(gp.queue) - len(items)
        txt = "\n".join(lines) + (S("music.queue.more", more=more) if more > 0 else "")
        await interaction.response.send_message(txt, ephemeral=True)

    @app_commands.command(name="volume", description="Set playback volume (50–200%).")
    @app_commands.describe(percent="Volume as a percentage (50–200). Defaults to 100.")
    async def volume(self, interaction: discord.Interaction, percent: Optional[int] = 100):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        gp = self._player(interaction.guild)
        p = 100 if percent is None else max(50, min(200, int(percent)))
        gp.volume = p / 100.0
        await interaction.response.send_message(f"Volume set to **{p}%**.", ephemeral=True)

    @commands.Cog.listener()
    async def on_voice_state_update(self, member: discord.Member, before: discord.VoiceState, after: discord.VoiceState):
        if not member.guild.me or member.id != member.guild.me.id:
            return
        if before.channel and not after.channel:
            gp = self.players.get(member.guild.id)
            if gp:
                gp.stop()
                gp.vc = None


async def setup(bot: commands.Bot):
    await bot.add_cog(MusicCog(bot))
