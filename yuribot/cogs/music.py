from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import re
import time
from collections import deque
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Iterable, List, Sequence
from urllib.parse import urlparse

import aiohttp
import discord
from discord.ext import commands
import wavelink

log = logging.getLogger(__name__)


SPOTIFY_URL_RE = re.compile(
    r"(?:https?://open\.spotify\.com/(?:embed/)?|spotify:)(?P<type>track|album|playlist)[/:](?P<id>[A-Za-z0-9]+)"
)


# ---------------------------------------------------------------------------
# Helpers & storage
# ---------------------------------------------------------------------------
@dataclass(slots=True)
class QueuedTrack:
    track: wavelink.Playable
    requester_id: int
    requester_display: str

    def requester(self, guild: discord.Guild | None) -> str:
        if guild:
            member = guild.get_member(self.requester_id)
            if member:
                return member.mention
        return self.requester_display


def _format_duration(length: int | None) -> str:
    if not length or length <= 0:
        return "?"
    seconds = int(length // 1000)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


def _format_track_title(track: wavelink.Playable) -> str:
    title = getattr(track, "title", "Unknown track")
    uri = getattr(track, "uri", None)
    if uri:
        return f"[{title}]({uri})"
    return title


def _player_is_paused(player: wavelink.Player) -> bool:
    state = getattr(player, "is_paused", False)
    if callable(state):
        try:
            return bool(state())
        except Exception:
            return False
    return bool(state)


class PlaylistStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = asyncio.Lock()
        self._data = self._read()

    def _read(self) -> dict:
        try:
            with self.path.open("r", encoding="utf-8") as fp:
                return json.load(fp)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            log.warning("music playlists file %s is invalid JSON; resetting", self.path)
            return {}

    def _write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as fp:
            json.dump(self._data, fp, indent=2)

    async def list(self, guild_id: int) -> List[str]:
        async with self._lock:
            return sorted(pdata.get("label", name) for name, pdata in self._data.get(str(guild_id), {}).items())

    async def get(self, guild_id: int, name: str) -> dict | None:
        async with self._lock:
            return self._data.get(str(guild_id), {}).get(name.lower())

    async def set(self, guild_id: int, name: str, identifiers: Sequence[str]) -> None:
        entry = {"label": name, "tracks": list(identifiers)}
        async with self._lock:
            guild_key = str(guild_id)
            self._data.setdefault(guild_key, {})[name.lower()] = entry
            self._write()

    async def delete(self, guild_id: int, name: str) -> bool:
        async with self._lock:
            guild_key = str(guild_id)
            playlists = self._data.get(guild_key, {})
            if name.lower() not in playlists:
                return False
            playlists.pop(name.lower(), None)
            self._write()
            return True


class SpotifyResolver:
    """Resolve Spotify links into search strings for YouTube."""

    API_ROOT = "https://api.spotify.com/v1"

    def __init__(self, client_id: str, client_secret: str, *, max_tracks: int = 100) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.max_tracks = max(1, max_tracks)
        self._session: aiohttp.ClientSession | None = None
        self._token: str | None = None
        self._token_expiry: float = 0.0
        self._token_lock = asyncio.Lock()

    def enabled(self) -> bool:
        return bool(self.client_id and self.client_secret)

    async def close(self) -> None:
        if self._session and not self._session.closed:
            await self._session.close()

    async def _session_or_create(self) -> aiohttp.ClientSession:
        if self._session and not self._session.closed:
            return self._session
        self._session = aiohttp.ClientSession()
        return self._session

    async def _ensure_token(self) -> str | None:
        if not self.enabled():
            return None
        now = time.monotonic()
        if self._token and now < self._token_expiry - 30:
            return self._token
        async with self._token_lock:
            if self._token and now < self._token_expiry - 30:
                return self._token
            session = await self._session_or_create()
            data = {"grant_type": "client_credentials"}
            auth = aiohttp.BasicAuth(self.client_id, self.client_secret)
            try:
                async with session.post("https://accounts.spotify.com/api/token", data=data, auth=auth) as resp:
                    if resp.status != 200:
                        log.warning("spotify: token request failed with status %s", resp.status)
                        self._token = None
                        return None
                    payload = await resp.json()
            except Exception:
                log.exception("spotify: token request failed")
                self._token = None
                return None
            self._token = payload.get("access_token")
            expires_in = float(payload.get("expires_in", 3600))
            self._token_expiry = time.monotonic() + max(30.0, expires_in)
            return self._token

    async def _api_get(self, url: str, *, params: dict | None = None) -> dict | None:
        token = await self._ensure_token()
        if not token:
            return None
        session = await self._session_or_create()
        headers = {"Authorization": f"Bearer {token}"}
        try:
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status != 200:
                    log.warning("spotify: request to %s failed with status %s", url, resp.status)
                    return None
                return await resp.json()
        except Exception:
            log.exception("spotify: request to %s failed", url)
            return None

    async def _collect_playlist_tracks(self, playlist_id: str) -> List[dict]:
        url = f"{self.API_ROOT}/playlists/{playlist_id}/tracks"
        params = {"limit": 100}
        results: List[dict] = []
        while url and len(results) < self.max_tracks:
            data = await self._api_get(url, params=params)
            if not data:
                break
            params = None
            for item in data.get("items", []):
                track = item.get("track")
                if not track:
                    continue
                results.append(track)
                if len(results) >= self.max_tracks:
                    break
            url = data.get("next")
        return results

    async def _collect_album_tracks(self, album_id: str) -> List[dict]:
        url = f"{self.API_ROOT}/albums/{album_id}/tracks"
        params = {"limit": 50}
        results: List[dict] = []
        while url and len(results) < self.max_tracks:
            data = await self._api_get(url, params=params)
            if not data:
                break
            params = None
            for track in data.get("items", []):
                results.append(track)
                if len(results) >= self.max_tracks:
                    break
            url = data.get("next")
        return results

    async def _fetch_track(self, track_id: str) -> dict | None:
        url = f"{self.API_ROOT}/tracks/{track_id}"
        return await self._api_get(url)

    def _track_to_query(self, data: dict) -> str | None:
        name = data.get("name")
        if not name:
            return None
        artists = ", ".join(artist.get("name") for artist in data.get("artists", []) if artist.get("name"))
        if artists:
            return f"{name} - {artists}"
        return name

    async def resolve_queries(self, query: str) -> List[str]:
        if not self.enabled():
            return []
        match = SPOTIFY_URL_RE.search(query)
        if not match:
            return []
        spotify_type = match.group("type")
        spotify_id = match.group("id")
        queries: List[str] = []
        if spotify_type == "track":
            data = await self._fetch_track(spotify_id)
            built = self._track_to_query(data or {})
            if built:
                queries.append(built)
        elif spotify_type == "album":
            tracks = await self._collect_album_tracks(spotify_id)
            for track in tracks:
                built = self._track_to_query(track)
                if built:
                    queries.append(built)
        elif spotify_type == "playlist":
            tracks = await self._collect_playlist_tracks(spotify_id)
            for track in tracks:
                built = self._track_to_query(track)
                if built:
                    queries.append(built)
        return queries[: self.max_tracks]


class MusicControllerView(discord.ui.View):
    def __init__(self, cog: "MusicCog", player: "YuriPlayer") -> None:
        super().__init__(timeout=300)
        self.cog = cog
        self.player = player
        self._sync_state()

    def _sync_state(self) -> None:
        has_track = self.player.current is not None
        has_queue = bool(self.player.queue)
        for child in self.children:
            if not isinstance(child, discord.ui.Button):
                continue
            if child.custom_id == "music:playpause":
                child.disabled = not has_track
            elif child.custom_id == "music:skip":
                child.disabled = not has_track
            elif child.custom_id == "music:stop":
                child.disabled = not has_track
            elif child.custom_id == "music:loop":
                child.disabled = not has_track
                child.style = discord.ButtonStyle.success if self.player.loop_current else discord.ButtonStyle.secondary
            elif child.custom_id == "music:shuffle":
                child.disabled = not has_queue or len(self.player.queue) < 2

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not self.player.channel:
            await interaction.response.send_message("Player is not connected.", ephemeral=True)
            return False
        voice = interaction.user.voice if isinstance(interaction.user, discord.Member) else None
        if not voice or voice.channel != self.player.channel:
            await interaction.response.send_message("Join my voice channel to use the controller!", ephemeral=True)
            return False
        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        channel = self.cog._player_text_channel(self.player)
        if channel and self.player.controller_message:
            with suppress(Exception):
                await self.player.controller_message.edit(view=self)

    @discord.ui.button(emoji="⏯️", style=discord.ButtonStyle.primary, custom_id="music:playpause")
    async def playpause(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        if _player_is_paused(self.player):
            await self.player.resume()
        else:
            await self.player.pause()
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()

    @discord.ui.button(emoji="⏭️", style=discord.ButtonStyle.secondary, custom_id="music:skip")
    async def skip(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog._skip_current(self.player)
        await interaction.response.defer()

    @discord.ui.button(emoji="⏹️", style=discord.ButtonStyle.danger, custom_id="music:stop")
    async def stop(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        await self.cog._stop_player(self.player)
        await interaction.response.defer()

    @discord.ui.button(emoji="🔁", style=discord.ButtonStyle.secondary, custom_id="music:loop")
    async def loop(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.player.loop_current = not self.player.loop_current
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()

    @discord.ui.button(emoji="🔀", style=discord.ButtonStyle.secondary, custom_id="music:shuffle")
    async def shuffle(self, interaction: discord.Interaction, _: discord.ui.Button) -> None:
        self.player.shuffle_queue()
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()


class YuriPlayer(wavelink.Player):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.queue: Deque[QueuedTrack] = deque()
        self.current: QueuedTrack | None = None
        self.loop_current: bool = False
        self.bound_text_id: int | None = None
        self.controller_message: discord.Message | None = None

    def shuffle_queue(self) -> None:
        if len(self.queue) < 2:
            return
        items = list(self.queue)
        random.shuffle(items)
        self.queue.clear()
        self.queue.extend(items)


class MusicCog(commands.Cog):
    """Music system backed by Lavalink via Wavelink."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.max_bitrate = int(os.getenv("MUSIC_MAX_BITRATE", "384000"))
        playlist_path = Path(__file__).resolve().parent.parent / "data" / "music_playlists.json"
        self.playlists = PlaylistStore(playlist_path)
        spotify_id = os.getenv("SPOTIFY_CLIENT_ID")
        spotify_secret = os.getenv("SPOTIFY_CLIENT_SECRET")
        try:
            max_spotify = int(os.getenv("SPOTIFY_MAX_TRACKS", "100"))
        except ValueError:
            max_spotify = 100
        self.spotify = (
            SpotifyResolver(spotify_id, spotify_secret, max_tracks=max_spotify)
            if spotify_id and spotify_secret
            else None
        )
        self._node_task: asyncio.Task | None = self.bot.loop.create_task(self._connect_nodes())

    async def cog_unload(self) -> None:
        if self._node_task:
            self._node_task.cancel()
            with suppress(Exception):
                await self._node_task
        if self.spotify:
            await self.spotify.close()

    # ---- node bootstrap ----
    async def _connect_nodes(self) -> None:
        await self.bot.wait_until_ready()
        if wavelink.NodePool.nodes:
            return
        config = self._lavalink_config()
        try:
            await wavelink.NodePool.create_node(bot=self.bot, **config)
            log.info("music: connected to lavalink node %s:%s", config.get("host"), config.get("port"))
        except Exception:
            log.exception("music: failed to connect to lavalink node")

    def _lavalink_config(self) -> dict:
        url = os.getenv("LAVALINK_URL", "http://127.0.0.1:2333")
        parsed = urlparse(url)
        host = parsed.hostname or "127.0.0.1"
        port = parsed.port or 2333
        password = os.getenv("LAVALINK_PASSWORD", "youshallnotpass")
        identifier = os.getenv("LAVALINK_NAME", "MAIN")
        resume_key = os.getenv("LAVALINK_RESUME_KEY")
        config = {
            "host": host,
            "port": port,
            "password": password,
            "https": parsed.scheme == "https",
            "identifier": identifier,
        }
        if resume_key:
            config["resume_key"] = resume_key
        return config

    # ---- helpers ----
    async def _maybe_defer(self, ctx: commands.Context) -> None:
        if ctx.interaction and not ctx.interaction.response.is_done():
            await ctx.interaction.response.defer()

    async def _reply(self, ctx: commands.Context, **kwargs) -> None:
        if ctx.interaction:
            await ctx.send(**kwargs)
        else:
            await ctx.reply(**kwargs)

    async def _get_player(self, ctx: commands.Context, *, connect: bool = True) -> YuriPlayer | None:
        if not ctx.guild:
            await self._reply(ctx, content="This command can only be used in a guild.")
            return None
        player = ctx.guild.voice_client
        if player and not isinstance(player, YuriPlayer):
            await self._reply(ctx, content="Another voice client is already running here.")
            return None
        if player and isinstance(player, YuriPlayer):
            return player
        if not connect:
            return None
        if not isinstance(ctx.author, discord.Member) or not ctx.author.voice or not ctx.author.voice.channel:
            await self._reply(ctx, content="Join a voice channel first!")
            return None
        channel = ctx.author.voice.channel
        player = await channel.connect(cls=YuriPlayer)
        channel_bitrate = getattr(channel, "bitrate", self.max_bitrate) or self.max_bitrate
        with suppress(AttributeError):
            player.preferred_bitrate = min(channel_bitrate, self.max_bitrate)
        player.bound_text_id = ctx.channel.id if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)) else None
        return player

    def _player_text_channel(self, player: YuriPlayer) -> discord.abc.Messageable | None:
        if not player.guild or not player.bound_text_id:
            return None
        channel = player.guild.get_channel(player.bound_text_id)
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            return channel
        return None

    async def _search_tracks(self, query: str) -> List[wavelink.Playable]:
        if self.spotify:
            spotify_queries = await self.spotify.resolve_queries(query)
        else:
            spotify_queries = []

        tracks: List[wavelink.Playable] = []
        if spotify_queries:
            for entry in spotify_queries:
                try:
                    result = await wavelink.YouTubeTrack.search(query=entry, return_first=True)
                except Exception:
                    continue
                if isinstance(result, list):
                    track = result[0] if result else None
                else:
                    track = result
                if track:
                    tracks.append(track)
            if tracks:
                return tracks

        try:
            playlist = await wavelink.YouTubePlaylist.search(query=query)
        except Exception:
            playlist = None
        if playlist:
            tracks.extend(list(playlist.tracks))
        else:
            try:
                results = await wavelink.YouTubeTrack.search(query=query, return_first=False)
            except Exception:
                results = None
            if isinstance(results, list):
                tracks.extend(results)
            elif results:
                tracks.append(results)
        return tracks

    async def refresh_controller(self, player: YuriPlayer) -> None:
        channel = self._player_text_channel(player)
        if not channel:
            return
        embed = self._build_controller_embed(player)
        view = MusicControllerView(self, player)
        if player.controller_message:
            try:
                await player.controller_message.edit(embed=embed, view=view)
                return
            except discord.HTTPException:
                player.controller_message = None
        player.controller_message = await channel.send(embed=embed, view=view)

    def _build_controller_embed(self, player: YuriPlayer) -> discord.Embed:
        embed = discord.Embed(color=discord.Color.blurple())
        if player.current:
            track = player.current.track
            requester = player.current.requester(player.guild)
            embed.title = "Now Playing"
            embed.description = f"{_format_track_title(track)}\nRequested by {requester}"
            embed.add_field(name="Duration", value=_format_duration(getattr(track, "length", None)))
        else:
            embed.title = "Player Idle"
            embed.description = "Add something to the queue with /play"
        embed.add_field(name="Loop", value="On" if player.loop_current else "Off")
        embed.add_field(name="Volume", value=f"{getattr(player, 'volume', 100)}%")
        if player.queue:
            upcoming = []
            for idx, entry in enumerate(list(player.queue)[:5], start=1):
                upcoming.append(f"{idx}. {_format_track_title(entry.track)} — {entry.requester(player.guild)}")
            embed.add_field(name="Up Next", value="\n".join(upcoming), inline=False)
        return embed

    async def _queue_tracks(self, player: YuriPlayer, tracks: Iterable[wavelink.Playable], author: discord.Member) -> List[QueuedTrack]:
        added: List[QueuedTrack] = []
        for track in tracks:
            entry = QueuedTrack(track=track, requester_id=author.id, requester_display=author.display_name)
            if not player.current:
                player.current = entry
                await player.play(track)
            else:
                player.queue.append(entry)
            added.append(entry)
        return added

    async def _advance_queue(self, player: YuriPlayer) -> None:
        if player.loop_current and player.current:
            await player.play(player.current.track)
            return
        if player.queue:
            player.current = player.queue.popleft()
            await player.play(player.current.track)
            return
        player.current = None
        await self.refresh_controller(player)

    async def _skip_current(self, player: YuriPlayer) -> None:
        player.loop_current = False
        await player.stop()

    async def _stop_player(self, player: YuriPlayer) -> None:
        player.loop_current = False
        player.queue.clear()
        player.current = None
        await player.stop()
        await self.refresh_controller(player)

    async def _resolve_identifiers(self, identifiers: Sequence[str]) -> List[wavelink.Playable]:
        resolved: List[wavelink.Playable] = []
        for entry in identifiers:
            try:
                results = await wavelink.YouTubeTrack.search(query=entry, return_first=True)
            except Exception:
                continue
            if isinstance(results, list):
                track = results[0] if results else None
            else:
                track = results
            if track:
                resolved.append(track)
        return resolved

    def _collect_identifiers(self, player: YuriPlayer) -> List[str]:
        identifiers: List[str] = []
        if player.current:
            uri = getattr(player.current.track, "uri", None) or getattr(player.current.track, "identifier", None)
            if uri:
                identifiers.append(uri)
        for entry in player.queue:
            uri = getattr(entry.track, "uri", None) or getattr(entry.track, "identifier", None)
            if uri:
                identifiers.append(uri)
        return identifiers

    # ---- listeners ----
    @commands.Cog.listener()
    async def on_wavelink_node_ready(self, node: wavelink.Node) -> None:
        log.info("music: node %s is ready", node.identifier)

    @commands.Cog.listener()
    async def on_wavelink_track_end(self, player: wavelink.Player, track: wavelink.Playable, reason: str) -> None:
        if not isinstance(player, YuriPlayer):
            return
        if reason == "REPLACED":
            return
        await self._advance_queue(player)

    @commands.Cog.listener()
    async def on_wavelink_track_start(self, player: wavelink.Player, track: wavelink.Playable) -> None:
        if isinstance(player, YuriPlayer):
            await self.refresh_controller(player)

    # ---- commands ----
    @commands.hybrid_command(name="play", description="Play a song or playlist from YouTube")
    async def play(self, ctx: commands.Context, *, query: str) -> None:
        await self._maybe_defer(ctx)
        player = await self._get_player(ctx)
        if not player:
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        tracks = await self._search_tracks(query)
        if not tracks:
            await self._reply(ctx, content="No matches found.")
            return
        added = await self._queue_tracks(player, tracks, ctx.author)
        await self.refresh_controller(player)
        if len(added) == 1:
            title = _format_track_title(added[0].track)
            await self._reply(ctx, content=f"Queued {title}")
        else:
            await self._reply(ctx, content=f"Queued {len(added)} tracks")

    @commands.hybrid_command(name="pause", description="Pause the current track")
    async def pause(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content="Nothing is playing.")
            return
        await player.pause()
        await self._reply(ctx, content="Paused ⏸️")

    @commands.hybrid_command(name="resume", description="Resume playback")
    async def resume(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content="Nothing is playing.")
            return
        await player.resume()
        await self._reply(ctx, content="Resumed ▶️")

    @commands.hybrid_command(name="skip", description="Skip the current track")
    async def skip(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content="Nothing to skip.")
            return
        await self._skip_current(player)
        await self._reply(ctx, content="Skipped ⏭️")

    @commands.hybrid_command(name="stop", description="Stop playback and clear the queue")
    async def stop(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content="I'm not connected.")
            return
        await self._stop_player(player)
        await self._reply(ctx, content="Stopped and cleared the queue.")

    @commands.hybrid_command(name="leave", description="Disconnect the bot from voice")
    async def leave(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content="I'm not in a voice channel.")
            return
        await self._stop_player(player)
        await player.disconnect()
        await self._reply(ctx, content="Disconnected.")

    @commands.hybrid_command(name="nowplaying", description="Show the current track")
    async def nowplaying(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content="Not connected.")
            return
        embed = self._build_controller_embed(player)
        await self._reply(ctx, embed=embed)

    @commands.hybrid_command(name="queue", description="Show the queue")
    async def queue(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or (not player.current and not player.queue):
            await self._reply(ctx, content="Queue is empty.")
            return
        lines: List[str] = []
        if player.current:
            lines.append(f"Now: {_format_track_title(player.current.track)}")
        for idx, entry in enumerate(player.queue, start=1):
            lines.append(f"{idx}. {_format_track_title(entry.track)} — {_format_duration(getattr(entry.track, 'length', None))}")
        description = "\n".join(lines)
        embed = discord.Embed(title="Queue", description=description, color=discord.Color.dark_teal())
        await self._reply(ctx, embed=embed)

    @commands.hybrid_command(name="volume", description="Set the player volume (1-150)")
    async def volume(self, ctx: commands.Context, level: int) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content="I'm not connected.")
            return
        level = max(1, min(level, 150))
        await player.set_volume(level)
        await self._reply(ctx, content=f"Volume set to {level}%")
        await self.refresh_controller(player)

    @commands.hybrid_command(name="controller", description="Post or refresh the music controller")
    async def controller(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content="Nothing to control yet.")
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        await self.refresh_controller(player)
        await self._reply(ctx, content="Controller refreshed.")

    # ---- playlist commands ----
    @commands.hybrid_group(name="playlist", description="Manage server playlists", invoke_without_command=True)
    async def playlist(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await self._reply(ctx, content="This command only works in a guild.")
            return
        names = await self.playlists.list(ctx.guild.id)
        if not names:
            await self._reply(ctx, content="No playlists saved yet.")
            return
        await self._reply(ctx, content="Playlists: " + ", ".join(names))

    @playlist.command(name="save", description="Save the current queue as a playlist")
    async def playlist_save(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content="This command only works in a guild.")
            return
        player = await self._get_player(ctx, connect=False)
        if not player or (not player.current and not player.queue):
            await self._reply(ctx, content="Nothing to save.")
            return
        identifiers = self._collect_identifiers(player)
        if not identifiers:
            await self._reply(ctx, content="Unable to save tracks without URLs.")
            return
        await self.playlists.set(ctx.guild.id, name, identifiers)
        await self._reply(ctx, content=f"Saved playlist **{name}** with {len(identifiers)} tracks.")

    @playlist.command(name="load", description="Load a saved playlist into the queue")
    async def playlist_load(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content="This command only works in a guild.")
            return
        data = await self.playlists.get(ctx.guild.id, name)
        if not data:
            await self._reply(ctx, content="Playlist not found.")
            return
        await self._maybe_defer(ctx)
        player = await self._get_player(ctx)
        if not player:
            return
        identifiers = data.get("tracks", [])
        tracks = await self._resolve_identifiers(identifiers)
        if not tracks:
            await self._reply(ctx, content="Unable to resolve any tracks from that playlist.")
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        await self._queue_tracks(player, tracks, ctx.author)
        await self.refresh_controller(player)
        await self._reply(ctx, content=f"Loaded playlist **{data.get('label', name)}** ({len(tracks)} tracks).")

    @playlist.command(name="delete", description="Delete a saved playlist")
    async def playlist_delete(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content="This command only works in a guild.")
            return
        deleted = await self.playlists.delete(ctx.guild.id, name)
        if not deleted:
            await self._reply(ctx, content="Playlist not found.")
            return
        await self._reply(ctx, content=f"Deleted playlist **{name}**.")


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MusicCog(bot))
