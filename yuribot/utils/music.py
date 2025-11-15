# yuribot/util/music.py
from __future__ import annotations

import asyncio
import json
import logging
import random
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Deque, Iterable, List, Sequence

import aiohttp
import discord
import wavelink

log = logging.getLogger(__name__)

SPOTIFY_URL_RE = re.compile(
    r"(?:https?://open\.spotify\.com/(?:embed/)?|spotify:)"
    r"(?P<type>track|album|playlist)[/:](?P<id>[A-Za-z0-9]+)"
)


# ---------------------------------------------------------------------------
# Core types & helpers
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


def format_duration(length: int | None) -> str:
    if not length or length <= 0:
        return "?"
    seconds = int(length // 1000)
    minutes, sec = divmod(seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}:{minutes:02d}:{sec:02d}"
    return f"{minutes}:{sec:02d}"


def format_track_title(track: wavelink.Playable) -> str:
    title = getattr(track, "title", "Unknown track")
    uri = getattr(track, "uri", None)
    if uri:
        return f"[{title}]({uri})"
    return title


def player_is_paused(player: wavelink.Player) -> bool:
    state = getattr(player, "is_paused", False)
    if callable(state):
        try:
            return bool(state())
        except Exception:
            return False
    return bool(state)


# ---------------------------------------------------------------------------
# Persistent playlist storage
# ---------------------------------------------------------------------------


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
            return sorted(
                pdata.get("label", name)
                for name, pdata in self._data.get(str(guild_id), {}).items()
            )

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


# ---------------------------------------------------------------------------
# Spotify resolution → YT search queries
# ---------------------------------------------------------------------------


class SpotifyResolver:
    """Resolve Spotify links into search strings for YouTube."""

    API_ROOT = "https://api.spotify.com/v1"

    def __init__(
        self, client_id: str, client_secret: str, *, max_tracks: int = 100
    ) -> None:
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
                async with session.post(
                    "https://accounts.spotify.com/api/token", data=data, auth=auth
                ) as resp:
                    if resp.status != 200:
                        log.warning(
                            "spotify: token request failed with status %s", resp.status
                        )
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
                    log.warning(
                        "spotify: request to %s failed with status %s", url, resp.status
                    )
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
        artists = ", ".join(
            artist.get("name")
            for artist in data.get("artists", [])
            if artist.get("name")
        )
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


# ---------------------------------------------------------------------------
# Player model
# ---------------------------------------------------------------------------


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


# ---------------------------------------------------------------------------
# Backend operations (search, queue, etc.)
# ---------------------------------------------------------------------------


async def search_tracks(
    query: str, *, spotify: SpotifyResolver | None
) -> List[wavelink.Playable]:
    # Spotify URL → list of YT queries
    spotify_queries = await spotify.resolve_queries(query) if spotify else []
    tracks: List[wavelink.Playable] = []

    if spotify_queries:
        for entry in spotify_queries:
            try:
                result = await wavelink.YouTubeTrack.search(
                    query=entry, return_first=True
                )
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

    # Try playlist first (if query is a playlist URL)
    try:
        playlist = await wavelink.YouTubePlaylist.search(query=query)
    except Exception:
        playlist = None
    if playlist:
        tracks.extend(list(playlist.tracks))
    else:
        try:
            results = await wavelink.YouTubeTrack.search(
                query=query, return_first=False
            )
        except Exception:
            results = None
        if isinstance(results, list):
            tracks.extend(results)
        elif results:
            tracks.append(results)
    return tracks


async def queue_tracks(
    player: YuriPlayer,
    tracks: Iterable[wavelink.Playable],
    author: discord.Member,
) -> List[QueuedTrack]:
    added: List[QueuedTrack] = []
    for track in tracks:
        entry = QueuedTrack(
            track=track,
            requester_id=author.id,
            requester_display=author.display_name,
        )
        if not player.current:
            player.current = entry
            await player.play(track)
        else:
            player.queue.append(entry)
        added.append(entry)
    return added


async def advance_queue(player: YuriPlayer) -> None:
    if player.loop_current and player.current:
        await player.play(player.current.track)
        return
    if player.queue:
        player.current = player.queue.popleft()
        await player.play(player.current.track)
        return
    player.current = None


async def skip_current(player: YuriPlayer) -> None:
    player.loop_current = False
    await player.stop()


async def stop_player(player: YuriPlayer) -> None:
    player.loop_current = False
    player.queue.clear()
    player.current = None
    await player.stop()


async def resolve_identifiers(
    identifiers: Sequence[str],
) -> List[wavelink.Playable]:
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


def collect_identifiers(player: YuriPlayer) -> List[str]:
    identifiers: List[str] = []
    if player.current:
        uri = getattr(player.current.track, "uri", None) or getattr(
            player.current.track, "identifier", None
        )
        if uri:
            identifiers.append(uri)
    for entry in player.queue:
        uri = getattr(entry.track, "uri", None) or getattr(
            entry.track, "identifier", None
        )
        if uri:
            identifiers.append(uri)
    return identifiers
