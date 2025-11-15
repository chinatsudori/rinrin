from __future__ import annotations

import asyncio
import logging
import os
from contextlib import suppress
from pathlib import Path
from typing import List
from urllib.parse import urlparse

import discord
from discord.ext import commands
import wavelink

from yuribot.strings import S
from yuribot.ui.music import MusicControllerView, build_controller_embed
from yuribot.utils.music import (
    PlaylistStore,
    SpotifyResolver,
    YuriPlayer,
    advance_queue,
    collect_identifiers,
    queue_tracks,
    resolve_identifiers,
    search_tracks,
    skip_current,
    stop_player,
)

log = logging.getLogger(__name__)


class MusicCog(commands.Cog):
    """Music system backed by Lavalink via Wavelink."""

    def __init__(self, bot: commands.Bot) -> None:
        self.bot = bot
        self.max_bitrate = int(os.getenv("MUSIC_MAX_BITRATE", "384000"))

        playlist_path = (
            Path(__file__).resolve().parent.parent / "data" / "music_playlists.json"
        )
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

        self._node_task: asyncio.Task | None = self.bot.loop.create_task(
            self._connect_nodes()
        )

    async def cog_unload(self) -> None:
        if self._node_task:
            self._node_task.cancel()
            with suppress(Exception):
                await self._node_task
        if self.spotify:
            await self.spotify.close()

        # ---- node bootstrap ----

    async def _connect_nodes(self) -> None:
        """Connect to the Lavalink node using whatever Wavelink API is available."""
        await self.bot.wait_until_ready()

        NodePool = getattr(wavelink, "NodePool", None)
        if NodePool is None:
            log.error(
                "music: wavelink.NodePool is not available in this wavelink build."
            )
            return

        # If nodes already exist, don't recreate them.
        try:
            existing_nodes = getattr(NodePool, "nodes", {}) or {}
        except Exception:
            existing_nodes = {}
        if existing_nodes:
            return

        config = self._lavalink_config()

        # Normalize the kwargs once
        node_kwargs = {
            "host": config.get("host"),
            "port": config.get("port"),
            "password": config.get("password"),
            "https": config.get("https", False),
            # Most Wavelink 2.x builds accept 'bot' OR 'client'; we try both in order.
        }

        # Helper to try a callable with varied parameter names
        async def _try_call(func) -> bool:
            if func is None:
                return False
            try:
                # First try with 'bot'
                try:
                    await func(
                        bot=self.bot, identifier=config.get("identifier"), **node_kwargs
                    )
                    return True
                except TypeError:
                    # Fall back to 'client' if 'bot' isn't accepted
                    await func(
                        client=self.bot,
                        identifier=config.get("identifier"),
                        **node_kwargs,
                    )
                    return True
            except Exception:
                log.exception("music: node connect attempt with %r failed", func)
                return False

        try:
            # 1) Class-level create_node (classic 2.x API)
            if hasattr(NodePool, "create_node"):
                if await _try_call(getattr(NodePool, "create_node")):
                    log.info(
                        "music: connected to lavalink node %s:%s via NodePool.create_node (class)",
                        config.get("host"),
                        config.get("port"),
                    )
                    return

            # 2) Instance-level create_node (some builds put it on the instance)
            pool_instance = None
            try:
                pool_instance = NodePool()
            except Exception:
                pool_instance = None

            if pool_instance is not None:
                inst_create = getattr(pool_instance, "create_node", None)
                if inst_create and await _try_call(inst_create):
                    log.info(
                        "music: connected to lavalink node %s:%s via NodePool().create_node (instance)",
                        config.get("host"),
                        config.get("port"),
                    )
                    return

                # 3) Instance-level connect(...) fallback (just in case)
                inst_connect = getattr(pool_instance, "connect", None)
                if inst_connect and await _try_call(inst_connect):
                    log.info(
                        "music: connected to lavalink node %s:%s via NodePool().connect",
                        config.get("host"),
                        config.get("port"),
                    )
                    return

            log.error(
                "music: NodePool has no usable create_node/connect; attributes: %s",
                [a for a in dir(NodePool) if not a.startswith("_")],
            )
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
        config: dict = {
            "host": host,
            "port": port,
            "password": password,
            "https": parsed.scheme == "https",
            "identifier": identifier,
        }
        if resume_key:
            config["resume_key"] = resume_key
        return config

    # ---- generic helpers ----

    async def _maybe_defer(self, ctx: commands.Context) -> None:
        if ctx.interaction and not ctx.interaction.response.is_done():
            await ctx.interaction.response.defer()

    async def _reply(self, ctx: commands.Context, **kwargs) -> None:
        if ctx.interaction:
            await ctx.send(**kwargs)
        else:
            await ctx.reply(**kwargs)

    def _any_node_connected(self) -> bool:
        try:
            nodes = getattr(wavelink.NodePool, "nodes", {}) or {}
            return any(
                getattr(getattr(n, "status", None), "name", None) == "CONNECTED"
                for n in nodes.values()
            )
        except Exception:
            try:
                return any(getattr(n, "available", False) for n in nodes.values())  # type: ignore[name-defined]
            except Exception:
                return False

    async def _get_player(
        self, ctx: commands.Context, *, connect: bool = True
    ) -> YuriPlayer | None:
        if not ctx.guild:
            await self._reply(ctx, content=S("common.guild_only"))
            return None

        # Ensure a connected node before attempting voice connect.
        if not self._any_node_connected():
            await self._reply(ctx, content=S("music.controller.not_connected"))
            with suppress(Exception):
                await self._connect_nodes()
            return None

        player = ctx.guild.voice_client
        if player and not isinstance(player, YuriPlayer):
            await self._reply(ctx, content=S("music.error.other_client"))
            return None
        if player and isinstance(player, YuriPlayer):
            return player
        if not connect:
            return None

        if (
            not isinstance(ctx.author, discord.Member)
            or not ctx.author.voice
            or not ctx.author.voice.channel
        ):
            await self._reply(ctx, content=S("music.error.join_voice_first"))
            return None

        channel = ctx.author.voice.channel
        player = await channel.connect(cls=YuriPlayer)
        channel_bitrate = (
            getattr(channel, "bitrate", self.max_bitrate) or self.max_bitrate
        )
        with suppress(AttributeError):
            player.preferred_bitrate = min(channel_bitrate, self.max_bitrate)
        player.bound_text_id = (
            ctx.channel.id
            if isinstance(ctx.channel, (discord.TextChannel, discord.Thread))
            else None
        )
        return player  # type: ignore[return-value]

    def player_text_channel(self, player: YuriPlayer) -> discord.abc.Messageable | None:
        if not player.guild or not player.bound_text_id:
            return None
        channel = player.guild.get_channel(player.bound_text_id)
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            return channel
        return None

    async def refresh_controller(self, player: YuriPlayer) -> None:
        channel = self.player_text_channel(player)
        if not channel:
            return
        embed = build_controller_embed(player)
        view = MusicControllerView(self, player)
        if player.controller_message:
            try:
                await player.controller_message.edit(embed=embed, view=view)
                return
            except discord.HTTPException:
                player.controller_message = None
        player.controller_message = await channel.send(embed=embed, view=view)

    # Exposed helpers for UI view
    async def skip_current(self, player: YuriPlayer) -> None:
        await skip_current(player)
        await self.refresh_controller(player)

    async def stop_player(self, player: YuriPlayer) -> None:
        await stop_player(player)
        await self.refresh_controller(player)

    # ---- listeners ----

    @commands.Cog.listener()
    async def on_wavelink_node_ready(self, node: wavelink.Node) -> None:
        log.info("music: node %s is ready", getattr(node, "identifier", "unknown"))

    @commands.Cog.listener()
    async def on_wavelink_track_end(
        self, player: wavelink.Player, track: wavelink.Playable, reason: str
    ) -> None:
        if not isinstance(player, YuriPlayer):
            return
        if reason == "REPLACED":
            return
        await advance_queue(player)
        await self.refresh_controller(player)

    @commands.Cog.listener()
    async def on_wavelink_track_start(
        self, player: wavelink.Player, track: wavelink.Playable
    ) -> None:
        if isinstance(player, YuriPlayer):
            await self.refresh_controller(player)

    # =========================
    # Group: /music …
    # =========================

    @commands.hybrid_group(
        name="music",
        description="Music controls",
        invoke_without_command=True,
    )
    async def music(self, ctx: commands.Context) -> None:
        await self._reply(
            ctx,
            content=(
                "Use subcommands: play, pause, resume, skip, stop, leave, "
                "nowplaying, queue, volume, controller, playlist, node"
            ),
        )

    # ---- core controls ----

    @music.command(name="play", description=S("music.cmd.play"))
    async def music_play(self, ctx: commands.Context, *, query: str) -> None:
        await self._maybe_defer(ctx)
        player = await self._get_player(ctx)
        if not player:
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        tracks = await search_tracks(query, spotify=self.spotify)
        if not tracks:
            await self._reply(ctx, content=S("music.error.no_matches"))
            return
        added = await queue_tracks(player, tracks, ctx.author)  # type: ignore[arg-type]
        await self.refresh_controller(player)
        if len(added) == 1:
            title = format_track_title(added[0].track)
            await self._reply(ctx, content=S("music.info.queued_single", title=title))
        else:
            await self._reply(
                ctx, content=S("music.info.queued_multi", count=len(added))
            )

    @music.command(name="pause", description=S("music.cmd.pause"))
    async def music_pause(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content=S("music.error.nothing_playing"))
            return
        await player.pause()
        await self._reply(ctx, content=S("music.info.paused"))

    @music.command(name="resume", description=S("music.cmd.resume"))
    async def music_resume(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content=S("music.error.nothing_playing"))
            return
        await player.resume()
        await self._reply(ctx, content=S("music.info.resumed"))

    @music.command(name="skip", description=S("music.cmd.skip"))
    async def music_skip(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player or not player.current:
            await self._reply(ctx, content=S("music.error.nothing_to_skip"))
            return
        await skip_current(player)
        await self._reply(ctx, content=S("music.info.skipped"))

    @music.command(name="stop", description=S("music.cmd.stop"))
    async def music_stop(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content=S("music.error.not_connected"))
            return
        await stop_player(player)
        await self._reply(ctx, content=S("music.info.stopped"))
        await self.refresh_controller(player)

    @music.command(name="leave", description=S("music.cmd.leave"))
    async def music_leave(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content=S("music.error.not_in_voice"))
            return
        await stop_player(player)
        await player.disconnect()
        await self._reply(ctx, content=S("music.info.disconnected"))

    @music.command(name="nowplaying", description=S("music.cmd.nowplaying"))
    async def music_nowplaying(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content=S("music.error.not_connected"))
            return
        embed = build_controller_embed(player)
        await self._reply(ctx, embed=embed)

    @music.command(name="queue", description=S("music.cmd.queue"))
    async def music_queue(self, ctx: commands.Context) -> None:
        from yuribot.util.music import format_duration, format_track_title

        player = await self._get_player(ctx, connect=False)
        if not player or (not player.current and not player.queue):
            await self._reply(ctx, content=S("music.error.queue_empty"))
            return
        lines: List[str] = []
        if player.current:
            lines.append(
                S(
                    "music.queue.line_now",
                    track=format_track_title(player.current.track),
                )
            )
        for idx, entry in enumerate(player.queue, start=1):
            lines.append(
                S(
                    "music.queue.line_entry",
                    idx=idx,
                    track=format_track_title(entry.track),
                    duration=format_duration(getattr(entry.track, "length", None)),
                )
            )
        description = "\n".join(lines)
        embed = discord.Embed(
            title=S("music.queue.embed_title"),
            description=description,
            color=discord.Color.dark_teal(),
        )
        await self._reply(ctx, embed=embed)

    @music.command(name="volume", description=S("music.cmd.volume"))
    async def music_volume(self, ctx: commands.Context, level: int) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content=S("music.error.not_connected"))
            return
        level = max(1, min(level, 150))
        await player.set_volume(level)
        await self._reply(ctx, content=S("music.info.volume_set", level=level))
        await self.refresh_controller(player)

    @music.command(name="controller", description=S("music.cmd.controller"))
    async def music_controller(self, ctx: commands.Context) -> None:
        player = await self._get_player(ctx, connect=False)
        if not player:
            await self._reply(ctx, content=S("music.error.nothing_to_control"))
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        await self.refresh_controller(player)
        await self._reply(ctx, content=S("music.info.controller_refreshed"))

    # ---- playlist subgroup ----

    @music.group(
        name="playlist",
        description=S("music.cmd.playlist"),
        invoke_without_command=True,
    )
    async def music_playlist(self, ctx: commands.Context) -> None:
        if not ctx.guild:
            await self._reply(ctx, content=S("common.guild_only"))
            return
        names = await self.playlists.list(ctx.guild.id)
        if not names:
            await self._reply(ctx, content=S("music.info.playlists_none"))
            return
        await self._reply(
            ctx, content=S("music.info.playlists_list", names=", ".join(names))
        )

    @music_playlist.command(name="save", description=S("music.cmd.playlist_save"))
    async def music_playlist_save(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content=S("common.guild_only"))
            return
        player = await self._get_player(ctx, connect=False)
        if not player or (not player.current and not player.queue):
            await self._reply(ctx, content=S("music.error.nothing_to_save"))
            return
        identifiers = collect_identifiers(player)
        if not identifiers:
            await self._reply(ctx, content=S("music.error.no_urls_to_save"))
            return
        await self.playlists.set(ctx.guild.id, name, identifiers)
        await self._reply(
            ctx,
            content=S("music.info.playlist_saved", name=name, count=len(identifiers)),
        )

    @music_playlist.command(name="load", description=S("music.cmd.playlist_load"))
    async def music_playlist_load(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content=S("common.guild_only"))
            return
        data = await self.playlists.get(ctx.guild.id, name)
        if not data:
            await self._reply(ctx, content=S("music.error.playlist_missing"))
            return
        await self._maybe_defer(ctx)
        player = await self._get_player(ctx)
        if not player:
            return
        identifiers = data.get("tracks", [])
        tracks = await resolve_identifiers(identifiers)
        if not tracks:
            await self._reply(ctx, content=S("music.error.resolve_failed"))
            return
        if isinstance(ctx.channel, (discord.TextChannel, discord.Thread)):
            player.bound_text_id = ctx.channel.id
        await queue_tracks(player, tracks, ctx.author)  # type: ignore[arg-type]
        await self.refresh_controller(player)
        await self._reply(
            ctx,
            content=S(
                "music.info.playlist_loaded",
                name=data.get("label", name),
                count=len(tracks),
            ),
        )

    @music_playlist.command(name="delete", description=S("music.cmd.playlist_delete"))
    async def music_playlist_delete(self, ctx: commands.Context, name: str) -> None:
        if not ctx.guild:
            await self._reply(ctx, content=S("common.guild_only"))
            return
        deleted = await self.playlists.delete(ctx.guild.id, name)
        if not deleted:
            await self._reply(ctx, content=S("music.error.playlist_missing"))
            return
        await self._reply(ctx, content=S("music.info.playlist_deleted", name=name))

    # ---- node subgroup ----

    @music.group(
        name="node",
        description="Lavalink node tools",
        invoke_without_command=True,
    )
    async def music_node(self, ctx: commands.Context) -> None:
        nodes = getattr(wavelink.NodePool, "nodes", {}) or {}
        if not nodes:
            await self._reply(ctx, content="No nodes registered.")
            return
        lines = []
        for n in nodes.values():
            status = getattr(getattr(n, "status", None), "name", None) or "UNKNOWN"
            ident = getattr(n, "identifier", "unknown")
            host = getattr(n, "host", "?")
            port = getattr(n, "port", "?")
            lines.append(f"{ident} @ {host}:{port} — {status}")
        await self._reply(ctx, content="\n".join(lines))

    @music_node.command(
        name="connect",
        description="Connect to the Lavalink node now",
    )
    async def music_node_connect(self, ctx: commands.Context) -> None:
        import traceback

        try:
            await self._connect_nodes()
        except Exception as e:
            tb = traceback.format_exc()
            # Log to whatever logger you *do* see
            log.error("music: explicit node connect failure: %r\n%s", e, tb)
            # And send a compact version to Discord
            await self._reply(
                ctx,
                content=f"Node connect failed: `{type(e).__name__}: {e}`",
            )
            return

        # If we got here, _connect_nodes didn't raise. Show node statuses.
        nodes = getattr(wavelink.NodePool, "nodes", {}) or {}
        if not nodes:
            await self._reply(ctx, content="No nodes registered after connect.")
            return

        lines = []
        for n in nodes.values():
            status = getattr(getattr(n, "status", None), "name", None) or "UNKNOWN"
            ident = getattr(n, "identifier", "unknown")
            host = getattr(n, "host", "?")
            port = getattr(n, "port", "?")
            lines.append(f"{ident} @ {host}:{port} — {status}")

        await self._reply(
            ctx,
            content="Node connect attempted.\n" + "\n".join(lines),
        )


async def setup(bot: commands.Bot) -> None:
    await bot.add_cog(MusicCog(bot))
