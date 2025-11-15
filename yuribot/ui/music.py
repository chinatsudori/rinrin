# yuribot/ui/music.py
from __future__ import annotations

from contextlib import suppress

import discord

from yuribot.strings import S
from yuribot.util.music import (
    YuriPlayer,
    format_duration,
    format_track_title,
    player_is_paused,
)


class MusicControllerView(discord.ui.View):
    """Message controller view for a YuriPlayer."""

    def __init__(self, cog: "MusicCog", player: YuriPlayer) -> None:
        super().__init__(timeout=300)
        self.cog = cog
        self.player = player
        self._sync_state()

    # ----- internal helpers -----

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
                child.style = (
                    discord.ButtonStyle.success
                    if self.player.loop_current
                    else discord.ButtonStyle.secondary
                )
            elif child.custom_id == "music:shuffle":
                child.disabled = not has_queue or len(self.player.queue) < 2

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if not self.player.channel:
            await interaction.response.send_message(
                S("music.controller.not_connected"), ephemeral=True
            )
            return False

        voice = (
            interaction.user.voice
            if isinstance(interaction.user, discord.Member)
            else None
        )
        if not voice or voice.channel != self.player.channel:
            await interaction.response.send_message(
                S("music.controller.join_voice"), ephemeral=True
            )
            return False

        return True

    async def on_timeout(self) -> None:
        for child in self.children:
            if isinstance(child, discord.ui.Button):
                child.disabled = True
        channel = self.cog.player_text_channel(self.player)
        if channel and self.player.controller_message:
            with suppress(Exception):
                await self.player.controller_message.edit(view=self)

    # ----- buttons -----

    @discord.ui.button(
        emoji="⏯️",
        style=discord.ButtonStyle.primary,
        custom_id="music:playpause",
    )
    async def playpause(
        self, interaction: discord.Interaction, _: discord.ui.Button
    ) -> None:
        if player_is_paused(self.player):
            await self.player.resume()
        else:
            await self.player.pause()
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()

    @discord.ui.button(
        emoji="⏭️",
        style=discord.ButtonStyle.secondary,
        custom_id="music:skip",
    )
    async def skip(
        self, interaction: discord.Interaction, _: discord.ui.Button
    ) -> None:
        await self.cog.skip_current(self.player)
        await interaction.response.defer()

    @discord.ui.button(
        emoji="⏹️",
        style=discord.ButtonStyle.danger,
        custom_id="music:stop",
    )
    async def stop(
        self, interaction: discord.Interaction, _: discord.ui.Button
    ) -> None:
        await self.cog.stop_player(self.player)
        await interaction.response.defer()

    @discord.ui.button(
        emoji="🔁",
        style=discord.ButtonStyle.secondary,
        custom_id="music:loop",
    )
    async def loop(
        self, interaction: discord.Interaction, _: discord.ui.Button
    ) -> None:
        self.player.loop_current = not self.player.loop_current
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()

    @discord.ui.button(
        emoji="🔀",
        style=discord.ButtonStyle.secondary,
        custom_id="music:shuffle",
    )
    async def shuffle(
        self, interaction: discord.Interaction, _: discord.ui.Button
    ) -> None:
        self.player.shuffle_queue()
        await self.cog.refresh_controller(self.player)
        await interaction.response.defer()


def build_controller_embed(player: YuriPlayer) -> discord.Embed:
    embed = discord.Embed(color=discord.Color.blurple())

    if player.current:
        track = player.current.track
        requester = player.current.requester(player.guild)
        embed.title = S("music.controller.now_playing")
        embed.description = S(
            "music.controller.now_playing_desc",
            track=format_track_title(track),
            requester=requester,
        )
        embed.add_field(
            name=S("music.controller.field_duration"),
            value=format_duration(getattr(track, "length", None)),
        )
    else:
        embed.title = S("music.controller.idle")
        embed.description = S("music.controller.idle_hint")

    embed.add_field(
        name=S("music.controller.field_loop"),
        value=(
            S("music.controller.loop_on")
            if player.loop_current
            else S("music.controller.loop_off")
        ),
    )
    embed.add_field(
        name=S("music.controller.field_volume"),
        value=S(
            "music.controller.volume_value",
            percent=getattr(player, "volume", 100),
        ),
    )

    if player.queue:
        upcoming = []
        for idx, entry in enumerate(list(player.queue)[:5], start=1):
            upcoming.append(
                S(
                    "music.controller.up_next_line",
                    idx=idx,
                    track=format_track_title(entry.track),
                    requester=entry.requester(player.guild),
                )
            )
        embed.add_field(
            name=S("music.controller.field_up_next"),
            value="\n".join(upcoming),
            inline=False,
        )

    return embed
