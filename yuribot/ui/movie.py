from __future__ import annotations

import discord

from ..strings import S
from ..utils.movie import to_utc


def build_description(title: str, link: str | None) -> str:
    lines = [S("movie.desc.header", title=title)]
    if link:
        lines.append(S("movie.desc.link", link=link))
    return "\n".join(lines)[:1000]


def build_embed(
    *,
    title: str,
    start_am: int,
    start_pm: int,
    venue: discord.abc.GuildChannel | None,
    location: str | None,
    duration_minutes: int,
    event_am: discord.ScheduledEvent,
    event_pm: discord.ScheduledEvent,
    link: str | None = None,
) -> discord.Embed:
    embed = discord.Embed(
        title=S("movie.scheduled.title"),
        description=S(
            "movie.scheduled.desc",
            title=title,
            am=start_am,
            pm=start_pm,
        ),
        color=discord.Color.green(),
    )
    if venue:
        embed.add_field(name=S("movie.field.venue"), value=venue.mention, inline=True)
    elif location:
        embed.add_field(name=S("movie.field.location"), value=location, inline=True)
    embed.add_field(
        name=S("movie.field.duration"),
        value=S("movie.value.duration_min", minutes=duration_minutes),
        inline=True,
    )
    embed.add_field(
        name=S("movie.field.events"),
        value=S("movie.value.events_links", am_url=event_am.url, pm_url=event_pm.url),
        inline=False,
    )
    if link:
        embed.add_field(name=S("movie.field.link"), value=link[:256], inline=False)
    return embed
