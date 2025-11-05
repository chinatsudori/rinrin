from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

import discord

from ..strings import S
from ..utils.stats import human_delta


def build_uptime_embed(uptime_seconds: float, started_at: datetime) -> discord.Embed:
    embed = discord.Embed(
        title=S("stats.uptime.title"),
        color=discord.Color.green(),
    )
    embed.add_field(
        name=S("stats.uptime.field.uptime"),
        value=human_delta(uptime_seconds),
        inline=True,
    )
    embed.add_field(
        name=S("stats.uptime.field.since"),
        value=f"<t:{int(started_at.timestamp())}:F>",
        inline=True,
    )
    return embed


def build_botinfo_embed(metrics: Dict[str, Any]) -> discord.Embed:
    embed = discord.Embed(
        title=S("stats.botinfo.title"),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name=S("stats.botinfo.field.guilds"),
        value=str(metrics["guilds"]),
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.members_cached"),
        value=str(metrics["members_cached"]),
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.humans_bots"),
        value=f"{metrics['humans']} / {metrics['bots']}",
        inline=True,
    )
    shard_id = metrics.get("shard_id")
    shard_count = metrics.get("shard_count")
    if isinstance(shard_count, int) and shard_count > 1 and isinstance(shard_id, int):
        shard_label = f"{shard_id}/{shard_count}"
    else:
        shard_label = S("stats.botinfo.na")

    embed.add_field(
        name=S("stats.botinfo.field.commands"),
        value=str(metrics["commands_total"]),
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.shard"),
        value=shard_label,
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.gateway_ping"),
        value=f"{metrics['gw_latency_ms']:.0f} ms",
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.memory"),
        value=metrics["memory"],
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.cpu"),
        value=metrics["cpu"],
        inline=True,
    )
    embed.add_field(
        name=S("stats.botinfo.field.runtime"),
        value=S(
            "stats.botinfo.value.runtime",
            py=metrics["py_version"],
            dpy=metrics["discord_version"],
        ),
        inline=False,
    )
    return embed
