from __future__ import annotations

import discord

from ..strings import S
from ..utils.timestamp import tz_display


def build_timestamp_embed(*, epoch: int, local_iso: str, tzinfo) -> discord.Embed:
    tags = {
        S("tools.timestamp.label.relative"): f"<t:{epoch}:R>",
        S("tools.timestamp.label.full"): f"<t:{epoch}:F>",
        S("tools.timestamp.label.short_dt"): f"<t:{epoch}:f>",
        S("tools.timestamp.label.date"): f"<t:{epoch}:D>",
        S("tools.timestamp.label.date_short"): f"<t:{epoch}:d>",
        S("tools.timestamp.label.time"): f"<t:{epoch}:T>",
        S("tools.timestamp.label.time_short"): f"<t:{epoch}:t>",
    }
    preview_lines = [f"**{label}:** {value}" for label, value in tags.items()]
    copy_lines = [f"{label}: `{value}`" for label, value in tags.items()]

    embed = discord.Embed(
        title=S("tools.timestamp.title"),
        description="\n".join(preview_lines),
        color=discord.Color.blurple(),
    )
    embed.add_field(
        name=S("tools.timestamp.copy_field"),
        value="\n".join(copy_lines),
        inline=False,
    )
    embed.set_footer(text=S("tools.timestamp.footer", local_iso=local_iso, tz=tz_display(tzinfo)))
    return embed