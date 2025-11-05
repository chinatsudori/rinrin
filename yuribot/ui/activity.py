from __future__ import annotations

from typing import List

import discord

from ..strings import S

LESBIAN_COLORS = [
    "#D52D00",
    "#EF7627",
    "#FF9A56",
    "#FFFFFF",
    "#D162A4",
    "#B55690",
    "#A30262",
]


async def require_guild(inter: discord.Interaction) -> bool:
    if not inter.guild:
        if not inter.response.is_done():
            await inter.response.send_message(S("common.guild_only"), ephemeral=True)
        else:
            await inter.followup.send(S("common.guild_only"), ephemeral=True)
        return False
    return True


def area_with_vertical_gradient(ax, x_positions, y_values, cmap, bg_bottom: float = 0.0):
    import numpy as np

    area = ax.fill_between(x_positions, y_values, bg_bottom, color="none")
    xmin, xmax = min(x_positions) - 0.5, max(x_positions) + 0.5
    ymin, ymax = bg_bottom, max(max(y_values) * 1.05, 1)
    grad = np.linspace(0, 1, 256).reshape(256, 1)
    im = ax.imshow(
        grad,
        extent=[xmin, xmax, ymin, ymax],
        origin="lower",
        aspect="auto",
        cmap=cmap,
        alpha=1.0,
        zorder=1,
    )
    im.set_clip_path(area.get_paths()[0], transform=ax.transData)
    return area, im


def format_rank(rows: List[tuple[int, int]], guild: discord.Guild, limit: int) -> str:
    lines: List[str] = []
    for i, (uid, cnt) in enumerate(rows[:limit], start=1):
        member = guild.get_member(uid)
        name = member.mention if member else f"<@{uid}>"
        lines.append(f"{i}. {name} - **{cnt}**")
    return "\n".join(lines) if lines else S("activity.leaderboard.empty")


def format_hour_range_local(start: int, end: int, tzlabel: str = "PT") -> str:
    def meridian(hour: int) -> str:
        ampm = "AM" if hour < 12 else "PM"
        value = hour % 12 or 12
        return f"{value}{ampm}"

    return f"{meridian(start)}-{meridian(end)} {tzlabel}"


def build_rank_embed(
    guild: discord.Guild, rows: List[tuple[int, int, int]], *, color: discord.Color = discord.Color.gold()
) -> discord.Embed:
    lines: List[str] = []
    for index, (uid, level, xp) in enumerate(rows, start=1):
        member = guild.get_member(uid)
        who = member.mention if member else f"<@{uid}>"
        lines.append(f"{index}. {who} - **Lv {level}** ({xp} XP)")
    if not lines:
        description = "No one has started their journey yet."
    else:
        description = "\n".join(lines)
    return discord.Embed(
        title=S("activity.rank.title"),
        description=description,
        color=color,
    )


def build_profile_embed(
    *,
    target: discord.Member,
    level: int,
    total_xp: int,
    progress_current: int,
    progress_needed: int,
    stats: dict,
    engagement_ratio: float,
    reply_density: float,
    mention_depth: float,
    media_ratio: float,
    burstiness: float,
    prime_hour: str,
    prime_channel: str,
    voice_minutes: int,
    stream_minutes: int,
    activity_minutes: int,
    activity_joins: int,
) -> discord.Embed:
    steps = 20
    if progress_needed <= 0:
        filled = steps
        pct = 100
    else:
        ratio = max(0.0, min(1.0, progress_current / progress_needed))
        filled = int(round(ratio * steps))
        if 0 < ratio < 1 and filled == 0:
            filled = 1
        filled = min(steps, max(0, filled))
        pct = int(round(ratio * 100))
    bar = "[{}{}]".format("=" * filled, "." * (steps - filled))

    embed = discord.Embed(
        title=S("activity.profile.title", user=target.display_name),
        color=discord.Color.purple(),
    )
    embed.add_field(
        name="Level & Progress",
        value=(
            f"**Lv {level}** - {total_xp} XP\n"
            f"{bar}\n"
            f"{progress_current}/{progress_needed or progress_current} ({pct}%)"
        ),
        inline=False,
    )

    stats_line = (
        f"**STR** {stats.get('str', 0)}  **DEX** {stats.get('dex', 0)}  "
        f"**INT** {stats.get('int', 0)}  **WIS** {stats.get('wis', 0)}  "
        f"**CHA** {stats.get('cha', 0)}  **VIT** {stats.get('vit', 0)}"
    )
    embed.add_field(name=S("activity.profile.stats"), value=stats_line, inline=False)

    derived = (
        f"Engagement ratio: **{engagement_ratio:.2f}**\n"
        f"Reply density: **{reply_density:.2f}**\n"
        f"Mention depth: **{mention_depth:.2f}**\n"
        f"Media ratio: **{media_ratio:.2f}**\n"
        f"Burstiness: **{burstiness:.2f}**\n"
        f"Prime hour: **{prime_hour}**\n"
        f"Prime channel: {prime_channel}"
    )
    embed.add_field(name=S("activity.profile.derived"), value=derived, inline=False)

    embed.add_field(
        name=S("activity.profile.voice"),
        value=f"Voice: **{voice_minutes}** min; Streaming: **{stream_minutes}** min",
        inline=True,
    )
    embed.add_field(
        name=S("activity.profile.apps"),
        value=f"Activities: **{activity_minutes}** min; Joins: **{activity_joins}**",
        inline=True,
    )

    return embed


def build_metric_leaderboard_embed(
    *,
    guild: discord.Guild,
    metric_name: str,
    scope_label: str,
    rows: List[tuple[int, int]],
    color: discord.Color = discord.Color.blurple(),
) -> discord.Embed:
    lines: List[str] = []
    for index, (uid, count) in enumerate(rows, start=1):
        member = guild.get_member(int(uid))
        who = member.mention if member else f"<@{int(uid)}>"
        lines.append(f"{index}. {who} - **{int(count)}** {metric_name}")

    description = "\n".join(lines) if lines else S("activity.leaderboard.empty")
    return discord.Embed(
        title=f"Top {metric_name} - {scope_label}",
        description=description,
        color=color,
    )
