from __future__ import annotations

import logging
from typing import Optional, Sequence

import discord

log = logging.getLogger(__name__)

RULE_CHOICES = [
    "Respect Everyone",
    "Advertising & Self-promo",
    "Sources & Spoilers",
    "NSFW Content",
    "Politics-Free Zone",
    "Content & Posting",
    "AI Generated Content",
    "Roleplay",
    "Staff & Enforcement",
    "Other",
]


def permission_ok(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return any(
        [
            perms.manage_guild,
            perms.kick_members,
            perms.ban_members,
            perms.moderate_members,
        ]
    )


def color_for_temperature(temp: int) -> discord.Color:
    return {
        1: discord.Color.teal(),
        2: discord.Color.orange(),
        3: discord.Color.purple(),
        4: discord.Color.red(),
    }.get(temp, discord.Color.blurple())


def temperature_label(temp: int) -> str:
    from ..strings import S

    return {
        1: S("modlog.temp.gentle"),
        2: S("modlog.temp.formal"),
        3: S("modlog.temp.escalated"),
        4: S("modlog.temp.critical"),
    }.get(temp, S("modlog.temp.unknown", n=temp))


def summarize_actions(actions: Sequence[str]) -> Optional[str]:
    if not actions:
        return None
    return "\n".join(actions)[:1000]
