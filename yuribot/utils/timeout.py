from __future__ import annotations

import logging
from datetime import timedelta
from typing import Optional

import discord

log = logging.getLogger(__name__)

MAX_TIMEOUT_DAYS = 28


def has_mod_perms(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return any(
        [
            perms.moderate_members,
            perms.kick_members,
            perms.ban_members,
            perms.manage_guild,
        ]
    )


def can_act(
    actor: discord.Member,
    target: discord.Member,
    bot_member: Optional[discord.Member],
) -> tuple[bool, Optional[str]]:
    if actor.id == target.id:
        return False, "timeout.error.self"
    if target.guild.owner_id == target.id:
        return False, "timeout.error.owner"
    if not has_mod_perms(actor):
        return False, "timeout.error.actor_perms"
    if not bot_member or not bot_member.guild_permissions.moderate_members:
        return False, "timeout.error.bot_perms"
    if bot_member.top_role <= target.top_role:
        return False, "timeout.error.bot_hierarchy"
    if actor != target.guild.owner and actor.top_role <= target.top_role:
        return False, "timeout.error.actor_hierarchy"
    return True, None


def clamp_duration(
    days: int,
    hours: int,
    minutes: int,
    seconds: int,
) -> timedelta:
    delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)
    if delta.total_seconds() < 60:
        raise ValueError("timeout.error.min_duration")
    if delta > timedelta(days=MAX_TIMEOUT_DAYS):
        delta = timedelta(days=MAX_TIMEOUT_DAYS)
    return delta
