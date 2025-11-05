from __future__ import annotations

from typing import Iterable, Set

import discord

TARGET_ROLE_ID = 1417963012492623892


def role_ids(roles: Iterable[discord.Role]) -> Set[int]:
    return {role.id for role in roles if isinstance(role, discord.Role)}
