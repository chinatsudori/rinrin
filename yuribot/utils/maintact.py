from __future__ import annotations

import csv
import io
import logging
from typing import Iterable, Optional, Set, Tuple

import discord

from ..strings import S

log = logging.getLogger(__name__)


def require_manage_guild() -> app_commands.Check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
            return False
        if not interaction.user.guild_permissions.manage_guild:  # type: ignore[attr-defined]
            await interaction.response.send_message(S("common.need_manage_server"), ephemeral=True)
            return False
        return True

    from discord import app_commands

    return app_commands.check(predicate)


def month_from_day(day: str) -> str:
    return day[:7]


def read_csv_attachment(attachment: discord.Attachment) -> Iterable[list[str]]:
    raw = attachment.read()
    text = raw.decode("utf-8", errors="replace")
    return csv.reader(io.StringIO(text))
