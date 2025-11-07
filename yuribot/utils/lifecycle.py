from __future__ import annotations

import logging
import signal
from typing import Callable, Optional

import discord

from ..models import settings

log = logging.getLogger(__name__)


def configure_signal_handlers(bot: discord.Client, handler: Callable[[str], None]):
    loop = bot.loop
    for signame in ("SIGINT", "SIGTERM"):
        try:
            loop.add_signal_handler(getattr(signal, signame), handler, signame)
        except (NotImplementedError, AttributeError):
            continue


def build_shutdown_message(sig_name: str) -> str:
    return f"?? Bot rebooting (signal: {sig_name})"


def botlog_channels(bot: discord.Client):
    for guild in list(bot.guilds):
        try:
            channel_id = settings.get_bot_logs_channel(guild.id)
        except Exception:
            continue
        if not channel_id:
            continue
        channel = bot.get_channel(channel_id)
        if isinstance(channel, (discord.TextChannel, discord.Thread)):
            yield channel


def permission_check(member: discord.Member) -> bool:
    perms = member.guild_permissions
    return perms.manage_guild or perms.manage_channels or perms.administrator