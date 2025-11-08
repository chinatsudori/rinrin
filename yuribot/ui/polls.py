from __future__ import annotations

from typing import List

import discord

from ..strings import S


def check_options(options: List[str]) -> discord.Embed | None:
    if len(options) < 2:
        return None
    return None


def format_multi_warning() -> str:
    return "This Discord library build doesn't support multi-select polls; created single-choice."
