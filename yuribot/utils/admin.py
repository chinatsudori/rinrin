from __future__ import annotations

import os
from typing import Optional

import discord

ALLOWED_IMAGE_EXTS = {".png", ".jpg", ".jpeg"}


def validate_image_filename(name: str) -> Optional[str]:
    filename = (name or "").strip()
    _, ext = os.path.splitext(filename.lower())
    if ext not in ALLOWED_IMAGE_EXTS:
        return None
    if "/" in filename or "\\" in filename:
        return None
    return filename


async def ensure_guild(interaction: discord.Interaction) -> bool:
    from ..strings import S

    if interaction.guild:
        return True
    message = S("common.guild_only")
    if not interaction.response.is_done():
        await interaction.response.send_message(message, ephemeral=True)
    else:
        await interaction.followup.send(message, ephemeral=True)
    return False
