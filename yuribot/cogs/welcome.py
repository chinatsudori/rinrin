from __future__ import annotations

import logging
from typing import Optional

import discord
from discord.ext import commands

from ..models import settings
from ..strings import S
from ..ui.welcome import build_welcome_embed, welcome_content
from ..utils.welcome import (
    cfg_cache,
    has_perms,
    img_cache,
    ordinal,
    resolve_welcome_image,
)

log = logging.getLogger(__name__)


class WelcomeCog(commands.Cog):
    """Welcome messages for new members."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        cfg = cfg_cache.get(member.guild.id)
        if cfg is None:
            try:
                cfg = settings.get_welcome_settings(member.guild.id)
            except Exception as exc:
                log.exception(
                    "welcome.cfg.lookup_failed",
                    extra={"guild_id": member.guild.id, "error": str(exc)},
                )
                cfg = None
            cfg_cache.set(member.guild.id, cfg)

        if not cfg:
            log.debug(
                "welcome.skip_not_configured", extra={"guild_id": member.guild.id}
            )
            return

        channel_id: Optional[int] = cfg.get("welcome_channel_id")
        ch = member.guild.get_channel(channel_id) if channel_id else None
        if not isinstance(ch, discord.TextChannel):
            log.warning(
                "welcome.bad_channel",
                extra={"guild_id": member.guild.id, "channel_id": channel_id},
            )
            return

        me = member.guild.me  # type: ignore[assignment]
        can_send, missing = (
            has_perms(me, ch)
            if isinstance(me, discord.Member)
            else (False, ["bot not member?"])
        )
        if not can_send:
            log.error(
                "welcome.missing_permissions",
                extra={
                    "guild_id": member.guild.id,
                    "channel_id": ch.id,
                    "missing": missing,
                },
            )
            return

        try:
            human_count = sum(1 for m in member.guild.members if not m.bot)
        except Exception:
            human_count = None
        number = (
            human_count
            if human_count and human_count > 0
            else (member.guild.member_count or 0)
        )
        ordinal_str = ordinal(max(int(number), 1))

        embed = build_welcome_embed(member, ordinal_str)

        filename = cfg.get("welcome_image_filename") or "welcome.png"
        path = resolve_welcome_image(filename)

        file: Optional[discord.File] = None
        if path:
            try:
                file = discord.File(str(path), filename=path.name)
                embed.set_image(url=f"attachment://{path.name}")
            except Exception as exc:
                log.warning(
                    "welcome.attach_failed",
                    extra={
                        "guild_id": member.guild.id,
                        "path": str(path),
                        "error": str(exc),
                    },
                )

        content = welcome_content(member)

        allowed_mentions = discord.AllowedMentions(users=True)
        for attempt in (1, 2):
            try:
                if file:
                    await ch.send(
                        content=content,
                        embed=embed,
                        file=file,
                        allowed_mentions=allowed_mentions,
                    )
                else:
                    await ch.send(
                        content=content, embed=embed, allowed_mentions=allowed_mentions
                    )
                log.info(
                    "welcome.sent",
                    extra={
                        "guild_id": member.guild.id,
                        "channel_id": ch.id,
                        "user_id": member.id,
                        "ordinal": ordinal_str,
                        "image": bool(path),
                        "attempt": attempt,
                    },
                )
                break
            except discord.Forbidden as exc:
                log.error(
                    "welcome.forbidden",
                    extra={
                        "guild_id": member.guild.id,
                        "channel_id": ch.id,
                        "error": str(exc),
                    },
                )
                break
            except Exception as exc:
                log.warning(
                    "welcome.send_failed",
                    extra={
                        "guild_id": member.guild.id,
                        "channel_id": ch.id,
                        "attempt": attempt,
                        "error": str(exc),
                    },
                )
                if attempt == 1:
                    await discord.utils.sleep_until(
                        discord.utils.utcnow() + discord.utils.timedelta(seconds=0.4)
                    )
                else:
                    log.exception(
                        "welcome.send_failed_final",
                        extra={"guild_id": member.guild.id, "channel_id": ch.id},
                    )


async def setup(bot: commands.Bot):
    await bot.add_cog(WelcomeCog(bot))
