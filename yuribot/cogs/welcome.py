from __future__ import annotations
from pathlib import Path
import logging
import time
from typing import Optional, Tuple, Dict

import discord
from discord.ext import commands

from .. import models
from ..strings import S

log = logging.getLogger(__name__)

def _pkg_root() -> Path:
    # /app/yuribot
    return Path(__file__).resolve().parents[1]

def _app_root() -> Path:
    # /app
    return Path(__file__).resolve().parents[2]

def _ordinal(n: int) -> str:
    if 10 <= (n % 100) <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
    return f"{n}{suffix}"

def _basename_only(filename: str) -> str:
    # prevent traversal and weird separators
    name = (filename or "").strip()
    return Path(name).name  # drops any directories

class _TTLCache:
    def __init__(self, ttl_seconds: float):
        self.ttl = ttl_seconds
        self._store: Dict[tuple, Tuple[float, object]] = {}

    def get(self, key):
        rec = self._store.get(key)
        now = time.monotonic()
        if rec and (now - rec[0] < self.ttl):
            return rec[1]
        return None

    def set(self, key, value):
        self._store[key] = (time.monotonic(), value)

_cfg_cache = _TTLCache(ttl_seconds=60.0)     # guild welcome config
_img_cache = _TTLCache(ttl_seconds=300.0)    # resolved image path by filename

def _resolve_welcome_image(filename: str) -> Path | None:
    """Try a few common locations; return the first existing path. Cached."""
    fname = _basename_only(filename)
    cached = _img_cache.get(fname)
    if cached is not None:
        return cached  # Path or None

    candidates = [
        _app_root() / fname,                 # /app/welcome.png  <-- preferred
        _app_root() / "assets" / fname,      # /app/assets/welcome.png
        _pkg_root() / fname,                 # /app/yuribot/welcome.png
        _pkg_root() / "assets" / fname,      # /app/yuribot/assets/welcome.png
        Path.cwd() / fname,                  # working dir fallback
        Path.cwd() / "assets" / fname,
    ]
    found: Path | None = None
    for p in candidates:
        try:
            if p.exists():
                log.debug("welcome.image.resolve", extra={"path": str(p)})
                found = p
                break
        except Exception as e:
            # extremely defensive
            log.debug("welcome.image.exists_check_failed", extra={"path": str(p), "error": str(e)})

    if not found:
        log.warning("welcome.image.not_found", extra={"filename": fname, "tried": [str(p) for p in candidates]})
    _img_cache.set(fname, found)
    return found

def _has_perms(me: discord.Member, ch: discord.TextChannel) -> tuple[bool, list[str]]:
    perms = ch.permissions_for(me)
    missing = []
    if not perms.send_messages:
        missing.append("Send Messages")
    if not perms.embed_links:
        missing.append("Embed Links")
    if not perms.attach_files:
        missing.append("Attach Files")
    return (len(missing) == 0, missing)

class WelcomeCog(commands.Cog):
    """Welcome messages for new members."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        cfg = _cfg_cache.get(member.guild.id)
        if cfg is None:
            try:
                cfg = models.get_welcome_settings(member.guild.id)
            except Exception as e:
                log.exception("welcome.cfg.lookup_failed", extra={"guild_id": member.guild.id, "error": str(e)})
                cfg = None
            _cfg_cache.set(member.guild.id, cfg)

        if not cfg:
            log.debug("welcome.skip_not_configured", extra={"guild_id": member.guild.id})
            return

        ch = member.guild.get_channel(cfg.get("welcome_channel_id"))
        if not isinstance(ch, discord.TextChannel):
            log.warning("welcome.bad_channel", extra={"guild_id": member.guild.id, "channel_id": cfg.get("welcome_channel_id")})
            return

        me = member.guild.me  # type: ignore
        can, missing = _has_perms(me, ch) if isinstance(me, discord.Member) else (False, ["bot not member?"])
        if not can:
            log.error("welcome.missing_permissions", extra={"guild_id": member.guild.id, "channel_id": ch.id, "missing": missing})
            return

        #  member count (humans preferred) 
        try:
            human_count = sum(1 for m in member.guild.members if not m.bot)
        except Exception:
            human_count = None
        number = human_count if (human_count and human_count > 0) else (member.guild.member_count or 0)
        number = max(int(number), 1)
        ordinal = _ordinal(number)

        embed = discord.Embed(
            title=S("welcome.title"),
            description=S("welcome.desc", mention=member.mention, ordinal=ordinal),
            color=discord.Color.green(),
        )
        embed.timestamp = discord.utils.utcnow()

        filename = _basename_only(cfg.get("welcome_image_filename") or "welcome.png")
        path = _resolve_welcome_image(filename)

        file = None
        if path:
            try:
                file = discord.File(str(path), filename=path.name)
                embed.set_image(url=f"attachment://{path.name}")
            except Exception as e:
                log.warning("welcome.attach_failed", extra={"guild_id": member.guild.id, "path": str(path), "error": str(e)})

        content = S("welcome.content", mention=member.mention)

        for attempt in (1, 2):
            try:
                if file:
                    await ch.send(content=content, embed=embed, file=file, allowed_mentions=discord.AllowedMentions(users=True))
                else:
                    await ch.send(content=content, embed=embed, allowed_mentions=discord.AllowedMentions(users=True))
                log.info(
                    "welcome.sent",
                    extra={
                        "guild_id": member.guild.id,
                        "channel_id": ch.id,
                        "user_id": member.id,
                        "ordinal": ordinal,
                        "image": bool(path),
                        "attempt": attempt,
                    },
                )
                break
            except discord.Forbidden as e:
                log.error(
                    "welcome.forbidden",
                    extra={"guild_id": member.guild.id, "channel_id": ch.id, "error": str(e)},
                )
                break
            except Exception as e:
                log.warning(
                    "welcome.send_failed",
                    extra={"guild_id": member.guild.id, "channel_id": ch.id, "attempt": attempt, "error": str(e)},
                )
                if attempt == 1:
                    # small backoff then retry once
                    await discord.utils.sleep_until(discord.utils.utcnow() + discord.utils.timedelta(seconds=0.4))
                else:
                    log.exception("welcome.send_failed_final", extra={"guild_id": member.guild.id, "channel_id": ch.id})

async def setup(bot: commands.Bot):
    await bot.add_cog(WelcomeCog(bot))
