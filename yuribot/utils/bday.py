from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Optional

import discord
from discord.ext import tasks

from .. import config
from ..models import bday as model
from ..ui.bday import select_birthday_message

try:
    from zoneinfo import ZoneInfo  # py3.9+
except Exception:
    ZoneInfo = None

log = logging.getLogger(__name__)

# === Target channel ===========================================================
# Hard default per request; can be overridden in config.py:
#   BIRTHDAY_CHANNEL_ID = 123456789012345678
BIRTHDAY_CHANNEL_ID = int(getattr(config, "BIRTHDAY_CHANNEL_ID", 1417424779354574935))

# === Timezone normalization ===================================================

# Whatever the config provides (string, ZoneInfo, pytz tzfile, None)
DEFAULT_TZ_RAW = getattr(config, "TZ", getattr(config, "LOCAL_TZ", "UTC"))


def _tz_to_name(obj) -> str:
    """Best-effort: turn tz-like objects into an IANA tz string."""
    if obj is None:
        return "UTC"
    if isinstance(obj, str):
        return obj.strip() or "UTC"
    # zoneinfo.ZoneInfo has .key
    key = getattr(obj, "key", None)
    if isinstance(key, str) and key:
        return key
    # pytz tzfile/timezone has .zone
    zone = getattr(obj, "zone", None)
    if isinstance(zone, str) and zone:
        return zone
    # last resort: tzname() (may require a datetime; try without)
    try:
        tn = obj.tzname(None)  # type: ignore[arg-type]
        if isinstance(tn, str) and tn:
            return tn
    except Exception:
        pass
    return "UTC"


DEFAULT_TZ_NAME = _tz_to_name(DEFAULT_TZ_RAW)


def coerce_tz(tzname: Optional[str | object]) -> str:
    """
    Accepts str or tzinfo-like and returns a valid IANA tz string.
    Falls back to DEFAULT_TZ_NAME, then 'UTC'.
    """
    name = _tz_to_name(tzname) or DEFAULT_TZ_NAME or "UTC"
    name = str(name).strip() or "UTC"
    if ZoneInfo is not None:
        try:
            _ = ZoneInfo(name)
            return name
        except Exception:
            return "UTC"
    return name


def parse_mmdd(text: str) -> tuple[int, int]:
    t = (text or "").strip()
    if "-" not in t:
        raise ValueError("birthday.err.mmdd_format")
    m_s, d_s = [x.strip() for x in t.split("-", 1)]
    if not (m_s.isdigit() and d_s.isdigit()):
        raise ValueError("birthday.err.mmdd_digits")
    m, d = int(m_s), int(d_s)
    if not (1 <= m <= 12):
        raise ValueError("birthday.err.mmdd_month")
    if not (1 <= d <= 31):
        raise ValueError("birthday.err.mmdd_day")
    try:
        date(2000, m, d)  # leap-year baseline validation
    except Exception:
        raise ValueError("birthday.err.mmdd_invalid")
    return m, d


def today_in_tz(tzname: str) -> date:
    if ZoneInfo:
        return datetime.now(ZoneInfo(tzname)).date()
    return datetime.now(timezone.utc).date()


def is_users_birthday(today: date, month: int, day: int) -> bool:
    if month == 2 and day == 29:
        # Leap handling: celebrate on 2/28 if not a leap year
        try:
            date(today.year, 2, 29)
            is_leap = True
        except Exception:
            is_leap = False
        return (today.month, today.day) == (2, 29) or (
            not is_leap and (today.month, today.day) == (2, 28)
        )
    return today.month == month and today.day == day


# ---------- background service ----------


class BirthdayService:
    """
    Checks each guild every 30 minutes and posts birthday messages to a fixed channel.
    Message selection comes from ui.bday.select_birthday_message(user_id, closeness_level).
    """

    def __init__(self, bot: discord.Client):
        self.bot = bot
        self.loop = self._loop_task

    def start(self):
        model.ensure_tables()
        if not self.loop.is_running():
            self.loop.start()

    def stop(self):
        if self.loop.is_running():
            self.loop.cancel()

    @tasks.loop(minutes=30)
    async def _loop_task(self):
        await self.bot.wait_until_ready()
        for guild in list(getattr(self.bot, "guilds", []) or []):
            try:
                await self._check_guild(guild)
            except Exception:
                log.exception(
                    "birthday.guild_check_failed",
                    extra={"guild_id": getattr(guild, "id", None)},
                )

    async def _check_guild(self, guild: discord.Guild):
        entries = model.fetch_all_for_guild(guild.id)

        # Resolve target channel once per guild
        target_ch: Optional[discord.TextChannel] = None
        ch = guild.get_channel(BIRTHDAY_CHANNEL_ID)
        if isinstance(ch, discord.TextChannel):
            target_ch = ch
        fallback_ch = (
            guild.system_channel
            if isinstance(guild.system_channel, discord.TextChannel)
            else None
        )

        for b in entries:
            today = today_in_tz(b.tz)
            if not is_users_birthday(today, b.month, b.day):
                continue
            if b.last_year == today.year:
                continue

            level = (
                b.closeness_level
                if (b.closeness_level and 1 <= b.closeness_level <= 5)
                else 2
            )
            content = select_birthday_message(b.user_id, level)
            out = f"<@{b.user_id}> {content}"

            delivered = False

            if target_ch:
                try:
                    await target_ch.send(
                        out,
                        allowed_mentions=discord.AllowedMentions(
                            users=True, roles=False, everyone=False
                        ),
                    )
                    delivered = True
                except Exception as exc:
                    log.warning(
                        "birthday.post_failed",
                        extra={
                            "guild_id": guild.id,
                            "channel_id": target_ch.id,
                            "error": str(exc),
                        },
                    )

            if not delivered and fallback_ch:
                try:
                    await fallback_ch.send(
                        out,
                        allowed_mentions=discord.AllowedMentions(
                            users=True, roles=False, everyone=False
                        ),
                    )
                    delivered = True
                except Exception as exc:
                    log.warning(
                        "birthday.post_failed_fallback",
                        extra={
                            "guild_id": guild.id,
                            "channel_id": fallback_ch.id,
                            "error": str(exc),
                        },
                    )

            if delivered:
                try:
                    model.mark_congratulated(guild.id, b.user_id, today.year)
                except Exception:
                    log.exception(
                        "birthday.mark_failed",
                        extra={"guild_id": guild.id, "user_id": b.user_id},
                    )

    @_loop_task.before_loop
    async def _before(self):
        await self.bot.wait_until_ready()

    @_loop_task.error
    async def _on_error(self, err: Exception):
        log.exception("birthday.loop_error", exc_info=err)
