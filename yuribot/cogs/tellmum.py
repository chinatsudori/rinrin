from __future__ import annotations

import logging
import re
import discord
from discord.ext import commands

log = logging.getLogger(__name__)

# ============================================================
# Config
# ============================================================

BASE_KEYWORDS = (
    "mum",
    "thea",
    "summer-chan",
    "oneesama",
    "nee-chan",
    "rin",
    "rinrin",
    "nat",
    "queen",
)

HYPHEN_OPTIONAL = {
    "oneesama": True,
    "nee-chan": True,
    "summer-chan": True,
    "thea": False,
    "mum": False,
    "rin": False,
    "rinrin": False,
    "nat": False,
    "queen": False,
}

FUZZY_MAX_DIST = {
    "mum": 1,
    "thea": 1,
    "oneesama": 2,
    "nee-chan": 2,
    "summer-chan": 2,
    "rin": 1,
    "rinrin": 1,
    "nat": 1,
    "queen": 1,
}

# ============================================================
# Helpers
# ============================================================


def _elongatable_pattern(token: str, hyphen_optional: bool) -> str:
    parts = []
    for ch in token:
        if ch.isalnum():
            parts.append(re.escape(ch) + "+")
        elif ch == "-" and hyphen_optional:
            parts.append("-?")
        else:
            parts.append(re.escape(ch))
    core = "".join(parts)
    sfx = r"(?:'s|s'|s)?"
    return core + sfx


ELONGATED_ALTS = "|".join(
    _elongatable_pattern(t, HYPHEN_OPTIONAL.get(t, False)) for t in BASE_KEYWORDS
)
ELONGATED_RE = re.compile(rf"(?<!\w)(?:{ELONGATED_ALTS})(?!\w)", re.IGNORECASE)

TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z\-']*")


def _normalize_for_fuzzy(s: str) -> str:
    s = s.lower().replace("-", "").replace("'", "")
    s = re.sub(r"(.)\1+", r"\1", s)
    return s


def _strip_plural_possessive(norm: str) -> str:
    if norm.endswith("s") and len(norm) >= 3:
        return norm[:-1]
    return norm


def _canon_forms() -> dict[str, str]:
    return {t: _normalize_for_fuzzy(t) for t in BASE_KEYWORDS}


CANON = _canon_forms()


def _lev(a: str, b: str) -> int:
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) > len(b):
        a, b = b, a
    prev = list(range(len(a) + 1))
    for bj in b:
        cur = [prev[0] + 1]
        for i, ai in enumerate(a, start=1):
            ins = cur[i - 1] + 1
            dele = prev[i] + 1
            sub = prev[i - 1] + (ai != bj)
            cur.append(min(ins, dele, sub))
        prev = cur
    return prev[-1]


def _short_token_guard(token_norm: str, base_norm: str) -> bool:
    """
    Extra constraints for short bases (len<=4) to prevent junk like:
      thea ~ the, rin ~ in, mum ~ um, etc.
    """
    if len(base_norm) <= 4:
        # token must be at least as long as base, and share first letter
        if len(token_norm) < len(base_norm):
            return False
        if not token_norm or token_norm[0] != base_norm[0]:
            return False
    return True


def matches_keyword(content: str) -> bool:
    if not content:
        return False

    # 1) precise/fast path
    if ELONGATED_RE.search(content):
        return True

    # 2) fuzzy (constrained)
    tokens = TOKEN_RE.findall(content)
    if not tokens:
        return False

    items = list(FUZZY_MAX_DIST.items())
    for raw in tokens:
        norm = _normalize_for_fuzzy(raw)
        if len(norm) < 3:  # ignore ultra-short tokens
            continue
        norm_stripped = _strip_plural_possessive(norm)

        for base, maxd in items:
            base_norm = CANON[base]

            # guard for short bases
            if not _short_token_guard(norm, base_norm):
                # try stripped too
                if not _short_token_guard(norm_stripped, base_norm):
                    continue

            # check full norm
            d0 = _lev(norm, base_norm)
            if d0 <= maxd:
                # log.debug("match via fuzzy: %r ~ %r (d=%d)", norm, base_norm, d0)
                return True

            # check stripped (plural/possessive)
            if norm_stripped != norm:
                d1 = _lev(norm_stripped, base_norm)
                if d1 <= maxd:
                    # log.debug("match via fuzzy-stripped: %r ~ %r (d=%d)", norm_stripped, base_norm, d1)
                    return True

    return False


# ============================================================
# Cog
# ============================================================


class OwnerNotifyCog(commands.Cog):
    """DMs the bot owner when target names are mentioned (elongations+typos+plural/possessive, low-noise)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.owner: discord.User | None = None
        self.bot.loop.create_task(self.fetch_owner_once())

    async def fetch_owner_once(self):
        await self.bot.wait_until_ready()
        try:
            app_info = await self.bot.application_info()
            self.owner = app_info.owner
            if self.owner:
                log.info(
                    f"OwnerNotifyCog: Owner found and set to {self.owner} ({self.owner.id})"
                )
            else:
                log.warning("OwnerNotifyCog: Could not find bot owner.")
        except Exception as e:
            log.error(f"OwnerNotifyCog: Failed to fetch bot owner: {e}")

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None:
            return
        if not self.owner:
            return

        content = message.content or ""
        if not matches_keyword(content):
            return

        try:
            embed = discord.Embed(
                title="Keyword Mention Notification",
                description=content,
                color=discord.Color.blue(),
                timestamp=message.created_at,
            )
            embed.set_author(
                name=f"{message.author} ({message.author.id})",
                icon_url=message.author.display_avatar.url,
            )
            embed.add_field(
                name="Source",
                value=f"**Server:** {message.guild.name}\n**Channel:** <#{message.channel.id}>",
                inline=False,
            )
            embed.add_field(
                name="Jump to Message",
                value=f"[Click Here]({message.jump_url})",
                inline=False,
            )
            await self.owner.send(embed=embed)
        except discord.Forbidden:
            log.warning(
                f"OwnerNotifyCog: Could not send DM to owner {getattr(self.owner, 'id', 'unknown')}. (DMs may be closed)"
            )
        except Exception as e:
            log.exception(f"OwnerNotifyCog: Failed to send DM: {e}")


async def setup(bot: commands.Bot):
    await bot.add_cog(OwnerNotifyCog(bot))
