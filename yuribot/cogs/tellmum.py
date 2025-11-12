from __future__ import annotations

import logging
import re
import discord
from discord.ext import commands

log = logging.getLogger(__name__)

# ============================================================
# Config
# ============================================================

# Base watch-terms (lowercase). Hyphen may be optional per term (see map below).
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

# For which tokens should a hyphen be treated as OPTIONAL in matching?
HYPHEN_OPTIONAL = {
    "oneesama": True,  # oneesama / onee-sama
    "nee-chan": True,  # neechan / nee-chan
    "summer-chan": True,  # summerchan / summer-chan
    "thea": False,
    "mum": False,
    "rin": False,
    "rinrin": False,
    "nat": False,
    "queen": False,
}

# Fuzzy thresholds (edit distance) per canonical token (on normalized forms).
# Keep these tight to avoid noise.
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
    """
    Build a regex that matches the token with stretched letters (char+).
    Hyphens can be optional if configured.
    Then we allow an optional plural/possessive suffix: 's | s' | s
    """
    parts = []
    for ch in token:
        if ch.isalnum():
            parts.append(re.escape(ch) + "+")  # 'a' -> 'a+'
        elif ch == "-" and hyphen_optional:
            parts.append("-?")  # optional hyphen
        else:
            parts.append(re.escape(ch))  # literal punctuation
    core = "".join(parts)
    # Optional plural/possessive suffix
    sfx = r"(?:'s|s'|s)?"
    return core + sfx


# Build elongation regex alternation for all keywords.
ELONGATED_ALTS = "|".join(
    _elongatable_pattern(t, HYPHEN_OPTIONAL.get(t, False)) for t in BASE_KEYWORDS
)

# Whole-word (not inside other words) elongated pattern, case-insensitive.
ELONGATED_RE = re.compile(rf"(?<!\w)(?:{ELONGATED_ALTS})(?!\w)", re.IGNORECASE)

# Tokenizer for fuzzy fallback:
# include internal hyphens and apostrophes so we see "onee-sama", "thea's", "mums'"
TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z\-']*")


def _normalize_for_fuzzy(s: str) -> str:
    """
    Normalize a candidate string for fuzzy compare:
      - lowercase, remove hyphens/apostrophes
      - collapse repeated letters: 'theaaa' -> 'thea', 'muuum' -> 'mum'
    """
    s = s.lower()
    s = s.replace("-", "").replace("'", "")
    s = re.sub(r"(.)\1+", r"\1", s)
    return s


def _strip_plural_possessive(norm: str) -> str:
    """
    Strip a trailing plural/possessive: 's, s', or s.
    Operates on *normalized* strings (no hyphens/apostrophes).
    """
    if not norm:
        return norm
    # Already removed apostrophes in normalization; plural/possessive reduces to trailing 's'
    if norm.endswith("s"):
        # Avoid chopping single-letter tokens or 'ss' accidental
        base = norm[:-1]
        if len(base) >= 2:
            return base
    return norm


def _canon_forms() -> dict[str, str]:
    """Canonical normalized forms of watch terms (no hyphens/apostrophes, no elongation)."""
    out = {}
    for t in BASE_KEYWORDS:
        out[t] = _normalize_for_fuzzy(t)
    return out


CANON = _canon_forms()


def _levenshtein(a: str, b: str) -> int:
    """Classic Levenshtein distance (iterative DP)."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    if len(a) > len(b):
        a, b = b, a
    prev = list(range(len(a) + 1))
    for j, bj in enumerate(b, start=1):
        cur = [j]
        for i, ai in enumerate(a, start=1):
            ins = cur[i - 1] + 1
            dele = prev[i] + 1
            sub = prev[i - 1] + (ai != bj)
            cur.append(min(ins, dele, sub))
        prev = cur
    return prev[-1]


def matches_keyword(content: str) -> bool:
    """
    True if:
      1) elongated whole-word regex hits (with optional plural/possessive), or
      2) fuzzy distance <= per-term threshold against any token (after normalization),
         also trying a version with plural/possessive stripped.
    """
    if not content:
        return False

    # Fast path: elongated regex
    if ELONGATED_RE.search(content):
        return True

    # Fuzzy path
    tokens = TOKEN_RE.findall(content)
    if not tokens:
        return False

    items = list(FUZZY_MAX_DIST.items())  # [(token, maxdist), ...]
    for raw in tokens:
        norm = _normalize_for_fuzzy(raw)
        if not norm:
            continue
        norm_stripped = _strip_plural_possessive(norm)

        for base, maxd in items:
            base_norm = CANON[base]

            d0 = _levenshtein(norm, base_norm)
            if d0 <= maxd:
                return True

            # Try with plural/possessive stripped (handles thea's/theas/queens/etc.)
            if norm_stripped != norm:
                d1 = _levenshtein(norm_stripped, base_norm)
                if d1 <= maxd:
                    return True
    return False


# ============================================================
# Cog
# ============================================================


class OwnerNotifyCog(commands.Cog):
    """DMs the bot owner when target names are mentioned (elongations+typos+plural/possessive)."""

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
        # Ignore bots and DMs
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
