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
)

# For which tokens should a hyphen be treated as OPTIONAL in matching?
# onee-sama makes sense; I also allow it for nee-chan and summer-chan (practical).
HYPHEN_OPTIONAL = {
    "oneesama": True,  # matches oneesama / onee-sama
    "nee-chan": True,  # matches neechan / nee-chan
    "summer-chan": True,  # matches summerchan / summer-chan
    "thea": False,
    "mum": False,
}

# Fuzzy thresholds (edit distance) per canonical token (on normalized forms).
# Keep these tight to avoid noise. Distance is absolute, not ratio.
FUZZY_MAX_DIST = {
    "mum": 1,  # len=3 → allow one typo: "mim", "mum!"
    "thea": 1,  # len=4 → allow one typo
    "oneesama": 2,  # len=8 → allow 2 edits
    "nee-chan": 2,  # len=8 (w/o hyphen) → allow 2 edits
    "summer-chan": 2,  # longer → allow 2
}

# ============================================================
# Helpers
# ============================================================


def _elongatable_pattern(token: str, hyphen_optional: bool) -> str:
    """
    Build a regex that matches the token with *stretched letters*.
    Letters/digits become that char repeated 1+ times.
    Hyphens can be optional if configured.
    The whole thing is later wrapped with non-word boundaries.
    """
    parts = []
    for ch in token:
        if ch.isalnum():
            parts.append(re.escape(ch) + "+")  # 'a' -> 'a+'
        elif ch == "-" and hyphen_optional:
            parts.append("-?")  # optional hyphen
        else:
            parts.append(re.escape(ch))  # literal punctuation
    return "".join(parts)


# Build elongation regex alternation for all keywords.
ELONGATED_ALTS = "|".join(
    _elongatable_pattern(t, HYPHEN_OPTIONAL.get(t, False)) for t in BASE_KEYWORDS
)

# Whole-word (not inside other words) elongated pattern, case-insensitive.
ELONGATED_RE = re.compile(rf"(?<!\w)(?:{ELONGATED_ALTS})(?!\w)", re.IGNORECASE)

# Tokenizer for fuzzy fallback: grab word-like pieces including optional hyphens inside.
TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z\-]*")


def _normalize_for_fuzzy(s: str) -> str:
    """
    Normalize a candidate string for fuzzy compare:
      - lowercase
      - strip hyphens
      - collapse repeated letters: 'theaaa' -> 'thea', 'muuum' -> 'mum'
    """
    s = s.lower()
    s = s.replace("-", "")
    # collapse runs of the same letter: e.g., 'eeeee' -> 'e'
    s = re.sub(r"(.)\1+", r"\1", s)
    return s


def _canon_forms() -> dict[str, str]:
    """Canonical normalized forms of watch terms (w/o hyphen)."""
    out = {}
    for t in BASE_KEYWORDS:
        cn = _normalize_for_fuzzy(t)
        out[t] = cn
    return out


CANON = _canon_forms()


def _levenshtein(a: str, b: str) -> int:
    """Classic Levenshtein distance (iterative DP, O(len(a)*len(b)))."""
    if a == b:
        return 0
    if not a:
        return len(b)
    if not b:
        return len(a)
    # Ensure 'a' is the shorter for less memory.
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
      1) elongated whole-word regex hits, or
      2) fuzzy distance <= per-term threshold against any token (after normalization).
    """
    if not content:
        return False

    # Quick path: elongated regex (fast)
    if ELONGATED_RE.search(content):
        return True

    # Fuzzy fallback: check each word-like token
    # Example: "onee---sama" weird punctuation won't match elongated, but
    # normalization collapses it and Levenshtein will catch it if close.
    tokens = TOKEN_RE.findall(content)
    if not tokens:
        return False

    # Precompute canonical thresholds
    items = list(FUZZY_MAX_DIST.items())  # [(token, maxdist), ...]
    for raw in tokens:
        norm = _normalize_for_fuzzy(raw)
        if not norm:
            continue
        for base, maxd in items:
            base_norm = CANON[base]
            # If hyphen optional for that base, base_norm already has no hyphens.
            d = _levenshtein(norm, base_norm)
            if d <= maxd:
                return True
    return False


# ============================================================
# Cog
# ============================================================


class OwnerNotifyCog(commands.Cog):
    """DMs the bot owner when target names are mentioned (elongations+typos; safe boundaries)."""

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
            # Owner may not be fetched yet; silently ignore
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
