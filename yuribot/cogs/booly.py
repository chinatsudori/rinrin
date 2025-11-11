from __future__ import annotations

import random
from typing import Dict, List, Optional

import discord
from discord.ext import commands

from ..strings import S
from ..ui.booly import expand_emoji_tokens
from ..models import booly as booly_model
from ..utils.booly import (
    EXCLUDED_CHANNEL_IDS,
    MENTION_COOLDOWN,
    PERSONAL_COOLDOWN,
    GuildUserState,
    StateType,
    current_timestamp,
    has_mod_perms,
    load_state,
    mentioned_me,
    save_state,
)

# Hard exception: always send this when that user mentions the bot
ALWAYS_GIF_USER = 994264143634907157
ALWAYS_GIF_URL = (
    "https://tenor.com/view/sparkle-star-rail-laugh-gif-5535487387681154728"
)


class UserAutoResponder(commands.Cog):
    """
    Behavior:
    - Mentions (hard trigger):
        * If user has personal lines in DB -> use MOD pool
        * Else -> use GENERAL pool
        * Exception: user 994264143634907157 → always post the GIF above
      Mentions are rate-limited by MENTION_COOLDOWN per-user.
    - Personalized auto-replies (soft trigger):
        * For users that have personal lines in DB (not a static list)
        * Once per 24h (PERSONAL_COOLDOWN), skipping EXCLUDED_CHANNEL_IDS
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: StateType = load_state()
        self.general_pool: List[str] = []
        self.mod_pool: List[str] = []
        # user_id -> list[str]
        self.personal_pools: Dict[int, List[str]] = {}
        # global personal defaults (scope=personal, user_id is NULL)
        self.personal_default: List[str] = []
        self.reload_messages()

    def reload_messages(self) -> None:
        general, mod, personal, personal_default = booly_model.fetch_all_pools()
        self.general_pool = general
        self.mod_pool = mod
        # Ensure keys are ints (defensive)
        self.personal_pools = {int(k): v for k, v in personal.items()}
        self.personal_default = personal_default

    def _st(self, gid: int, uid: int) -> GuildUserState:
        g = str(gid)
        u = str(uid)
        if g not in self.state:
            self.state[g] = {}
        if u not in self.state[g]:
            self.state[g][u] = GuildUserState()
        return self.state[g][u]

    def _has_personal_cached(self, uid: int) -> bool:
        return bool(self.personal_pools.get(uid))

    def _ensure_personal_loaded(self, uid: int) -> bool:
        """If the user isn't in the in-memory map, query DB once and cache it."""
        if uid in self.personal_pools:
            return bool(self.personal_pools[uid])
        rows = booly_model.fetch_messages(booly_model.SCOPE_PERSONAL, uid)
        self.personal_pools[uid] = [m.content for m in rows] if rows else []
        return bool(self.personal_pools[uid])

    async def _safe_reply(
        self, src: discord.Message, content: str
    ) -> Optional[discord.Message]:
        if not content:
            return None
        content = expand_emoji_tokens(content)
        try:
            return await src.reply(content, mention_author=False)
        except discord.HTTPException:
            try:
                return await src.channel.send(content)
            except Exception:
                return None

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or message.guild is None:
            return

        gid = message.guild.id
        uid = message.author.id
        cid = message.channel.id
        member = message.author if isinstance(message.author, discord.Member) else None

        st = self._st(gid, uid)
        now = current_timestamp()
        is_hard = mentioned_me(self.bot, message)

        # Mentions (hard trigger)
        if is_hard:
            # Special-case GIF user
            if uid == ALWAYS_GIF_USER:
                if (
                    not st.last_mention_ts
                    or (now - st.last_mention_ts) >= MENTION_COOLDOWN
                ):
                    await self._safe_reply(message, ALWAYS_GIF_URL)
                    st.last_mention_ts = now
                    save_state(self.state)
                return
            if uid == 614545005129760799:
                if (
                    not st.last_mention_ts
                    or (now - st.last_mention_ts) >= MENTION_COOLDOWN
                ):
                    await self._safe_reply(message, ALWAYS_GIF_URL)
                    st.last_mention_ts = now
                    save_state(self.state)
                return

            if st.last_mention_ts and (now - st.last_mention_ts) < MENTION_COOLDOWN:
                return

            # Choose pool based on whether user has personal lines in DB
            is_personal = self._has_personal_cached(
                uid
            ) or self._ensure_personal_loaded(uid)
            use_mod_pool = is_personal and bool(self.mod_pool)

            pool = self.mod_pool if use_mod_pool else self.general_pool
            # For moderators, your original code forced mod pool; we keep the new rule you asked for:
            # pool selection is DB-driven, not role-driven.

            line = random.choice(pool) if pool else ""
            await self._safe_reply(message, str(S(line)).strip())
            st.last_mention_ts = now
            save_state(self.state)
            return

        # Personalized auto-replies (soft trigger, 24h)
        # Qualify purely by DB presence (not a static SPECIAL_IDS list)
        if self._has_personal_cached(uid) or self._ensure_personal_loaded(uid):
            if cid in EXCLUDED_CHANNEL_IDS:
                return
            last = st.last_auto_ts or 0
            if (now - last) >= PERSONAL_COOLDOWN:
                pool = self.personal_pools.get(uid) or self.personal_default
                line = random.choice(pool) if pool else ""
                await self._safe_reply(message, str(S(line)).strip())
                st.last_auto_ts = now
                st.last_key = line
                save_state(self.state)
            return

        # Non-special & no mention -> ignore
        return


async def setup(bot: commands.Bot):
    await bot.add_cog(UserAutoResponder(bot))
