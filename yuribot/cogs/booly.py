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


class UserAutoResponder(commands.Cog):
    """
    Behavior:
    - Mentions (hard trigger): reply using general or mod pool; rate-limited by MENTION_COOLDOWN.
    - Personalized auto-replies (soft trigger): for users that have *personal lines in DB*,
      reply at most once per 24h (PERSONAL_COOLDOWN), skipping excluded channels.
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
        self.personal_pools = personal
        self.personal_default = personal_default

    def _st(self, gid: int, uid: int) -> GuildUserState:
        g = str(gid)
        u = str(uid)
        if g not in self.state:
            self.state[g] = {}
        if u not in self.state[g]:
            self.state[g][u] = GuildUserState()
        return self.state[g][u]

    async def _safe_reply(self, src: discord.Message, content: str) -> Optional[discord.Message]:
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

        # Mentions (hard trigger) – general/mod only
        if is_hard:
            if st.last_mention_ts and (now - st.last_mention_ts) < MENTION_COOLDOWN:
                return
            pool = self.mod_pool if (member and has_mod_perms(member) and self.mod_pool) else self.general_pool
            line = random.choice(pool) if pool else ""
            await self._safe_reply(message, str(S(line)).strip())
            st.last_mention_ts = now
            save_state(self.state)
            return

        # Personalized auto-reply (soft trigger): user must have personal lines in DB
        # NOTE: no static SPECIAL_IDS; this is fully DB-driven.
        personal_pool = self.personal_pools.get(uid)
        if personal_pool:  # user is "special" if they have rows in booly_messages (scope=personal, user_id=uid)
            if cid in EXCLUDED_CHANNEL_IDS:
                return
            last = st.last_auto_ts or 0
            if (now - last) >= PERSONAL_COOLDOWN:
                # fallback to global personal default pool if their user-specific pool exists but is empty
                pool = personal_pool if personal_pool else self.personal_default
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
