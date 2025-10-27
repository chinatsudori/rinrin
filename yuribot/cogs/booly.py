from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import discord
from discord.ext import commands

from ..strings import S 

# === Config ===
TARGET_USER_ID = 994264143634907157
COOLDOWN_SECONDS = 24 * 60 * 60  # 24 hours
DATA_FILE = Path("./data/user_autoresponder.json")

RESPONSE_STRING_KEYS: List[str] = [
    # e.g., "pester.lines.1", "pester.lines.2", "pester.lines.3", ...
    # Add as many as you want:
    " Ur stinky",
    "🇱",
    # "woh… bro typed a whole essay to be wrong",
    # "youre lucky im bored enough right now to read that",
    # "be honest, you rehearsed in the mirror didnt you",
    # "not the dissertation 😭",
    # "lil bro out here fighting for his life in text form",
    # "imagine trying to start something and still losing against me",
    # "you done or should i grab popcorn",
    # "😭",
    "hey lil bro",
]

@dataclass
class GuildState:
    last_auto_ts: Optional[int] = None  # last automatic reply time (epoch seconds)
    last_key: Optional[str] = None      # last string key we used (avoid immediate repeats)

def _load_state() -> Dict[str, GuildState]:
    if DATA_FILE.exists():
        try:
            raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            out: Dict[str, GuildState] = {}
            for gid, blob in (raw or {}).items():
                out[gid] = GuildState(
                    last_auto_ts=blob.get("last_auto_ts"),
                    last_key=blob.get("last_key"),
                )
            return out
        except Exception:
            return {}
    return {}

def _save_state(state: Dict[str, GuildState]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    wire = {
        gid: {"last_auto_ts": st.last_auto_ts, "last_key": st.last_key}
        for gid, st in state.items()
    }
    DATA_FILE.write_text(json.dumps(wire, indent=2), encoding="utf-8")

def _now() -> int:
    return int(time.time())

def _pick_response(exclude_key: Optional[str]) -> Optional[tuple[str, str]]:
    """
    Returns (key, text) from RESPONSE_STRING_KEYS via S(key).
    Avoids exclude_key if possible. Skips keys that fail S() or resolve to empty.
    """
    keys = [k for k in RESPONSE_STRING_KEYS if k] or []
    if not keys:
        return None

    ordered = keys[:]
    random.shuffle(ordered)
    if exclude_key in ordered and len(ordered) > 1:
        ordered.remove(exclude_key)
        # put it at the end as a fallback
        ordered.append(exclude_key)

    for k in ordered:
        try:
            txt = str(S(k)).strip()
        except Exception:
            continue
        if txt:
            return (k, txt)

    return None

class UserAutoResponder(commands.Cog):
    """Responds to a specific user's messages with random lines, on a 224h cooldown,
    but always responds if the user directly mentions the bot (hard trigger)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: Dict[str, GuildState] = _load_state()

    def _get_guild_state(self, guild_id: int) -> GuildState:
        gid = str(guild_id)
        if gid not in self.state:
            self.state[gid] = GuildState()
        return self.state[gid]

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        # Basic guards
        if message.author.bot:
            return
        if message.guild is None:
            return
        if message.author.id != TARGET_USER_ID:
            return

        bot_user = getattr(self.bot, "user", None)
        is_hard_trigger = bool(bot_user and bot_user in message.mentions)

        # Cooldown check (per guild) unless hard-triggered by mention
        st = self._get_guild_state(message.guild.id)
        now = _now()

        if not is_hard_trigger:
            last = st.last_auto_ts or 0
            if now - last < COOLDOWN_SECONDS:
                return  # still cooling down for auto responses

        # Pick a response
        picked = _pick_response(exclude_key=st.last_key)
        if not picked:
            return  # nothing configured
        key, text = picked

        try:
            await message.reply(text, mention_author=False)
        except discord.HTTPException:
            # Fallback: send in channel (if reply failed for some reason)
            try:
                await message.channel.send(text)
            except Exception:
                return

        # Update state:
        # - For auto responses, set the cooldown timestamp
        # - For hard triggers, **do not** update cooldown so they can hard-trigger anytime
        st.last_key = key
        if not is_hard_trigger:
            st.last_auto_ts = now
        _save_state(self.state)

async def setup(bot: commands.Bot):
    await bot.add_cog(UserAutoResponder(bot))
