from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, List, Optional

import discord
from discord.ext import commands

from ..strings import S

# =========================
# Storage / constants
# =========================

DATA_FILE = Path("./data/user_autoresponder.json")

DEFAULT_COOLDOWN = 12 * 60 * 60  # 12 hours
MOM_COOLDOWN = 6 * 60 * 60
NAT_COOLDOWN = 8 * 60 * 60
MONKE_COOLDOWN = DEFAULT_COOLDOWN

# IDs
ID_MONKE   = 994264143634907157
ID_MOM_1   = 444390742266347535
ID_MUM     = 49670556760408064
ID_NAT     = 852192029085139004
ID_NOVANE  = 1275539727096741930
ID_L       = 234732044175933441
ID_OOKAMI  = 278958673835851777
ID_BLEP    = 251914689913683970

SPECIAL_IDS = {
    ID_MONKE, ID_MOM_1, ID_MUM, ID_NAT, ID_NOVANE, ID_L, ID_OOKAMI, ID_BLEP
}

# Channels where auto-replies for special users are disabled
EXCLUDED_CHANNEL_IDS = {
    1417965404004946141, 1417982528001933383, 1422486999671111711, 1417424779354574932,
    1417960610569916416, 1428158868843921429, 1417981392561770497, 1417983743624220732,
    1427744820863963230, 1420832231886422036, 1418204893629382757, 1427744882025300091,
    1420832036469473422, 1419936079158579222, 1418226340880056380,
}

# Hard-trigger follow-up window for Monke (seconds)
MONKE_FOLLOWUP_WINDOW = 180  # 3 minutes

# Monke: after hard-trigger quip, if he replies, use one of these GIFs
MONKE_FOLLOWUP_GIFS: List[str] = [
    "https://tenor.com/view/anime-animation-vtuber-envtuber-vtubers-gif-4518672340074185795",
    "https://tenor.com/view/anime-pat-gif-22001993",
    "https://tenor.com/view/angel-beats-tachibana-kanade-tenshi-angel-girl-gif-150290166219802185",
    "https://tenor.com/view/shiideraii-gif-3312903941915865885",
    "https://tenor.com/view/anime-hug-anime-anime-girl-anime-girls-anime-girls-hugging-gif-26094816",
]

# =========================
# Message pools
# =========================

SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
    "am I in trouble ? :sadcrydepression:",
    "I've been good I swear !",
    "yes miss ? 😊",
]

NOVANE_POOL: List[str] = [
    "Hai Nova-nee !",
    "can I play with your phone ?",
    "ur such a cutie !",
    "I'm hungryyy will you make me something ?",
    "your hair is so pretty Nova-nee !",
]

GENERAL_MENTION_POOL: List[str] = [
    "Hai hai ~",
    "hiya darling ~",
    "did you need me ?",
    "can I help ?",
    "Hey !",
    "huh, what ?",
    "Yes? :3",
    ":ping:",
]

MOM_1_POOL: List[str] = [
    "hi mom a::wavehi: im mostly behaving today ~",
    "hearts you ~ :gura_heart: ",
    "fine ill eat something but sister has to eat too",
    "dont worry... I’ve only caused, like… two minor emotional damages today a::henyaNodder:",
    "youd still love me if i accidentally ban someone right ? :sadcrydepression: ",
    "a::gimme_hug:",
    "おはよう、お母さん ～！:henyaHeart:”",
    "疲れた…”",
    "お母さんは今日もかわいい！:gura_heart:",
    "hi mom pls dont scroll up or check my logs :gura_heart: ",
]

MOM_2_POOL: List[str] = [
    "hi mom a::wavehi: I swear I've been good todayyy ~",
    "would you still love me if I was a worm ?",
    "look what I learned how to do today mom ! OH owwww :sadcrydepression: ",
    "Nat hasn't eaten yet !",
    "mooooom Nat stayed up too late again",
    "a::gimme_hug:",
    "don't worry mom, I already banned everyone that said gachas arent real games ! :gura_heart: ",
    "おはよう、お母さん ～！:henyaHeart:”",
    "疲れた…”",
    "お母さんは今日もかわいい！:gura_heart:",
    "hi mom pls dont scroll up or check my logs :gura_heart: ",
]

L_POOL: List[str] = [
    "will you take me to six flags again plsssss ? :sadcrydepression: ",
    "HAI L ! a::wavehi:",
    "can I have an ice cream ? :gura_heart: ",
    "will you read something to me next time ?",
    "I want up plss ",
    "can I have some money ? a::henyaNodder:",
]

OOKAMI_POOL: List[str] = [
    "Hai ookami ! a::wavehi:",
    "do I get richer too ?",
    "I saw your favorite girls again !",
    "shouldn't you be asleep ?",
]

BLEP_POOL: List[str] = [
    "Hai blepblep ! a::wavehi:",
    "blepblep is so pretty ~ :gura_heart: ",
    "ya you tell em blep ! a::henyaNodder:",
    "can I pet little blepblep again ?",
    "do you have any games on your phone ?",
    "yaaay blepblep is here ! :henyaHeart: ",
]

NAT_POOL: List[str] = [
    "hai sister  :WaveHiHi:",
    "sister I did a thing, look!",
    "I’m totally not trying to impress you~ (I am.)",
    "can we play minecraft together",
]

MONKE_POOL: List[str] = [
    "wrong ~",
    "wrong again ~",
    "NU UH",
    "👶",
    "baby needs his bottle ?",
    "ooh ooh aah aah 🐒",
    "mom scary man is talking to me. ban him",
    "lil bro thinks he’s sooo cool",
    "🤡",
    "sit.",
    "sit. lil bro sit.",
    "really?",
    "lil bro really thinks he cooked with that one 💤",
    "woh... lil bro be yappin again 🙄",
    "this why mom likes me better ~",
    "feeling silly, might time you out later ~",
    "you better be good today monke or I'll tell on you",
    "LMAO",
    "hehe silly monke trying to talk again",
    "-# watch im going to time him out it will be so funny hehe",
    "you sound jealouss ~",
    "🇱  rip bozo 💀",
    "seriously?",
    "yikes 💀",
    "it's the monke again 🙄",
    "🙄",
    "oooo it's the slowest monke again",
    "nerd!",
]

# =========================
# State
# =========================

@dataclass
class GuildUserState:
    last_auto_ts: Optional[int] = None      # last time a cooldown-based auto fired
    last_key: Optional[str] = None          # last line used
    last_hard_quip_ts: Optional[int] = None # used for Monke mention follow-up GIF
    last_bot_msg_id: Optional[int] = None   # id of Rinrin's last hard-quip message to this user

# state[guild_id][user_id] = GuildUserState
StateType = Dict[str, Dict[str, GuildUserState]]

def _load_state() -> StateType:
    if DATA_FILE.exists():
        try:
            raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            out: StateType = {}
            for gid, users in (raw or {}).items():
                inner: Dict[str, GuildUserState] = {}
                for uid, blob in (users or {}).items():
                    inner[uid] = GuildUserState(
                        last_auto_ts = blob.get("last_auto_ts"),
                        last_key      = blob.get("last_key"),
                        last_hard_quip_ts = blob.get("last_hard_quip_ts"),
                        last_bot_msg_id   = blob.get("last_bot_msg_id"),
                    )
                out[gid] = inner
            return out
        except Exception:
            return {}
    return {}

def _save_state(state: StateType) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    wire = {
        gid: { uid: asdict(st) for uid, st in users.items() }
        for gid, users in state.items()
    }
    DATA_FILE.write_text(json.dumps(wire, indent=2), encoding="utf-8")

def _now() -> int:
    return int(time.time())

def _mentioned_me(bot: commands.Bot, msg: discord.Message) -> bool:
    u = getattr(bot, "user", None)
    return bool(u and u in msg.mentions)

def _pool_for(user_id: int) -> List[str]:
    if user_id == ID_MONKE:
        return MONKE_POOL
    if user_id == ID_MOM_1:
        return MOM_1_POOL
    if user_id == ID_MUM:
        return MOM_2_POOL
    if user_id == ID_NAT:
        return NAT_POOL
    if user_id == ID_NOVANE:
        return NOVANE_POOL
    if user_id == ID_L:
        return L_POOL
    if user_id == ID_OOKAMI:
        return OOKAMI_POOL
    if user_id == ID_BLEP:
        return BLEP_POOL
    return SPECIAL_DEFAULT_POOL

def _cooldown_for(user_id: int) -> int:
    if user_id == ID_MONKE:
        return MONKE_COOLDOWN
    if user_id in (ID_MOM_1, ID_MUM):
        return MOM_COOLDOWN
    if user_id == ID_NAT:
        return NAT_COOLDOWN
    return DEFAULT_COOLDOWN

# =========================
# Cog
# =========================

class UserAutoResponder(commands.Cog):
    """
    - Special users (by ID): auto-reply once per cooldown after ANY message they send,
      except in excluded channels.
    - Any user in any channel: mentioning Rinrin triggers an immediate hard-quip (no cooldown).
    - Monke: also gets a small mention follow-up GIF window.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: StateType = _load_state()

    def _st(self, gid: int, uid: int) -> GuildUserState:
        g = str(gid); u = str(uid)
        if g not in self.state:
            self.state[g] = {}
        if u not in self.state[g]:
            self.state[g][u] = GuildUserState()
        return self.state[g][u]

    async def _safe_reply(self, src: discord.Message, content: str) -> Optional[discord.Message]:
        if not content:
            return None
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
        st = self._st(gid, uid)
        now = _now()
        is_hard = _mentioned_me(self.bot, message)

        # ===== HARD TRIGGERS (mentions) — allowed anywhere, ignore cooldown =====
        if is_hard:
            pool = _pool_for(uid) if uid in SPECIAL_IDS else GENERAL_MENTION_POOL
            line = random.choice(pool) if pool else None
            sent = await self._safe_reply(message, str(S(line)).strip() if line else "")
            # Optional: monke mention follow-up GIF window
            if uid == ID_MONKE and sent is not None:
                st.last_hard_quip_ts = now
                st.last_bot_msg_id = sent.id
                _save_state(self.state)
            return

        # ===== AUTO REPLIES for SPECIAL USERS (cooldown-based) =====
        if uid in SPECIAL_IDS:
            # Skip auto checks in excluded channels
            if cid in EXCLUDED_CHANNEL_IDS:
                return

            cd = _cooldown_for(uid)
            last = st.last_auto_ts or 0
            if (now - last) >= cd:
                pool = _pool_for(uid)
                line = random.choice(pool) if pool else None
                await self._safe_reply(message, str(S(line)).strip() if line else "")
                st.last_auto_ts = now
                st.last_key = line
                _save_state(self.state)
            return

        # Not special and no mention → ignore
        return


async def setup(bot: commands.Bot):
    await bot.add_cog(UserAutoResponder(bot))
