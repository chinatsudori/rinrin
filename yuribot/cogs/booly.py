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
MOM_COOLDOWN = 6 * 60 * 60       # "mom" can ping more often
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

# Monke: when cooldown expired, respond to his **second** message with this GIF then start cooldown
MONKE_SECOND_MESSAGE_GIF = "https://tenor.com/view/sparkle-star-rail-laugh-gif-5535487387681154728"

# =========================
# Message pools
# =========================

# General “special users” default pool (used by unspecified special users)
SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
    "am I in trouble ? :sadcrydepression:",
    "I've been good I swear !",
    "yes miss ? 😊",
]

# Novane
NOVANE_POOL: List[str] = [
    "Hai Nova-nee !",
    "can I play with your phone ?",
    "ur such a cutie !",
    "I'm hungryyy will you make me something ?",
    "your hair is so pretty Nova-nee !",
]

# Pings from general members
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

# Mom (4443…)
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

# Mum (4967…)
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

# L
L_POOL: List[str] = [
    "will you take me to six flags again plsssss ? :sadcrydepression: ",
    "HAI L ! a::wavehi:",
    "can I have an ice cream ? :gura_heart: ",
    "will you read something to me next time ?",
    "I want up plss ",
    "can I have some money ? a::henyaNodder:",
]

# Ookami
OOKAMI_POOL: List[str] = [
    "Hai ookami ! a::wavehi:",
    "do I get richer too ?",
    "I saw your favorite girls again !",
    "shouldn't you be asleep ?",
]

# Blep
BLEP_POOL: List[str] = [
    "Hai blepblep ! a::wavehi:",
    "blepblep is so pretty ~ :gura_heart: ",
    "ya you tell em blep ! a::henyaNodder:",
    "can I pet little blepblep again ?",
    "do you have any games on your phone ?",
    "yaaay blepblep is here ! :henyaHeart: ",
]

# Nat (onee-chan)
NAT_POOL: List[str] = [
    "hai sister  :WaveHiHi:",
    "sister I did a thing, look!",
    "I’m totally not trying to impress you~ (I am.)",
    "can we play minecraft together",
]

# Monke (standard quips for normal / hard triggers)
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
    last_auto_ts: Optional[int] = None          # last time a cooldown-based auto fired
    last_key: Optional[str] = None              # last line used
    monke_since_reset_msgs: int = 0             # messages since cooldown expired (monke only)
    last_hard_quip_ts: Optional[int] = None     # last time we hard-responded to mention (monke)
    last_bot_msg_id: Optional[int] = None       # id of bot msg sent on last hard quip (monke)

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
                        monke_since_reset_msgs = int(blob.get("monke_since_reset_msgs") or 0),
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

def _get_pool_for_user(user_id: int) -> List[str]:
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

def _cooldown_for_user(user_id: int) -> int:
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
    Per-user autoresponder with per-user cooldowns.

    - Any user: if they mention Rinrin, respond immediately (hard trigger), ignoring cooldown.
      * If it's Monke, set a short follow-up window; if he replies, send a follow-up GIF.

    - Monke special: when cooldown has expired, wait for his **second** message (no mention)
      and respond with MONKE_SECOND_MESSAGE_GIF, then **start cooldown**.

    - Everyone else (special list): when cooldown has expired, respond once with a random line
      from their pool and start cooldown.

    - Anyone else mentioning Rinrin (not in special routing) gets a reply from GENERAL_MENTION_POOL.
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state: StateType = _load_state()

    def _get_user_state(self, guild_id: int, user_id: int) -> GuildUserState:
        gid = str(guild_id)
        uid = str(user_id)
        if gid not in self.state:
            self.state[gid] = {}
        if uid not in self.state[gid]:
            self.state[gid][uid] = GuildUserState()
        return self.state[gid][uid]

    async def _safe_send_reply(self, src: discord.Message, content: str) -> Optional[discord.Message]:
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
        # Basic guards
        if message.author.bot or message.guild is None:
            return

        guild_id = message.guild.id
        author_id = message.author.id
        st = self._get_user_state(guild_id, author_id)
        now = _now()
        is_hard = _mentioned_me(self.bot, message)

        SPECIAL_IDS = {
            ID_MONKE, ID_MOM_1, ID_MUM, ID_NAT, ID_NOVANE, ID_L, ID_OOKAMI, ID_BLEP
        }

        # ===== HARD TRIGGERS (mentions) =====
        if is_hard:
            pool = _get_pool_for_user(author_id) if author_id in SPECIAL_IDS else GENERAL_MENTION_POOL
            if pool:
                line = random.choice(pool)
                sent = await self._safe_send_reply(message, str(S(line)).strip())
                # For hard trigger, do NOT touch cooldowns.
                if author_id == ID_MONKE and sent is not None:
                    # Start follow-up window for Monke (if he replies soon, send a follow-up GIF)
                    st.last_hard_quip_ts = now
                    st.last_bot_msg_id = sent.id
                    _save_state(self.state)
            return

        #
        # --- MONKE special logic ---
        if author_id == ID_MONKE:
            cd = _cooldown_for_user(author_id)
            last = st.last_auto_ts or 0

            if now - last >= cd:
                # Cooldown expired → wait for his SECOND message, then reply with laugh GIF and start cooldown
                st.monke_since_reset_msgs += 1
                if st.monke_since_reset_msgs >= 2:
                    await self._safe_send_reply(message, MONKE_SECOND_MESSAGE_GIF)
                    st.last_auto_ts = now
                    st.monke_since_reset_msgs = 0
                    _save_state(self.state)
                else:
                    _save_state(self.state)  # record first post after cooldown
            else:
                # Cooldown active, but if within follow-up window after a hard quip, send a follow-up GIF
                if st.last_hard_quip_ts and (now - st.last_hard_quip_ts) <= MONKE_FOLLOWUP_WINDOW:
                    gif = random.choice(MONKE_FOLLOWUP_GIFS)
                    await self._safe_send_reply(message, gif)
                    st.last_hard_quip_ts = None
                    st.last_bot_msg_id = None
                    _save_state(self.state)
            return

        # --- Everyone else in SPECIAL_IDS (cooldown-based single reply) ---
        if author_id in {ID_MOM_1, ID_MUM, ID_NAT, ID_NOVANE, ID_L, ID_OOKAMI, ID_BLEP}:
            if author_id == ID_MOM_1:
                pool = MOM_1_POOL
            elif author_id == ID_MUM:
                pool = MOM_2_POOL
            elif author_id == ID_NAT:
                pool = NAT_POOL
            elif author_id == ID_NOVANE:
                pool = NOVANE_POOL
            elif author_id == ID_L:
                pool = L_POOL
            elif author_id == ID_OOKAMI:
                pool = OOKAMI_POOL
            elif author_id == ID_BLEP:
                pool = BLEP_POOL
            else:
                pool = SPECIAL_DEFAULT_POOL

            cd = _cooldown_for_user(author_id)
            last = st.last_auto_ts or 0
            if (now - last) >= cd and pool:
                line = random.choice(pool)
                await self._safe_send_reply(message, str(S(line)).strip())
                st.last_auto_ts = now
                st.last_key = line
                _save_state(self.state)
            return

        # Not special and no mention → ignore
        return


async def setup(bot: commands.Bot):
    await bot.add_cog(UserAutoResponder(bot))
