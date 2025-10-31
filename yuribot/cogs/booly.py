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

# Personalized (per-user) auto-replies are once per day
PERSONAL_COOLDOWN = 24 * 60 * 60  # 24h

# Mentions are rate-limited so people can't spam
MENTION_COOLDOWN = 1200  # seconds (20 min)

# IDs
ID_MONKE   = 994264143634907157
ID_MOM_1   = 444390742266347535
ID_MUM     = 49670556760408064
ID_NAT     = 852192029085139004
ID_NOVANE  = 1275539727096741930
ID_L       = 234732044175933441
ID_OOKAMI  = 278958673835851777
ID_BLEP    = 251914689913683970
ID_VIVI    = 315694140480421889
ID_ADDI    = 1143394906606424165
ID_BAGE    = 1149355492456538185

SPECIAL_IDS = {
    ID_MONKE, ID_MOM_1, ID_MUM, ID_NAT, ID_NOVANE, ID_L, ID_OOKAMI, ID_BLEP, ID_VIVI, ID_ADDI, ID_BAGE
}

# Channels where **personalized** auto-replies are disabled
EXCLUDED_CHANNEL_IDS = {
    1417965404004946141, 1417982528001933383, 1422486999671111711, 1417424779354574932,
    1417960610569916416, 1428158868843921429, 1417981392561770497, 1417983743624220732,
    1427744820863963230, 1420832231886422036, 1418204893629382757, 1427744882025300091,
    1420832036469473422, 1419936079158579222, 1418226340880056380, 1417424779354574936,
    
}

# =========================
# Emoji expansion
# =========================
# Use these to write pools with :name: or a::name: and expand right before sending.
EMOJI = {
    "gura_heart": "<:gura_heart:1432286456558391376>",
    "henyaHeart": "<:henyaHeart:1432286471837978645>",
    "sadcrydepression": "<:sadcrydepression:1432289105131081738>",
    "ping": "<a:ping:1432286407736557608>",
    "gimme_hug": "<a:gimme_hug:1275464256330006589>",
    "henyaNodder": "<a:henyaNodder:1432286485905801306>",
    "wavehi": "<a:wavehi:1432286440028639272>",
}

def expand_emoji_tokens(text: str) -> str:
    if not text:
        return text
    out = text
    for name, tag in EMOJI.items():
        out = out.replace(f":{name}:", tag).replace(f"a::{name}:", tag)
    return out


# =========================
# Message pools
# =========================

# General quips for mentions (anyone)
GENERAL_MENTION_POOL: List[str] = [
    "Hai hai ~",
    "hiya darling ~",
    "did you need me ?",
    "can I help ?",
    "Hey !",
    "huh, what ?",
    "Yes? :3",
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    
    ":ping:",    


]

# Slightly deferential if a mod/staff pings
MOD_MENTION_POOL: List[str] = [
    "am I in trouble ? :sadcrydepression:",
    "I've been good I swear !",
    "yes ? 😊",
    ":henyaHeart:",
    ":gura_heart:",
    ":henyaHeart:",
    ":gura_heart:",
    ":henyaHeart:",
    ":gura_heart:",
]

# Default cute lines for “other” special users
SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
]

# Per-user pools
MOM_1_POOL: List[str] = [
    "hi mom a::wavehi: im mostly behaving today ~",
    "hearts you ~ :gura_heart:",
    "fine ill eat something but sister has to eat too",
    "dont worry... I’ve only caused, like… two minor emotional damages today :henyaNodder:",
    "youd still love me if i accidentally ban someone right ? :sadcrydepression:",
    "a::gimme_hug:",
    "おはよう、お母さん ～！:henyaHeart:",
    "疲れた…",
    "お母さんは今日もかわいい！:gura_heart:",
    "hi mom pls dont scroll up or check my logs :gura_heart:",
]
MOM_2_POOL: List[str] = [
    "hi mom a::wavehi: I swear I've been good todayyy ~",
    "would you still love me if I was a worm ?",
    "look what I learned how to do today mom ! OH owwww :sadcrydepression:",
    "Nat hasn't eaten yet !",
    "mooooom Nat stayed up too late again",
    "a::gimme_hug:",
    "don't worry mom, I already banned everyone that said gachas arent real games ! :gura_heart:",
    "おはよう、お母さん ～！:henyaHeart:",
    "疲れた…",
    "お母さんは今日もかわいい！:gura_heart:",
    "hi mom pls dont scroll up or check my logs :gura_heart:",
]
NAT_POOL: List[str] = [
    "hai sister  :WaveHiHi:",
    "sister I did a thing, look!",
    "I’m totally not trying to impress you~ (I am.)",
    "can we play minecraft together",
]
NOVANE_POOL: List[str] = [
    "Hai Nova-nee !",
    "can I play with your phone ?",
    "ur such a cutie !",
    "I'm hungryyy will you make me something ?",
    "your hair is so pretty Nova-nee !",
]
L_POOL: List[str] = [
    "will you take me to six flags again plsssss ? :sadcrydepression:",
    "can I have an ice cream ? :gura_heart:",
    "HAI L ! a::wavehi:",
    "can I have an ice cream ? :gura_heart:",
    "will you read something to me next time ?",
    "I want up plss",
    "can I have some money ? :henyaNodder:",
]
BAGE_POOL: List[str] = [
    "*To me, you are a dream I yearn to hold and yet fear to lose. To you, I am someone who can come and go, and you won't pursue.*",
    "This was her precious treasure, regained.",
    "You can celebrate a 95% Valentine's Day.",
    "Je t'aime.",
    "我爱Bagelina阿姨! :henyaHeart:",
]
OOKAMI_POOL: List[str] = [
    "Hai ookami ! a::wavehi:",
    "do I get richer too ?",
    "I saw your favorite girls again !",
    "shouldn't you be asleep ?",
]
BLEP_POOL: List[str] = [
    "Hai blepblep ! a::wavehi:",
    "blepblep is so pretty ~ :gura_heart:",
    "ya you tell em blep ! a::henyaNodder:",
    "can I pet little blepblep again ?",
    "do you have any games on your phone ?",
    "yaaay blepblep is here ! :henyaHeart:",
]
VIVI_POOL: List[str] = [
    "hi aunt vivi a::wavehi: I promise I didn’t start this time",
    "can I hide behind you again :sadcrydepression:",
    "aunt vivi blink twice if you’re tired of everyone’s nonsense",
    "luv u :gura_heart:",
    "can we play some games vivivi :henyaNodder: ?",
]
ADDI_POOL: List[str] = [
    "good puppyyy ! :gura_heart:",
    "best puppy !!",
    "awww such a cutie. pat pat pat :gura_heart:",
    "you get extra pets for that one :henyaNodder:",
    "ok fine you earned a treat 😤",
    "woof woof !~",
    "arf arf I want pats too ! :gimme_hug:",
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
    last_auto_ts: Optional[int] = None      # last time a personalized auto fired
    last_key: Optional[str] = None          # last personalized line used
    last_mention_ts: Optional[int] = None   # last time Rinrin replied to a mention from this user

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
                        last_auto_ts    = blob.get("last_auto_ts"),
                        last_key        = blob.get("last_key"),
                        last_mention_ts = blob.get("last_mention_ts"),
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

def _has_mod_perms(member: discord.Member) -> bool:
    perms = getattr(member, "guild_permissions", None)
    if not perms:
        return False
    return any([
        perms.manage_guild,
        perms.manage_channels,
        perms.kick_members,
        perms.ban_members,
        perms.moderate_members,
        perms.administrator,
    ])

def _personal_pool_for(user_id: int) -> List[str]:
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
    if user_id == ID_VIVI:
        return VIVI_POOL
    if user_id == ID_ADDI:
        return ADDI_POOL    
    if user_id == ID_BAGE:
        return BAGE_POOL
    return SPECIAL_DEFAULT_POOL


# =========================
# Cog
# =========================

class UserAutoResponder(commands.Cog):
    """
    - Personalized replies (special users) fire once per 24h after any message
      (skips excluded channels).
    - Mentions (any user, any channel) always use general or mod quips (never personalized),
      and are rate-limited by MENTION_COOLDOWN.
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
        # Expand our emoji tokens before sending
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
        now = _now()
        is_hard = _mentioned_me(self.bot, message)

        # ===== Mentions (hard trigger) — any user, any channel; rate-limited =====
        if is_hard:
            if st.last_mention_ts and (now - st.last_mention_ts) < MENTION_COOLDOWN:
                return
            pool = MOD_MENTION_POOL if (member and _has_mod_perms(member)) else GENERAL_MENTION_POOL
            line = random.choice(pool) if pool else ""
            text = str(S(line)).strip()
            await self._safe_reply(message, text)
            st.last_mention_ts = now
            _save_state(self.state)
            return

        # ===== Personalized auto-replies for special users (24h) =====
        if uid in SPECIAL_IDS:
            if cid in EXCLUDED_CHANNEL_IDS:
                return
            last = st.last_auto_ts or 0
            if (now - last) >= PERSONAL_COOLDOWN:
                pool = _personal_pool_for(uid)
                line = random.choice(pool) if pool else ""
                text = str(S(line)).strip()
                await self._safe_reply(message, text)
                st.last_auto_ts = now
                st.last_key = line
                _save_state(self.state)
            return

        # Non-special & no mention → ignore
        return


async def setup(bot: commands.Bot):
    await bot.add_cog(UserAutoResponder(bot))
