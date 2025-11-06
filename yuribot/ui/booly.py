from __future__ import annotations

from typing import Dict, List

from ..utils.booly import (
    ID_ADDI,
    ID_BAGE,
    ID_BLEP,
    ID_L,
    ID_MOM_1,
    ID_MONKE,
    ID_MUM,
    ID_NAT,
    ID_NOVANE,
    ID_OOKAMI,
    ID_VIVI,
)

__all__ = [
    "EMOJI",
    "expand_emoji_tokens",
    "GENERAL_MENTION_POOL",
    "MOD_MENTION_POOL",
    "personal_pool_for",
]

# =========================
# Emoji expansion
# =========================
# Use these to write pools with :name: or a::name: and expand right before sending.
EMOJI: Dict[str, str] = {
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
    "yes ? ??",
    ":henyaHeart:",
    ":gura_heart:",
    ":henyaHeart:",
    ":gura_heart:",
    ":henyaHeart:",
    ":gura_heart:",
]

# Default cute lines for "other" special users
SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
]

# ===== Personalized pools =====
SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
]

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
    "*It wasn't because you were perfect that I loved you. It was because I loved you... that you were perfect to me.*",
    "*This was her precious treasure, regained.*",
    "*You can celebrate a 95% Valentine's Day.*",
    "*Je t'aime.*",
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


_PERSONAL_POOLS: Dict[int, List[str]] = {
    ID_MONKE: MONKE_POOL,
    ID_MOM_1: MOM_1_POOL,
    ID_MUM: MOM_2_POOL,
    ID_NAT: NAT_POOL,
    ID_NOVANE: NOVANE_POOL,
    ID_L: L_POOL,
    ID_OOKAMI: OOKAMI_POOL,
    ID_BLEP: BLEP_POOL,
    ID_VIVI: VIVI_POOL,
    ID_ADDI: ADDI_POOL,
    ID_BAGE: BAGE_POOL,
}


def personal_pool_for(user_id: int) -> List[str]:
    return _PERSONAL_POOLS.get(user_id, SPECIAL_DEFAULT_POOL)

