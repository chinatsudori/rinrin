from __future__ import annotations

from typing import Dict, List, Tuple

__all__ = [
    "ID_MONKE",
    "ID_MOM_1",
    "ID_MUM",
    "ID_NAT",
    "ID_NOVANE",
    "ID_L",
    "ID_OOKAMI",
    "ID_BLEP",
    "ID_VIVI",
    "ID_ADDI",
    "ID_BAGE",
    "SPECIAL_IDS",
    "EXCLUDED_CHANNEL_IDS",
    "SPECIAL_DEFAULT_POOL",
    "GENERAL_MENTION_MESSAGES",
    "MOD_MENTION_MESSAGES",
    "PERSONAL_POOLS",
    "DEFAULT_BOOLY_ROWS",
]

# Personalized user IDs
ID_MONKE = 994264143634907157
ID_MOM_1 = 444390742266347535
ID_MUM = 49670556760408064
ID_NAT = 852192029085139004
ID_NOVANE = 1275539727096741930
ID_L = 234732044175933441
ID_OOKAMI = 278958673835851777
ID_BLEP = 251914689913683970
ID_VIVI = 315694140480421889
ID_ADDI = 1143394906606424165
ID_BAGE = 1149355492456538185

SPECIAL_IDS = {
    ID_MONKE,
    ID_MOM_1,
    ID_MUM,
    ID_NAT,
    ID_NOVANE,
    ID_L,
    ID_OOKAMI,
    ID_BLEP,
    ID_VIVI,
    ID_ADDI,
    ID_BAGE,
}

# Channels where personalized auto replies are disabled
EXCLUDED_CHANNEL_IDS = {
    1417965404004946141,
    1417982528001933383,
    1422486999671111711,
    1417424779354574932,
    1417960610569916416,
    1428158868843921429,
    1417981392561770497,
    1417983743624220732,
    1427744820863963230,
    1420832231886422036,
    1418204893629382757,
    1427744882025300091,
    1420832036469473422,
    1419936079158579222,
    1418226340880056380,
    1417424779354574936,
}

GENERAL_MENTION_MESSAGES: List[str] = [
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

MOD_MENTION_MESSAGES: List[str] = [
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

SPECIAL_DEFAULT_POOL: List[str] = [
    ":henyaHeart:",
    ":gura_heart:",
]

PERSONAL_POOLS: Dict[int, List[str]] = {
    ID_MOM_1: [
        "hi mom a::wavehi: im mostly behaving today ~",
        "hearts you ~ :gura_heart:",
        "fine ill eat something but sister has to eat too",
        "dont worry... I've only caused, like… two minor emotional damages today :henyaNodder:",
        "youd still love me if i accidentally ban someone right ? :sadcrydepression:",
        "a::gimme_hug:",
        "おはよう、お母さん ～！:henyaHeart:",
        "疲れた…",
        "お母さんは今日もかわいい！:gura_heart:",
        "hi mom pls dont scroll up or check my logs :gura_heart:",
    ],
    ID_MUM: [
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
    ],
    ID_NAT: [
        "hai sister  :WaveHiHi:",
        "sister I did a thing, look!",
        "I'm totally not trying to impress you~ (I am.)",
        "can we play minecraft together",
    ],
    ID_NOVANE: [
        "Hai Nova-nee !",
        "can I play with your phone ?",
        "ur such a cutie !",
        "I'm hungryyy will you make me something ?",
        "your hair is so pretty Nova-nee !",
    ],
    ID_L: [
        "will you take me to six flags again plsssss ? :sadcrydepression:",
        "can I have an ice cream ? :gura_heart:",
        "HAI L ! a::wavehi:",
        "can I have an ice cream ? :gura_heart:",
        "will you read something to me next time ?",
        "I want up plss",
        "can I have some money ? :henyaNodder:",
    ],
    ID_BAGE: [
        "*To me, you are a dream I yearn to hold and yet fear to lose. To you, I am someone who can come and go, and you won't pursue.*",
        "*It wasn't because you were perfect that I loved you. It was because I loved you... that you were perfect to me.*",
        "*This was her precious treasure, regained.*",
        "*You can celebrate a 95% Valentine's Day.*",
        "*Je t'aime.*",
        "我爱Bagelina阿姨! :henyaHeart:",
    ],
    ID_OOKAMI: [
        "Hai ookami ! a::wavehi:",
        "do I get richer too ?",
        "I saw your favorite girls again !",
        "shouldn't you be asleep ?",
    ],
    ID_BLEP: [
        "Hai blepblep ! a::wavehi:",
        "blepblep is so pretty ~ :gura_heart:",
        "ya you tell em blep ! a::henyaNodder:",
        "can I pet little blepblep again ?",
        "do you have any games on your phone ?",
        "yaaay blepblep is here ! :henyaHeart:",
    ],
    ID_VIVI: [
        "hi aunt vivi a::wavehi: I promise I didn't start this time",
        "can I hide behind you again :sadcrydepression:",
        "aunt vivi blink twice if you're tired of everyone's nonsense",
        "luv u :gura_heart:",
        "can we play some games vivivi :henyaNodder: ?",
    ],
    ID_ADDI: [
        "good puppyyy ! :gura_heart:",
        "best puppy !!",
        "awww such a cutie. pat pat pat :gura_heart:",
        "you get extra pets for that one :henyaNodder:",
        "ok fine you earned a treat 😤",
        "woof woof !~",
        "arf arf I want pats too ! :gimme_hug:",
    ],
    ID_MONKE: [
        "wrong ~",
        "wrong again ~",
        "NU UH",
        "👶",
        "baby needs his bottle ?",
        "ooh ooh aah aah 🐒",
        "mom scary man is talking to me. ban him",
        "lil bro thinks he's sooo cool",
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
    ],
}

DEFAULT_BOOLY_ROWS: Tuple[Tuple[str, int | None, str], ...] = (
    *[("mention_general", None, msg) for msg in GENERAL_MENTION_MESSAGES],
    *[("mention_mod", None, msg) for msg in MOD_MENTION_MESSAGES],
    *[
        ("personal", user_id, msg)
        for user_id, pool in PERSONAL_POOLS.items()
        for msg in pool
    ],
)
