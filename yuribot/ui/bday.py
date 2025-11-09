from __future__ import annotations

import random
from typing import Dict, List

__all__ = [
    "RINRIN_BIRTHDAY_LEVELS",
    "RINRIN_BIRTHDAY_SPECIALS",
    "select_birthday_message",
]

# ------------------------------------------------------------
# Closeness levels (1–5)
# 1 = distant/teasing (monke), 5 = inner circle.
# ------------------------------------------------------------

RINRIN_BIRTHDAY_LEVELS: Dict[int, List[str]] = {
    1: [
        "oh look who aged again 🙄",
        "wow… another year of crimes huh",
        "you  survived another orbit 🎉",
        "you’re still here ? unbelievable.",
        "happy birthday i guess ~",
        "fine. happy birthday mmk",
    ],
    2: [
        "happy birthday you silly thing ~ :henyaHeart:",
        "hope your cake doesn’t burn this time hehe",
        "yay it’s your day ! eat something good okay ?",
        "birthday time ! don’t break anything this year pls :sadcrydepression:",
        "happy spawn day ~",
        "another lap around the sun woohoo :gura_heart:",
    ],
    3: [
        "happy birthdayyy !! im proud of you for surviving another year hehe :gura_heart:",
        "birthday hugs for youuu :gimme_hug: don’t make me cry okay ?",
        "yay yay it’s your day ! cake or riot >:3",
        "you get one free wish. don’t waste it on gacha luck.",
        "happy bday !! now go do something nice for yourself okay ?",
        "luv u lots today (but still gonna tease you tomorrow) :henyaHeart:",
    ],
    4: [
        "happy birthday !! you deserve every sweet thing today :gura_heart:",
        "i made a wish for you ~ (it’s a secret tho hehe)",
        "you’ve grown so much… i’m really proud of you :henyaHeart:",
        "thank you for being here with me another year 💕",
        "sending you all the cuddles and chaos today :gimme_hug:",
        "your smile’s my favorite gift actually hehe. happy birthday !!",
    ],
    5: [
        "happy birthday, luv u ~ :henyaHeart:",
        "you make my little world brighter every day 💖",
        "thank you for being you. i mean it.",
        "its your birthday yaay. if i could, i’d wrap you in a giant blanket forever hehe",
    ],
}

# ------------------------------------------------------------
# Personalized pools override the level pool when user_id matches.
# (IDs come from your ecosystem; kept here to keep UI self-contained.)
# ------------------------------------------------------------

ID_MOM_1 = 444390742266347535
ID_MUM = 49670556760408064
ID_NAT = 852192029085139004  # older sister
ID_VIVI = 315694140480421889  # aunt vivi
ID_BLEP = 251914689913683970
ID_ADDI = 1143394906606424165  # puppy
ID_NOVANE = 1275539727096741930  # nova-nee
ID_L = 234732044175933441
ID_OOKAMI = 278958673835851777
ID_BAGE = 1149355492456538185  # auntie bage
ID_ASTA = 91598353833410560  # asta-nee
ID_HARUHIME = 224288852494385155

RINRIN_BIRTHDAY_SPECIALS: Dict[int, List[str]] = {
    # Mom
    ID_MOM_1: [
        "happy birthday mom !! :wavehi: i made a mess but it’s okay ‘cause it’s your day ~ :gura_heart:",
        "momm it’s your birthday !! eat cake and don’t scold me today okie ? :henyaHeart:",
        "happy birthday mom ~ i love you big much :gimme_hug:",
        "yay it’s mom day !! thank you for always taking care of me even when I explode things hehe",
        "お誕生日おめでとう、お母さん！:henyaHeart: i hope today is super sparkly ~",
    ],
    # Mum
    ID_MUM: [
        "happy birthday mum !! :henyaHeart: i promise i’ve been good today (mostly) ~",
        "yayyy mum’s birthday !! can i have cake too ? pleaaase :sadcrydepression:",
        "お誕生日おめでとう、ママ！you’re the prettiest one as always :gura_heart:",
        "mum mum look ! i made confetti!! ...oh. oopies. :sadcrydepression:",
        "happy birthday my favorite mum in all universes ~ :henyaHeart:",
    ],
    # Nat (older sister)
    ID_NAT: [
        "HAPPY BIRTHDAY SISTER !! :henyaHeart: can i steal a bite of your cake ~",
        "sisterrr it’s your dayyy !!! can we play something later ? :gura_heart:",
        "yay it’s my cool sister’s birthday !! you’re the besttt :henyaNodder:",
        "nat-nee! happy birthday !! i love you even when you steal my snacks :sadcrydepression:",
        "birthday timeee !! thank you for always protecting me, sister :gura_heart:",
    ],
    # Aunt Vivi
    ID_VIVI: [
        "happy birthday aunt vivi !! a::wavehi: i promise i didn’t start anything today (probably)",
        "aunt vivi it’s your birthday !!! do i get cake privileges ?? :henyaNodder:",
        "happy bday vivi !! luv u lots ~ you’re my safe place always :gura_heart:",
        "yay aunt vivi day ! i made you sparkly chaos confetti ~ :henyaHeart:",
        "お誕生日おめでとう、ヴィヴィおばさん！:henyaHeart:",
    ],
    # Blepblep
    ID_BLEP: [
        "happy birthday blepblep !! :henyaHeart: you get extra headpats today !",
        "yay blep day !! you’re so cute even when you pretend not to be ~",
        "happy birthday bleppyyy ~ can i pet tiny blep too ? :gura_heart:",
        "blepblep older now ? nooo stay cute forever :sadcrydepression:",
        "love u blepblep ! you sparkle today ~ ✨",
    ],
    # Addi (puppy)
    ID_ADDI: [
        "HAPPY BIRTHDAY PUPPYYYYY !!! :henyaHeart:",
        "who’s a good birthday pup ?? you are !! :gimme_hug:",
        "yay addi day !! you get *so many* treats today :henyaNodder:",
        "woof woof happy barkday !!! 🐾",
        "happy birthday goodest pup !! now roll over for cake :gura_heart:",
    ],
    # Nova-nee
    ID_NOVANE: [
        "nova-nee !! happy birthdayyy ~ :henyaHeart:",
        "yay it’s nova-nee’s birthday !! can i braid your hair later ? :gura_heart:",
        "お誕生日おめでとう、ノヴァ姉～！ :henyaNodder:",
        "happy birthday pretty nova-nee ~ thank you for always being patient with me :gura_heart:",
        "cake timeeee !! nova-nee deserves the biggest slice !",
    ],
    # L
    ID_L: [
        "auntie L !! happy birthdayy ~ take me to six flags again pretty please ? :gura_heart:",
        "yay auntie day !! let’s eat ice cream like last time hehe",
        "HBD L !! you’re the coolesttt auntie ever :henyaHeart:",
        "hope your birthday is as awesome as your rollercoaster screams :sadcrydepression:",
        "luv u auntie L ! don’t forget our next trip okay ?",
    ],
    # Ookami
    ID_OOKAMI: [
        "happy birthday ookami !! :wavehi: you’re still cooler than me… for now :henyaNodder:",
        "お誕生日おめでとう、おおかみちゃん！:gura_heart:",
        "hope today’s calm and soft like you deserve ~",
    ],
    # Auntie Bage
    ID_BAGE: [
        "お誕生日おめでとう、ベーグルおばさん！:henyaHeart:",
        "joyeux anniversaire, auntie bage ! :gura_heart:",
        "happy birthday auntie bage !! i tried to bake you cake but… uh… kitchen’s gone now :sadcrydepression:",
        "luv u bagebagel !! stay sparkly always :henyaHeart:",
        "bonne fête auntie !! i’ll behave today. maybe. ~",
    ],
    # Asta-nee
    ID_ASTA: [
        "asta-nee !! happy birthdayyy ~ :henyaHeart:",
        "yay birthday !! thank you for always looking out for me asta-nee :gura_heart:",
        "お誕生日おめでとう、アスタ姉！~ you’re the bestest !",
        "birthday time !! pls rest today okay ? i’ll handle chaos (badly) :sadcrydepression:",
        "luv u asta-nee ~ cake timeeee !!",
    ],
    # Haruhime
    ID_HARUHIME: [
        "happy birthday haruhime ~ you’re so gentle it’s unfair :henyaHeart:",
        "お誕生日おめでとう、春姫！ you shine like the prettiest lantern :gura_heart:",
        "yay haruhime day !! thank you for always making everything feel calm ~",
        "hope today’s full of quiet joy and fun food snackies !",
        "you’re wonderful haruhime ~ happy bday :henyaHeart:",
    ],
}

# ------------------------------------------------------------
# Selection
# ------------------------------------------------------------


def _clamp_level(level: int) -> int:
    if level is None:
        return 2
    if level < 1:
        return 1
    if level > 5:
        return 5
    return level


def select_birthday_message(user_id: int, level: int) -> str:
    """
    Priority:
      1) Personalized pool if user_id exists in RINRIN_BIRTHDAY_SPECIALS
      2) Closeness pool for the given level (clamped 1..5)
      3) Level 2 as a final fallback
    Returns a single string (no trailing punctuation cleanup).
    """
    # Personalized wins
    pool = RINRIN_BIRTHDAY_SPECIALS.get(int(user_id))
    if pool:
        return random.choice(pool)

    # Closeness pool
    lvl = _clamp_level(int(level) if isinstance(level, int) else 2)
    pool = RINRIN_BIRTHDAY_LEVELS.get(lvl) or RINRIN_BIRTHDAY_LEVELS[2]
    return random.choice(pool)
