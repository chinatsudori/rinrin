from __future__ import annotations

from typing import Dict

__all__ = ["EMOJI", "expand_emoji_tokens"]

# Emoji expansion map so booly messages can reference custom emoji tokens.
EMOJI: Dict[str, str] = {
    "gura_heart": "<:gura_heart:1432286456558391376>",
    "henyaHeart": "<:henyaHeart:1432286471837978645>",
    "sadcrydepression": "<:sadcrydepression:1432289105131081738>",
    "ping": "<a:ping:1432286407736557608>",
    "gimme_hug": "<a:gimme_hug:1275464256330006589>",
    "henyaNodder": "<a:henyaNodder:1432286485905801306>",
    "wavehi": "<a:wavehi:1432286440028639272>",
    "WaveHiHi": "<a:wavehi:1432286440028639272>",
}


def expand_emoji_tokens(text: str) -> str:
    if not text:
        return text
    out = text
    for name, tag in EMOJI.items():
        out = out.replace(f":{name}:", tag).replace(f"a::{name}:", tag)
    return out
