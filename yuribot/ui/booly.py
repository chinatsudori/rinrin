from __future__ import annotations

from typing import Dict

__all__ = ["EMOJI", "expand_emoji_tokens"]

# Emoji expansion map so booly messages can reference custom emoji tokens.
EMOJI: Dict[str, str] = {
    "gura_heart": "gura_heart",
    "henyaHeart": "henyaHeart",
    "sadcrydepression": "sadcrydepression",
    "ping": "ping",
    "gimme_hug": "gimme_hug",
    "henyaNodder": "henyaNodder",
    "wavehi": "wavehi",
    "WaveHiHi": "wavehi",
}


def expand_emoji_tokens(text: str) -> str:
    if not text:
        return text
    out = text
    for name, tag in EMOJI.items():
        out = out.replace(f":{name}:", tag).replace(f"a::{name}:", tag)
    return out
