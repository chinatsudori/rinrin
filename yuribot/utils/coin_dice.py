from __future__ import annotations

import random
import re
from typing import List, Tuple

MAX_COINS = 20
MAX_DICE_TOTAL = 50

DICE_SPEC_RE = re.compile(
    r"""
    ^\s*
    (?:(\d*)d)?       # optional N before 'd'
    (\d+)             # sides M
    ([+-]\d+)?        # optional modifier
    \s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


class DiceSpecError(ValueError):
    pass


def parse_specs(text: str) -> List[Tuple[int, int, int, str]]:
    if not text:
        raise DiceSpecError("empty")
    tokens = [
        token.strip() for token in re.split(r"[, \u3000]+", text) if token.strip()
    ]
    parsed: List[Tuple[int, int, int, str]] = []
    for token in tokens:
        match = DICE_SPEC_RE.match(token)
        if not match:
            raise DiceSpecError(token)
        count_str, sides_str, mod_str = match.groups()
        count = int(count_str) if count_str else 1
        sides = int(sides_str)
        modifier = int(mod_str) if mod_str else 0
        if count <= 0 or sides <= 0:
            raise DiceSpecError(token)
        spec_repr = f"{count}d{sides}" + (
            f"+{modifier}" if modifier > 0 else f"{modifier}" if modifier < 0 else ""
        )
        parsed.append((count, sides, modifier, spec_repr))
    return parsed


def flip_coins(count: int) -> Tuple[List[str], int, int]:
    results = [random.choice(("Heads", "Tails")) for _ in range(count)]
    heads = results.count("Heads")
    tails = count - heads
    return results, heads, tails


def roll_dice(specs: List[Tuple[int, int, int, str]]):
    lines = []
    total_rolls = 0
    grand_total = 0
    for count, sides, modifier, spec in specs:
        rolls = [random.randint(1, sides) for _ in range(count)]
        subtotal = sum(rolls) + modifier
        total_rolls += count
        grand_total += subtotal
        lines.append((spec, rolls, modifier, subtotal))
    return lines, total_rolls, grand_total
