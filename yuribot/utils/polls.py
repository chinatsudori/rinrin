from __future__ import annotations

import inspect
import logging
from datetime import timedelta
from typing import Tuple

import discord

log = logging.getLogger(__name__)

MAX_OPTIONS = 6
MAX_HOURS = 168  # 7 days


def create_poll(question: str, hours: int, allow_multi: bool) -> Tuple[discord.Poll, bool]:
    duration = timedelta(hours=int(hours))
    try:
        params = inspect.signature(discord.Poll).parameters  # type: ignore[attr-defined]
    except (TypeError, ValueError, AttributeError):
        params = {}

    base = {"question": question}
    if "duration" in params:
        base["duration"] = duration
    else:
        base["duration"] = int(hours)

    multikeys = ("allow_multiselect", "allow_multiple_choices", "multiple")
    poll = None
    multiset = False

    for key in multikeys:
        if key in params:
            try:
                poll = discord.Poll(**base, **{key: allow_multi})  # type: ignore[arg-type]
                multiset = True
                break
            except TypeError:
                continue

    if poll is None:
        poll = discord.Poll(**base)  # type: ignore[arg-type]
        for key in multikeys:
            if hasattr(poll, key):
                try:
                    setattr(poll, key, allow_multi)
                    multiset = True
                    break
                except Exception:
                    continue

    honored = bool(allow_multi is False or multiset)
    return poll, honored


def add_answer_compat(poll: discord.Poll, text: str) -> None:
    try:
        poll.add_answer(text=text)  # type: ignore[attr-defined]
    except TypeError:
        poll.add_answer(discord.PollAnswer(text=text))  # type: ignore[attr-defined]
