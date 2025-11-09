"""Modular database access layer for Yuribot."""

from . import booly
from . import common
from . import guilds
from . import mangaupdates
from . import message_archive
from . import mod_actions
from . import polls
from . import role_welcome
from . import settings

# ---- Lazy exports to avoid circular imports ----
from importlib import import_module as _import_module

__all__ = [
    "booly",
    "common",
    "guilds",
    "mangaupdates",
    "message_archive",
    "mod_actions",
    "polls",
    "role_welcome",
    "settings",
    "birthday",  # we expose the name…
]


def __getattr__(name: str):
    # …but only import the submodule on first access.
    if name == "birthday":
        mod = _import_module(f"{__name__}.birthday")
        globals()[name] = mod
        return mod
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
