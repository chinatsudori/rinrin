from __future__ import annotations

import json
import time
from dataclasses import dataclass, asdict
from typing import Dict, Optional

import discord
from discord.ext import commands

from ..data.booly_defaults import (
    EXCLUDED_CHANNEL_IDS,
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
    SPECIAL_IDS,
    SPECIAL_DEFAULT_POOL,
)
from .storage import resolve_data_file

__all__ = [
    "DATA_FILE",
    "PERSONAL_COOLDOWN",
    "MENTION_COOLDOWN",
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
    "EXCLUDED_CHANNEL_IDS",
    "SPECIAL_IDS",
    "SPECIAL_DEFAULT_POOL",
    "GuildUserState",
    "StateType",
    "load_state",
    "save_state",
    "current_timestamp",
    "mentioned_me",
    "has_mod_perms",
]


DATA_FILE = resolve_data_file("user_autoresponder.json")

# Personalized (per-user) auto-replies are once per day
PERSONAL_COOLDOWN = 24 * 60 * 60  # 24h

# Mentions are rate-limited so people can't spam
MENTION_COOLDOWN = 1200  # seconds (20 min)


@dataclass
class GuildUserState:
    last_auto_ts: Optional[int] = None      # last time a personalized auto fired
    last_key: Optional[str] = None          # last personalized line used
    last_mention_ts: Optional[int] = None   # last time Rinrin replied to a mention from this user


# state[guild_id][user_id] = GuildUserState
StateType = Dict[str, Dict[str, GuildUserState]]


def load_state() -> StateType:
    if DATA_FILE.exists():
        try:
            raw = json.loads(DATA_FILE.read_text(encoding="utf-8"))
            out: StateType = {}
            for gid, users in (raw or {}).items():
                inner: Dict[str, GuildUserState] = {}
                for uid, blob in (users or {}).items():
                    inner[uid] = GuildUserState(
                        last_auto_ts=blob.get("last_auto_ts"),
                        last_key=blob.get("last_key"),
                        last_mention_ts=blob.get("last_mention_ts"),
                    )
                out[gid] = inner
            return out
        except Exception:
            return {}
    return {}


def save_state(state: StateType) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    wire = {
        gid: {uid: asdict(st) for uid, st in users.items()}
        for gid, users in state.items()
    }
    DATA_FILE.write_text(json.dumps(wire, indent=2), encoding="utf-8")


def current_timestamp() -> int:
    return int(time.time())


def mentioned_me(bot: commands.Bot, msg: discord.Message) -> bool:
    user = getattr(bot, "user", None)
    return bool(user and user in msg.mentions)


def has_mod_perms(member: discord.Member) -> bool:
    perms = getattr(member, "guild_permissions", None)
    if not perms:
        return False
    return any(
        [
            perms.manage_guild,
            perms.manage_channels,
            perms.kick_members,
            perms.ban_members,
            perms.moderate_members,
            perms.administrator,
        ]
    )
