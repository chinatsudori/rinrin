# Added by automation: channel resolver shim
from __future__ import annotations

def get_from_guild(guild, fallback_id: int, key: str | None = None):
    """Return a channel from per-guild settings if defined, else fallback to the old hard-coded ID.
    This is synchronous and uses the cache just like guild.get_channel.
    """
    try:
        from ..models import settings as ms
    except Exception:
        # fallback if relative import differs
        try:
            from yuribot.models import settings as ms  # package name
        except Exception:
            ms = None
    channel_id = fallback_id
    if ms and guild:
        if key:
            cid = ms.get_channel_id(guild.id, key, fallback_id=fallback_id)
            if cid:
                channel_id = int(cid)
        else:
            # Without a key, we just honor fallback_id
            channel_id = int(fallback_id)
    ch = None
    if hasattr(guild, "get_channel"):
        ch = guild.get_channel(channel_id)
    if ch is None and hasattr(guild, "get_thread"):
        ch = guild.get_thread(channel_id)
    return ch

def get_from_bot(bot, fallback_id: int):
    # Keep behavior as close to Client.get_channel as possible
    channel_id = int(fallback_id)
    if hasattr(bot, "get_channel"):
        return bot.get_channel(channel_id)
    return None
