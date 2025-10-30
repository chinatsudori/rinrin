from __future__ import annotations

import logging
import inspect
from datetime import timedelta
from typing import Optional

import discord
from discord import app_commands
from discord.ext import commands

from ..strings import S  # string table

log = logging.getLogger(__name__)

MAX_OPTIONS = 6
MAX_HOURS = 168  # 7 days

def _mk_poll(question: str, hours: int, multi: bool) -> tuple[discord.Poll, bool]:
    """
    Create a Poll that works across discord.py variants.

    Returns (poll, multiselect_honored)
      - multiselect_honored=False when the lib doesn't support multi at all.
    """
    dur = timedelta(hours=int(hours))
    try:
        sig = inspect.signature(discord.Poll)  # type: ignore[attr-defined]
        params = sig.parameters
    except (TypeError, ValueError):
        params = {}

    base = {"question": question}
    if "duration" in params:
        base["duration"] = dur
    else:
        # Some forks want int hours; pass anyway
        base["duration"] = int(hours)

    multikeys = ("allow_multiselect", "allow_multiple_choices", "multiple")
    poll = None
    multi_set = False

    # Try constructor kwargs first
    for key in multikeys:
        if key in params:
            try:
                poll = discord.Poll(**base, **{key: multi})  # type: ignore[arg-type]
                multi_set = True
                break
            except TypeError:
                pass

    if poll is None:
        # Fall back: build without multi flag
        poll = discord.Poll(**base)  # type: ignore[arg-type]
        # Try setting attribute post-init
        for key in multikeys:
            if hasattr(poll, key):
                try:
                    setattr(poll, key, multi)
                    multi_set = True
                    break
                except Exception:
                    pass

    # If the lib simply has no concept of multi, we'll return False
    honored = bool(multi is False or multi_set)
    return poll, honored


def _add_answer_compat(poll: discord.Poll, text: str) -> None:
    """Support both add_answer(text=...) and add_answer(PollAnswer(...))."""
    try:
        poll.add_answer(text=text)  # modern
    except TypeError:
        poll.add_answer(discord.PollAnswer(text=text))  # older signature

class PollsCog(commands.Cog):
    """Create native Discord polls with custom durations and up to 6 options."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    poll = app_commands.Group(
        name="poll",
        description=S("poll.native.group_desc"),
    )

    @poll.command(name="create", description=S("poll.native.create_desc"))
    @app_commands.describe(
        question=S("poll.native.arg.question"),
        opt1=S("poll.native.arg.opt1"),
        opt2=S("poll.native.arg.opt2"),
        opt3=S("poll.native.arg.opt3"),
        opt4=S("poll.native.arg.opt4"),
        opt5=S("poll.native.arg.opt5"),
        opt6=S("poll.native.arg.opt6"),
        hours=S("poll.native.arg.hours"),
        multi=S("poll.native.arg.multi"),
        ephemeral=S("poll.native.arg.ephemeral"),
    )
    async def create(
        self,
        interaction: discord.Interaction,
        question: str,
        opt1: str,
        opt2: str,
        opt3: Optional[str] = None,
        opt4: Optional[str] = None,
        opt5: Optional[str] = None,
        opt6: Optional[str] = None,
        hours: app_commands.Range[int, 1, MAX_HOURS] = 48,
        multi: bool = False,
        ephemeral: bool = False,
    ):
        # Guild-only
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        options = [o for o in (opt1, opt2, opt3, opt4, opt5, opt6) if o]
        if len(options) < 2:
            return await interaction.response.send_message(
                S("poll.native.err.need_two"), ephemeral=True
            )
        if len(options) > MAX_OPTIONS:
            return await interaction.response.send_message(
                S("poll.native.err.too_many", n=MAX_OPTIONS), ephemeral=True
            )

        try:
            poll, multi_honored = _mk_poll(question[:300], int(hours), multi)

            for text in options:
                _add_answer_compat(poll, text[:300])

            await interaction.response.send_message(
                poll=poll,
                ephemeral=ephemeral
            )

            if multi and not multi_honored:
                try:
                    await interaction.followup.send(
                        "This Discord library build doesn’t support multi-select polls; created single-choice.",
                        ephemeral=True,
                    )
                except Exception:
                    pass

            log.info(
                "poll.create",
                extra={
                    "guild_id": interaction.guild_id,
                    "channel_id": getattr(interaction.channel, "id", None),
                    "user_id": interaction.user.id,
                    "hours": int(hours),
                    "multi": multi,
                    "multi_honored": multi_honored,
                    "opts": len(options),
                },
            )

        except Exception as e:
            log.exception("poll.create.failed", exc_info=e)
            msg = S("poll.native.err.create_failed", err=type(e).__name__)
            if not interaction.response.is_done():
                await interaction.response.send_message(msg, ephemeral=True)
            else:
                await interaction.followup.send(msg, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(PollsCog(bot))
