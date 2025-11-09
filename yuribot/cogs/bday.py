from __future__ import annotations

import logging
from typing import Optional, List

import discord
from discord import app_commands
from discord.ext import commands

from .. import config
from ..models import bday as model
from ..utils import bday as utils
from ..utils.booly import has_mod_perms
from ..strings import S  # use centralized strings

log = logging.getLogger(__name__)
DEFAULT_TZ = getattr(config, "TZ", getattr(config, "LOCAL_TZ", "UTC")) or "UTC"
DEFAULT_TZ_NAME = utils.DEFAULT_TZ_NAME


def _is_mod(member: discord.Member | None) -> bool:
    return bool(member and has_mod_perms(member))


def _fmt_row_line(
    g: int, u: int, m: int, d: int, tz: str, year: int | None, closeness: int | None
) -> str:
    parts = [
        f"<@{u}>",
        f"`{m:02d}-{d:02d}`",
        f"`{tz}`",
    ]
    if closeness:
        parts.append(f"{S('birthday.label.closeness')} {closeness}")
    if year:
        parts.append(f"{S('birthday.label.last')} {year}")
    return " • ".join(parts)


class BirthdayCog(commands.GroupCog, name="birthday", description="Birthday reminders"):
    """
    Commands:
      /birthday set mm-dd [timezone] [user]         # mods can target others
      /birthday view [user]                         # mods can view others
      /birthday remove                              # self only
      /birthday edit [user] [mm-dd] [timezone]      # partial update; mods can target others
      /birthday list [user]                         # list all in guild (mods only) or single user
      /birthday closeness set level [user]          # mods set closeness 1..5
      /birthday closeness view [user]               # mods view closeness
    """

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        model.ensure_tables()
        self._svc = utils.BirthdayService(bot)
        self._svc.start()

    def cog_unload(self):
        try:
            self._svc.stop()
        except Exception:
            pass

    # --- Set ---

    @app_commands.command(
        name="set", description="Set a birthday (MM-DD) and optional timezone."
    )
    @app_commands.describe(
        date_mmdd=S("birthday.hint.mmdd"),
        timezone_name=S(
            "birthday.hint.tz", default=f"Optional IANA TZ (default: {DEFAULT_TZ_NAME})"
        ),
        user=S("birthday.hint.user_mod"),
    )
    async def cmd_set(
        self,
        interaction: discord.Interaction,
        date_mmdd: str,
        timezone_name: Optional[str] = None,
        user: Optional[discord.Member] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )
        target: discord.Member
        if user is not None:
            if not _is_mod(actor):
                return await interaction.response.send_message(
                    S("birthday.err.perms_other"), ephemeral=True
                )
            target = user
        else:
            if not isinstance(interaction.user, discord.Member):
                return await interaction.response.send_message(
                    S("birthday.err.resolve_self"), ephemeral=True
                )
            target = interaction.user

        try:
            m, d = utils.parse_mmdd(date_mmdd)
        except ValueError as exc:
            return await interaction.response.send_message(S(str(exc)), ephemeral=True)

        tzname = utils.coerce_tz(timezone_name)
        model.upsert_birthday(interaction.guild.id, target.id, m, d, tzname)

        base = S("birthday.saved", m=f"{m:02d}", d=f"{d:02d}", tz=tzname)
        suffix = (
            ""
            if target.id == interaction.user.id
            else S("birthday.saved.for", user=target.mention)
        )
        await interaction.response.send_message(base + suffix, ephemeral=True)

    # --- View ---

    @app_commands.command(name="view", description="View a stored birthday.")
    @app_commands.describe(user=S("birthday.hint.user_mod_view"))
    async def cmd_view(
        self, interaction: discord.Interaction, user: Optional[discord.Member] = None
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )
        target: discord.Member
        if user is not None:
            if not _is_mod(actor):
                return await interaction.response.send_message(
                    S("birthday.err.perms_other"), ephemeral=True
                )
            target = user
        else:
            if not isinstance(interaction.user, discord.Member):
                return await interaction.response.send_message(
                    S("birthday.err.resolve_self"), ephemeral=True
                )
            target = interaction.user

        b = model.get_birthday(interaction.guild.id, target.id)
        if not b:
            who = (
                S("birthday.none_self")
                if target.id == interaction.user.id
                else S("birthday.none_other", user=target.mention)
            )
            return await interaction.response.send_message(who, ephemeral=True)

        closeness = b.closeness_level if b.closeness_level else 2
        txt = S(
            "birthday.view.line",
            m=f"{b.month:02d}",
            d=f"{b.day:02d}",
            tz=b.tz,
            last=(b.last_year or ""),
            closeness=str(closeness),
        )
        if target.id != interaction.user.id:
            txt = f"{target.mention} • " + txt
        await interaction.response.send_message(txt, ephemeral=True)

    # --- Remove (self only) ---

    @app_commands.command(name="remove", description="Remove your birthday entry.")
    async def cmd_remove(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )
        if not isinstance(interaction.user, discord.Member):
            return await interaction.response.send_message(
                S("birthday.err.resolve_self"), ephemeral=True
            )

        ok = model.delete_birthday(interaction.guild.id, interaction.user.id)
        if not ok:
            return await interaction.response.send_message(
                S("birthday.none_self"), ephemeral=True
            )
        await interaction.response.send_message(S("birthday.removed"), ephemeral=True)

    # --- Edit (partial update) ---

    @app_commands.command(
        name="edit", description="Edit an existing birthday (partial)."
    )
    @app_commands.describe(
        user=S("birthday.hint.user_required_or_self"),
        date_mmdd=S("birthday.hint.mmdd_optional"),
        timezone_name=S("birthday.hint.tz_optional"),
        closeness=S("birthday.hint.closeness_optional"),
    )
    async def cmd_edit(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        date_mmdd: Optional[str] = None,
        timezone_name: Optional[str] = None,
        closeness: Optional[int] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )
        target: discord.Member
        # editing others requires mod perms
        if user is not None:
            if not _is_mod(actor):
                return await interaction.response.send_message(
                    S("birthday.err.perms_other"), ephemeral=True
                )
            target = user
        else:
            if not isinstance(interaction.user, discord.Member):
                return await interaction.response.send_message(
                    S("birthday.err.resolve_self"), ephemeral=True
                )
            target = interaction.user

        existing = model.get_birthday(interaction.guild.id, target.id)
        if not existing:
            who = (
                S("birthday.none_self")
                if target.id == interaction.user.id
                else S("birthday.none_other", user=target.mention)
            )
            return await interaction.response.send_message(who, ephemeral=True)

        new_m = new_d = None
        if date_mmdd:
            try:
                new_m, new_d = utils.parse_mmdd(date_mmdd)
            except ValueError as exc:
                return await interaction.response.send_message(
                    S(str(exc)), ephemeral=True
                )

        new_tz = utils.coerce_tz(timezone_name) if timezone_name else None

        changed = False
        if new_m is not None or new_d is not None or new_tz is not None:
            ok = model.update_birthday(
                interaction.guild.id,
                target.id,
                month=(new_m if new_m is not None else None),
                day=(new_d if new_d is not None else None),
                tz=new_tz,
            )
            changed = changed or ok

        if closeness is not None:
            if not (1 <= closeness <= 5):
                return await interaction.response.send_message(
                    S("birthday.err.closeness_range"), ephemeral=True
                )
            model.set_closeness(interaction.guild.id, target.id, closeness)
            changed = True

        if not changed:
            return await interaction.response.send_message(
                S("birthday.edit.noop"), ephemeral=True
            )

        who = (
            ""
            if target.id == interaction.user.id
            else S("birthday.saved.for", user=target.mention)
        )
        return await interaction.response.send_message(
            S("birthday.edit.ok") + who, ephemeral=True
        )

    # --- List ---

    @app_commands.command(name="list", description="List stored birthdays.")
    @app_commands.describe(
        user=S("birthday.hint.user_optional"),
    )
    async def cmd_list(
        self, interaction: discord.Interaction, user: Optional[discord.Member] = None
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )

        # If user is specified: allow mods; for non-mods only allow self
        if user is not None and not _is_mod(actor) and user.id != interaction.user.id:
            return await interaction.response.send_message(
                S("birthday.err.perms_other"), ephemeral=True
            )

        entries: List[model.Birthday]
        if user is not None:
            entries = model.fetch_for_user(interaction.guild.id, user.id)
            title = S("birthday.list.title_user", user=user.mention)
        else:
            # listing all is mods only
            if not _is_mod(actor):
                # Fallback: non-mod listing without target = list your own
                b = model.get_birthday(interaction.guild.id, interaction.user.id)
                if not b:
                    return await interaction.response.send_message(
                        S("birthday.none_self"), ephemeral=True
                    )
                line = _fmt_row_line(
                    b.guild_id,
                    b.user_id,
                    b.month,
                    b.day,
                    b.tz,
                    b.last_year,
                    b.closeness_level,
                )
                embed = discord.Embed(
                    title=S("birthday.list.title_user", user=interaction.user.mention),
                    description=line,
                )
                return await interaction.response.send_message(
                    embed=embed, ephemeral=True
                )

            entries = model.fetch_all_for_guild(interaction.guild.id)
            title = S("birthday.list.title_all")

        if not entries:
            return await interaction.response.send_message(
                S("birthday.list.empty"), ephemeral=True
            )

        lines = [
            _fmt_row_line(
                e.guild_id,
                e.user_id,
                e.month,
                e.day,
                e.tz,
                e.last_year,
                e.closeness_level,
            )
            for e in entries
        ]
        # Discord safe size: trim if necessary
        if len(lines) > 40:
            extra = len(lines) - 40
            lines = lines[:40] + [S("birthday.list.more", count=str(extra))]

        embed = discord.Embed(title=title, description="\n".join(lines))
        await interaction.response.send_message(embed=embed, ephemeral=True)

    # --- Closeness admin (unchanged, uses S keys) ---

    closeness = app_commands.Group(
        name="closeness", description="Manage birthday closeness level (1..5)"
    )

    @closeness.command(name="set", description="(Mods) Set closeness level (1..5).")
    @app_commands.describe(
        level="1..5", user="Target user (optional; defaults to self)"
    )
    async def cmd_closeness_set(
        self,
        interaction: discord.Interaction,
        level: int,
        user: Optional[discord.Member] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )
        if user is not None and not _is_mod(actor):
            return await interaction.response.send_message(
                S("birthday.err.perms_other"), ephemeral=True
            )

        target = (
            user
            if user is not None
            else (
                interaction.user
                if isinstance(interaction.user, discord.Member)
                else None
            )
        )
        if target is None:
            return await interaction.response.send_message(
                S("birthday.err.resolve_self"), ephemeral=True
            )

        if level < 1 or level > 5:
            return await interaction.response.send_message(
                S("birthday.err.closeness_range"), ephemeral=True
            )

        b = model.get_birthday(interaction.guild.id, target.id)
        if not b:
            return await interaction.response.send_message(
                S("birthday.none_target_first"), ephemeral=True
            )

        model.set_closeness(interaction.guild.id, target.id, level)
        who = (
            ""
            if target.id == interaction.user.id
            else S("birthday.saved.for", user=target.mention)
        )
        await interaction.response.send_message(
            S("birthday.closeness.set", level=str(level)) + who, ephemeral=True
        )

    @closeness.command(name="view", description="(Mods) View closeness level.")
    @app_commands.describe(user="Target user (optional; defaults to self)")
    async def cmd_closeness_view(
        self, interaction: discord.Interaction, user: Optional[discord.Member] = None
    ):
        if not interaction.guild:
            return await interaction.response.send_message(
                S("common.guild_only"), ephemeral=True
            )

        actor = (
            interaction.user if isinstance(interaction.user, discord.Member) else None
        )
        if user is not None and not _is_mod(actor):
            return await interaction.response.send_message(
                S("birthday.err.perms_other"), ephemeral=True
            )

        target = (
            user
            if user is not None
            else (
                interaction.user
                if isinstance(interaction.user, discord.Member)
                else None
            )
        )
        if target is None:
            return await interaction.response.send_message(
                S("birthday.err.resolve_self"), ephemeral=True
            )

        b = model.get_birthday(interaction.guild.id, target.id)
        if not b:
            who = (
                S("birthday.none_self")
                if target.id == interaction.user.id
                else S("birthday.none_other", user=target.mention)
            )
            return await interaction.response.send_message(who, ephemeral=True)

        level = b.closeness_level if b.closeness_level else 2
        who = (
            ""
            if target.id == interaction.user.id
            else S("birthday.saved.for", user=target.mention)
        )
        await interaction.response.send_message(
            S("birthday.closeness.view", level=str(level)) + who, ephemeral=True
        )


async def setup(bot: commands.Bot):
    await bot.add_cog(BirthdayCog(bot))
