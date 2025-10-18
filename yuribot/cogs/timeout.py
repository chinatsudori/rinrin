from __future__ import annotations
import logging
from datetime import timedelta
import discord
from discord.ext import commands
from discord import app_commands

from .. import models
from ..utils.time import now_local, to_iso
from ..strings import S

log = logging.getLogger(__name__)

MAX_TIMEOUT_DAYS = 28  # Discord hard cap

def _has_mod_perms(m: discord.Member) -> bool:
    p = m.guild_permissions
    return any([p.moderate_members, p.kick_members, p.ban_members, p.manage_guild])

def _can_act(actor: discord.Member, target: discord.Member, bot_member: discord.Member | None) -> tuple[bool, str | None]:
    if actor.id == target.id:
        return False, S("timeout.error.self")
    if target.guild.owner_id == target.id:
        return False, S("timeout.error.owner")
    if not _has_mod_perms(actor):
        return False, S("timeout.error.actor_perms")
    if not bot_member or not bot_member.guild_permissions.moderate_members:
        return False, S("timeout.error.bot_perms")
    if bot_member.top_role <= target.top_role:
        return False, S("timeout.error.bot_hierarchy")
    if actor != target.guild.owner and actor.top_role <= target.top_role:
        return False, S("timeout.error.actor_hierarchy")
    return True, None

class TimeoutCog(commands.Cog):
    """Moderation: timeouts with arbitrary durations (up to Discord's max)."""

    def __init__(self, bot: commands.Bot):
        self.bot = bot

    @app_commands.command(
        name="timeout",
        description="Timeout a member for custom days/hours/minutes/seconds (Discord cap ~28 days).",
    )
    @app_commands.describe(
        user="Member to timeout",
        days=f"Days (0–{MAX_TIMEOUT_DAYS})",
        hours="Hours (0–23)",
        minutes="Minutes (0–59)",
        seconds="Seconds (0–59)",
        reason="Optional reason (appears in Audit Log and DM)",
        dm_user="Attempt to DM the user about the timeout",
        post="If true, post publicly in this channel",
    )
    async def timeout(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        days: app_commands.Range[int, 0, MAX_TIMEOUT_DAYS] = 0,
        hours: app_commands.Range[int, 0, 23] = 0,
        minutes: app_commands.Range[int, 0, 59] = 0,
        seconds: app_commands.Range[int, 0, 59] = 0,
        reason: str | None = None,
        dm_user: bool = True,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=not post)

        me = interaction.guild.me  # bot as a Member
        ok, why = _can_act(interaction.user, user, me)
        if not ok:
            log.info(
                "mod.timeout.denied",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "reason": why,
                },
            )
            return await interaction.followup.send(why, ephemeral=not post)

        delta = timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

        # keep a practical floor to avoid fat-finger spam; same as previous behavior
        if delta.total_seconds() < 60:
            return await interaction.followup.send(S("timeout.error.min_duration"), ephemeral=not post)

        # Enforce Discord cap
        if delta > timedelta(days=MAX_TIMEOUT_DAYS):
            delta = timedelta(days=MAX_TIMEOUT_DAYS)

        until = discord.utils.utcnow() + delta

        # Try to DM the target first (so they see the reason)
        if dm_user:
            try:
                embed = discord.Embed(
                    title=S("timeout.dm.title", guild=interaction.guild.name),
                    description=(reason or S("timeout.dm.no_reason")),
                    color=discord.Color.orange(),
                )
                embed.add_field(
                    name=S("timeout.dm.field.duration"),
                    value=S("timeout.dm.value.duration", d=days, h=hours, m=minutes, s=seconds),
                    inline=True,
                )
                embed.add_field(
                    name=S("timeout.dm.field.until"),
                    value=f"<t:{int(until.timestamp())}:F>",
                    inline=True,
                )
                await user.send(embed=embed)
            except Exception:
                # DM closed or blocked; ignore
                pass

        # Apply timeout
        try:
            await user.timeout(until, reason=reason or S("timeout.audit.default_reason"))
        except discord.Forbidden:
            log.warning(
                "mod.timeout.forbidden",
                extra={
                    "guild_id": interaction.guild_id,
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                },
            )
            return await interaction.followup.send(S("timeout.error.forbidden_apply"), ephemeral=not post)
        except discord.HTTPException as e:
            log.warning(
                "mod.timeout.http_error",
                extra={
                    "guild_id": interaction.guild_id,
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "error": str(e),
                },
            )
            return await interaction.followup.send(S("timeout.error.http_apply", err=e), ephemeral=not post)

        # Log to mod-logs channel (if configured)
        try:
            ch_id = models.get_mod_logs_channel(interaction.guild_id)
            if ch_id:
                ch = interaction.guild.get_channel(ch_id)
                if isinstance(ch, discord.TextChannel):
                    log_embed = discord.Embed(
                        title=S("timeout.log.title"),
                        color=discord.Color.orange(),
                        timestamp=now_local(),
                    )
                    log_embed.add_field(
                        name=S("timeout.log.field.user"),
                        value=f"{user.mention} (`{user.id}`)",
                        inline=False,
                    )
                    log_embed.add_field(
                        name=S("timeout.log.field.by"),
                        value=f"{interaction.user.mention} (`{interaction.user.id}`)",
                        inline=False,
                    )
                    log_embed.add_field(
                        name=S("timeout.log.field.duration"),
                        value=S("timeout.dm.value.duration", d=days, h=hours, m=minutes, s=seconds),
                        inline=True,
                    )
                    log_embed.add_field(
                        name=S("timeout.log.field.until"),
                        value=f"<t:{int(until.timestamp())}:F>",
                        inline=True,
                    )
                    if reason:
                        log_embed.add_field(name=S("timeout.log.field.reason"), value=reason[:1000], inline=False)
                    await ch.send(embed=log_embed)
        except Exception:
            log.exception(
                "mod.timeout.log_failed",
                extra={"guild_id": interaction.guild_id, "channel_id": ch_id if 'ch_id' in locals() else None},
            )

        # Persist mod action (best-effort)
        try:
            models.add_mod_action(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                rule="Staff & Enforcement",
                offense=0,
                action="timeout",
                details=reason or "",
                evidence_url="",
                actor_user_id=interaction.user.id,
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("mod.timeout.persist_failed", extra={"guild_id": interaction.guild_id})

        await interaction.followup.send(
            S(
                "timeout.done",
                user=user.mention,
                d=days, h=hours, m=minutes, s=seconds,
                until_ts=int(until.timestamp()),
            ),
            ephemeral=not post,
        )

        log.info(
            "mod.timeout.applied",
            extra={
                "guild_id": interaction.guild_id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "d": int(days), "h": int(hours), "m": int(minutes), "s": int(seconds),
                "until": int(until.timestamp()),
                "post": post,
                "dm_user": dm_user,
                "reason_len": len(reason or ""),
            },
        )

    @app_commands.command(name="untimeout", description="Remove a timeout from a member.")
    @app_commands.describe(
        user="Member to remove timeout from",
        reason="Optional reason",
        post="If true, post publicly in this channel",
    )
    async def untimeout(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        reason: str | None = None,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=not post)

        me = interaction.guild.me
        ok, why = _can_act(interaction.user, user, me)
        if not ok:
            log.info(
                "mod.untimeout.denied",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "reason": why,
                },
            )
            return await interaction.followup.send(why, ephemeral=not post)

        try:
            await user.timeout(None, reason=reason or S("timeout.audit.remove_reason"))
        except discord.Forbidden:
            log.warning(
                "mod.untimeout.forbidden",
                extra={"guild_id": interaction.guild_id, "actor_id": interaction.user.id, "target_id": user.id},
            )
            return await interaction.followup.send(S("timeout.error.forbidden_remove"), ephemeral=not post)
        except discord.HTTPException as e:
            log.warning(
                "mod.untimeout.http_error",
                extra={
                    "guild_id": interaction.guild_id,
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "error": str(e),
                },
            )
            return await interaction.followup.send(S("timeout.error.http_remove", err=e), ephemeral=not post)

        try:
            ch_id = models.get_mod_logs_channel(interaction.guild_id)
            if ch_id:
                ch = interaction.guild.get_channel(ch_id)
                if isinstance(ch, discord.TextChannel):
                    emb = discord.Embed(
                        title=S("timeout.remove.title"),
                        color=discord.Color.green(),
                        timestamp=now_local(),
                    )
                    emb.add_field(name=S("timeout.log.field.user"), value=f"{user.mention} (`{user.id}`)", inline=False)
                    emb.add_field(name=S("timeout.log.field.by"), value=f"{interaction.user.mention} (`{interaction.user.id}`)", inline=False)
                    if reason:
                        emb.add_field(name=S("timeout.log.field.reason"), value=reason[:1000], inline=False)
                    await ch.send(embed=emb)
        except Exception:
            log.exception(
                "mod.untimeout.log_failed",
                extra={"guild_id": interaction.guild_id, "channel_id": ch_id if 'ch_id' in locals() else None},
            )

        await interaction.followup.send(S("timeout.remove.done", user=user.mention), ephemeral=not post)

        log.info(
            "mod.untimeout.applied",
            extra={
                "guild_id": interaction.guild_id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "post": post,
                "reason_len": len(reason or ""),
            },
        )

async def setup(bot: commands.Bot):
    await bot.add_cog(TimeoutCog(bot))
