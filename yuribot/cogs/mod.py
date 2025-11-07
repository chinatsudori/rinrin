from __future__ import annotations

import logging
from datetime import timedelta
from typing import Dict, Optional, Tuple

import discord
from discord import app_commands
from discord.ext import commands

from ..models import booly as booly_model
from ..models import mod_actions, settings
from ..strings import S
from ..ui.modlog import build_dm_embed as build_modlog_dm_embed
from ..ui.modlog import build_modlog_embed, build_relay_embed
from ..ui.timeout import build_dm_embed as build_timeout_dm_embed
from ..utils.modlog import RULE_CHOICES, permission_ok
from ..utils.time import now_local, to_iso
from ..utils.timeout import MAX_TIMEOUT_DAYS, can_act, clamp_duration
from ..utils.booly import has_mod_perms

BOOLY_SCOPE_CHOICES = [
    app_commands.Choice(name="General mention", value=booly_model.SCOPE_MENTION_GENERAL),
    app_commands.Choice(name="Mod mention", value=booly_model.SCOPE_MENTION_MOD),
    app_commands.Choice(name="Personal", value=booly_model.SCOPE_PERSONAL),
]

_SCOPE_LABELS = {
    booly_model.SCOPE_MENTION_GENERAL: "General mention",
    booly_model.SCOPE_MENTION_MOD: "Mod mention",
    booly_model.SCOPE_PERSONAL: "Personal",
}

log = logging.getLogger(__name__)


def _require_mod() -> app_commands.Check:
    async def predicate(interaction: discord.Interaction) -> bool:
        if not interaction.guild:
            await interaction.response.send_message(S("common.guild_only"), ephemeral=not post)
            return False
        member = interaction.user if isinstance(interaction.user, discord.Member) else None
        if not member or not has_mod_perms(member):
            await interaction.response.send_message(S("modlog.err.perms"), ephemeral=not post)
            return False
        return True

    return app_commands.check(predicate)


class ModCog(commands.GroupCog, name="mod", description="Moderation tools"):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self._dm_relays: Dict[int, Tuple[int, int]] = {}

    def _reload_booly_cache(self) -> None:
        cog = self.bot.get_cog("UserAutoResponder")
        if cog and hasattr(cog, "reload_messages"):
            try:
                cog.reload_messages()  # type: ignore[call-arg]
            except Exception:
                log.exception("mod.booly.reload_failed")

    # ===== Timeout =====

    @app_commands.command(
        name="timeout",
        description="Timeout a member for custom days/hours/minutes/seconds (Discord cap ~28 days).",
    )
    @app_commands.describe(
        user="Member to timeout",
        days=f"Days (0-{MAX_TIMEOUT_DAYS})",
        hours="Hours (0-23)",
        minutes="Minutes (0-59)",
        seconds="Seconds (0-59)",
        reason="Optional reason (appears in Audit Log and DM)",
        dm_user="Attempt to DM the user about the timeout",
        post="If true, post publicly in this channel",
    )
    @_require_mod()
    async def timeout(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        days: app_commands.Range[int, 0, MAX_TIMEOUT_DAYS] = 0,
        hours: app_commands.Range[int, 0, 23] = 0,
        minutes: app_commands.Range[int, 0, 59] = 0,
        seconds: app_commands.Range[int, 0, 59] = 0,
        reason: Optional[str] = None,
        dm_user: bool = True,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)

        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=not post)

        me = interaction.guild.me
        ok, why_key = can_act(interaction.user, user, me)
        if not ok:
            message = S(why_key) if why_key else S("timeout.error.unknown")
            log.info(
                "mod.timeout.denied",
                extra={
                    "guild_id": getattr(interaction, "guild_id", None),
                    "actor_id": interaction.user.id,
                    "target_id": user.id,
                    "reason": why_key,
                },
            )
            return await interaction.followup.send(message, ephemeral=not post)

        try:
            delta = clamp_duration(days, hours, minutes, seconds)
        except ValueError as exc:
            return await interaction.followup.send(S(str(exc)), ephemeral=not post)

        until = discord.utils.utcnow() + delta

        if dm_user:
            try:
                duration_display = S("timeout.dm.value.duration", d=days, h=hours, m=minutes, s=seconds)
                embed = build_timeout_dm_embed(
                    guild_name=interaction.guild.name,
                    reason=reason or S("timeout.dm.no_reason"),
                    until_timestamp=int(until.timestamp()),
                    duration_display=duration_display,
                )
                await user.send(embed=embed)
            except Exception:
                pass

        try:
            await user.timeout(until, reason=reason[:512] if reason else None)
        except discord.Forbidden:
            return await interaction.followup.send(S("timeout.error.bot_perms"), ephemeral=not post)
        except discord.HTTPException:
            return await interaction.followup.send(S("timeout.error.http"), ephemeral=not post)

        try:
            mod_actions.add_timeout(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                actor_user_id=interaction.user.id,
                duration_seconds=int(delta.total_seconds()),
                reason=reason or "",
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("mod.timeout.persist_failed", extra={"guild_id": interaction.guild_id})

        await interaction.followup.send(
            S(
                "timeout.success",
                user=user.mention,
                duration=int(delta.total_seconds()),
                until=int(until.timestamp()),
            ),
            ephemeral=not post,
        )

        log.info(
            "mod.timeout.used",
            extra={
                "guild_id": interaction.guild_id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "duration": int(delta.total_seconds()),
                "dm_user": dm_user,
                "post": post,
            },
        )

    # ===== Modlog =====

    @app_commands.command(
        name="modlog",
        description="Record a moderation action (temperature-based), optionally timeout and/or ban.",
    )
    @app_commands.describe(
        user="User who broke the rules",
        rule="Which rule was involved",
        temperature="1=Gentle, 2=Formal, 3=Escalated, 4=Critical",
        reason="Short reason to show to the user",
        details="Optional detailed context",
        evidence="Optional image/screenshot",
        timeout_minutes="Optional timeout (minutes, up to ~28 days)",
        ban="Ban the user (yes/no)",
        dm_user="Attempt to DM the user (OFF by default)",
        post="If true, post publicly in this channel",
    )
    @app_commands.choices(rule=[app_commands.Choice(name=r, value=r) for r in RULE_CHOICES])
    @_require_mod()
    async def modlog_add(
        self,
        interaction: discord.Interaction,
        user: discord.Member,
        rule: app_commands.Choice[str],
        temperature: app_commands.Range[int, 1, 4],
        reason: str,
        details: Optional[str] = None,
        evidence: Optional[discord.Attachment] = None,
        timeout_minutes: Optional[app_commands.Range[int, 1, 40320]] = None,
        ban: Optional[bool] = False,
        dm_user: bool = False,
        post: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=not post)
        if not permission_ok(interaction.user):
            return await interaction.response.send_message(S("modlog.err.perms"), ephemeral=not post)

        channel_id = settings.get_mod_logs_channel(interaction.guild_id)
        if not channel_id:
            return await interaction.response.send_message(S("modlog.err.no_channel"), ephemeral=not post)
        channel = interaction.guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return await interaction.response.send_message(S("modlog.err.bad_channel"), ephemeral=not post)

        await interaction.response.defer(ephemeral=not post, thinking=True)

        temp_value = int(temperature)
        actions_taken: list[str] = []
        evidence_url: Optional[str] = None

        # Optional timeout
        if timeout_minutes and timeout_minutes > 0:
            if not interaction.user.guild_permissions.moderate_members:
                actions_taken.append(S("modlog.action.timeout.denied_perm", m=int(timeout_minutes)))
            else:
                try:
                    until = discord.utils.utcnow() + timedelta(minutes=int(timeout_minutes))
                    await user.timeout(until, reason=reason or S("modlog.reason.timeout_default"))
                    actions_taken.append(S("modlog.action.timeout.ok", m=int(timeout_minutes)))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.timeout.forbidden"))
                except discord.HTTPException:
                    actions_taken.append(S("modlog.action.timeout.http"))

        # Optional ban
        if ban:
            if not interaction.user.guild_permissions.ban_members:
                actions_taken.append(S("modlog.action.ban.denied_perm"))
            else:
                try:
                    await interaction.guild.ban(user, reason=reason[:512] if reason else None, delete_message_days=0)
                    actions_taken.append(S("modlog.action.ban.ok"))
                except discord.Forbidden:
                    actions_taken.append(S("modlog.action.ban.forbidden"))
                except discord.HTTPException:
                    actions_taken.append(S("modlog.action.ban.http"))

        if evidence and evidence.content_type and evidence.content_type.startswith("image/"):
            evidence_url = evidence.url

        mod_embed = build_modlog_embed(
            user=user,
            rule=rule.value,
            temperature=temp_value,
            reason=reason,
            details=details,
            actions=actions_taken,
            actor=interaction.user,
            evidence_url=evidence_url,
        )
        try:
            await channel.send(embed=mod_embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception(
                "modlog.post_failed",
                extra={"guild_id": interaction.guild_id, "channel_id": channel.id},
            )

        try:
            mod_actions.add_mod_action(
                guild_id=interaction.guild_id,
                target_user_id=user.id,
                target_username=str(user),
                rule=rule.value,
                offense=temp_value,
                action=("ban" if ban else ("timeout" if timeout_minutes else "warning")),
                details=(details or ""),
                evidence_url=evidence_url or "",
                actor_user_id=interaction.user.id,
                created_at=to_iso(now_local()),
            )
        except Exception:
            log.exception("modlog.persist_failed", extra={"guild_id": interaction.guild_id})

        if dm_user:
            dm_embed = build_modlog_dm_embed(
                user=user,
                rule=rule.value,
                temperature=temp_value,
                reason=reason,
                details=details,
                actions=actions_taken,
            )
            try:
                await user.send(embed=dm_embed)
                self._dm_relays[user.id] = (interaction.guild_id, channel.id)
            except Exception:
                await channel.send(
                    S("modlog.dm.could_not_dm", user=user.mention),
                    allowed_mentions=discord.AllowedMentions.none(),
                )

        await interaction.followup.send(S("modlog.done"), ephemeral=not post)

        log.info(
            "modlog.add.used",
            extra={
                "guild_id": interaction.guild_id,
                "channel_id": channel.id,
                "actor_id": interaction.user.id,
                "target_id": user.id,
                "rule": rule.value,
                "temp": temp_value,
                "timeout_m": int(timeout_minutes) if timeout_minutes else 0,
                "ban": bool(ban),
                "dm_user": bool(dm_user),
                "post": bool(post),
                "has_evidence": bool(evidence_url),
            },
        )

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.guild is not None or message.author.bot:
            return
        relay = self._dm_relays.get(message.author.id)
        if not relay:
            return
        guild_id, channel_id = relay
        guild = self.bot.get_guild(guild_id)
        if not guild:
            return
        channel = guild.get_channel(channel_id)
        if not isinstance(channel, discord.TextChannel):
            return

        embed = build_relay_embed(message)
        try:
            await channel.send(embed=embed, allowed_mentions=discord.AllowedMentions.none())
        except Exception:
            log.exception("modlog.relay.post_failed", extra={"guild_id": guild_id, "channel_id": channel_id})

    @app_commands.command(
        name="modlog_close_dm",
        description="Stop relaying DM replies from a user to the modlog channel.",
    )
    @app_commands.describe(user="User to stop relaying", post="If true, post publicly in this channel")
    @_require_mod()
    async def modlog_close_dm(self, interaction: discord.Interaction, user: discord.Member, post: bool = False):
        if not permission_ok(interaction.user):
            return await interaction.response.send_message(S("modlog.err.perms"), ephemeral=not post)

        await interaction.response.defer(ephemeral=not post, thinking=False)
        self._dm_relays.pop(user.id, None)
        await interaction.followup.send(S("modlog.relay.closed", user=user.mention), ephemeral=not post)

        log.info(
            "modlog.relay.closed",
            extra={
                "guild_id": getattr(interaction, "guild_id", None),
                "actor_id": interaction.user.id,
                "user_id": user.id,
                "post": post,
            },
        )

    # ===== Booly management =====

    booly = app_commands.Group(name="booly", description="Manage booly autoresponses")

    @booly.command(name="list", description="List booly messages for a scope.")
    @app_commands.describe(
        scope="Which message pool to view",
        user="Required for personal scope",
    )
    @app_commands.choices(scope=BOOLY_SCOPE_CHOICES)
    @_require_mod()
    async def booly_list(self, interaction: discord.Interaction,
        scope: app_commands.Choice[str],
        user: Optional[discord.Member] = None,
    , post: bool = False):
        target_user_id: Optional[int] = None
        if scope.value == booly_model.SCOPE_PERSONAL:
            if user is not None:
                target_user_id = user.id
            elif user_id:
                try:
                    target_user_id = int(user_id)
                except ValueError:
                    target_user_id = None
        if scope.value == booly_model.SCOPE_PERSONAL and not target_user_id:
            return await interaction.response.send_message(S("mod.booly.need_user"), ephemeral=not post)

        await interaction.response.defer(ephemeral=not post, thinking=False)

        messages = booly_model.fetch_messages(scope.value, target_user_id)
        lines = [f"`{msg.id}` • {msg.content}" for msg in messages]
        if not lines:
            lines = [S("mod.booly.none")]
        else:
            if len(lines) > 25:
                extra = len(lines) - 25
                lines = lines[:25] + [S("mod.booly.more", count=extra)]
        title = (
            S("mod.booly.title.personal", user=user.mention if user else "?")
            if scope.value == booly_model.SCOPE_PERSONAL
            else S("mod.booly.title.scope", scope=_SCOPE_LABELS.get(scope.value, scope.value))
        )
        embed = discord.Embed(title=title, description="\n".join(lines))
        await interaction.followup.send(embed=embed, ephemeral=not post)

    @booly.command(name="add", description="Add a new booly message.")
    @app_commands.describe(
        scope="Which message pool to add to",
        content="Message content (supports emoji tokens)",
        user="Required for personal scope",
    )
    @app_commands.choices(scope=BOOLY_SCOPE_CHOICES)
    @_require_mod()
    async def booly_add(self, interaction: discord.Interaction,
        scope: app_commands.Choice[str],
        content: str,
        user: Optional[discord.Member] = None,
        user_id: Optional[str] = None,
        post: bool = False):
        target_user_id: Optional[int] = None
        if scope.value == booly_model.SCOPE_PERSONAL:
            if user is not None:
                target_user_id = user.id
            elif user_id:
                try:
                    target_user_id = int(user_id)
                except ValueError:
                    target_user_id = None
        if scope.value == booly_model.SCOPE_PERSONAL and not target_user_id:
            return await interaction.response.send_message(S("mod.booly.need_user"), ephemeral=not post)

        await interaction.response.defer(ephemeral=not post, thinking=True)
        message = booly_model.create_message(scope.value, content.strip(), target_user_id)
        self._reload_booly_cache()
        await interaction.followup.send(S("mod.booly.added", id=message.id), ephemeral=not post)
        if target_user_id and interaction.guild:
            try:
                from ..utils.booly import load_state, save_state, GuildUserState, PERSONAL_COOLDOWN
                import time, random
                state = load_state()
                gid = str(interaction.guild.id)
                uid = str(target_user_id)
                now = int(time.time())
                g = state.setdefault(gid, {})
                st = g.get(uid) or GuildUserState()
                if not st.last_auto_ts or (now - (st.last_auto_ts or 0)) >= PERSONAL_COOLDOWN:
                    from ..db import connect as _db_connect
                    with _db_connect() as con:
                        rows = [r[0] for r in con.execute("SELECT content FROM booly_messages WHERE scope=? AND user_id=?", (booly_model.SCOPE_PERSONAL, target_user_id)).fetchall()]
                    if rows:
                        line = random.choice(rows)
                        content_out = f"<@{target_user_id}> " + str(S(line)).strip()
                        await interaction.followup.send(content_out, ephemeral=not post)
                        st.last_auto_ts = now
                        st.last_key = line
                        g[uid] = st
                        save_state(state)
            except Exception:
                pass

    @booly.command(name="edit", description="Edit an existing booly message.")
    @app_commands.describe(message_id="ID of the message to edit", content="Updated text")
    @_require_mod()
    async def booly_edit(
        self,
        interaction: discord.Interaction,
        message_id: int,
        content: str,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)
        updated = booly_model.update_message(message_id, content.strip())
        if not updated:
            return await interaction.followup.send(S("mod.booly.not_found", id=message_id), ephemeral=not post)
        self._reload_booly_cache()
        await interaction.followup.send(S("mod.booly.updated", id=message_id), ephemeral=not post)

    @booly.command(name="delete", description="Delete a booly message.")

    @booly.command(name="ping", description="Ping one or more users using booly logic (personal -> general/mod).")
    @app_commands.describe(
        user="Primary user to ping",
        extra_ids="Comma/space-separated additional user IDs to ping",
        scope="Fallback pool if personal empty (General/Mod)",
        post="Post publicly (True) or reply ephemerally (False)",
    )
    @app_commands.choices(scope=BOOLY_SCOPE_CHOICES)
    @_require_mod()
    async def booly_ping(
        self,
        interaction: discord.Interaction,
        user: Optional[discord.Member] = None,
        extra_ids: Optional[str] = None,
        scope: Optional[app_commands.Choice[str]] = None,
        post: bool = False,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=True)
        if not interaction.guild:
            return await interaction.followup.send(S("common.guild_only"), ephemeral=not post)

        targets = []
        if user is not None:
            targets.append(user.id)
        if extra_ids:
            for tok in extra_ids.replace(",", " ").split():
                try:
                    sid = int(tok.strip())
                    if sid not in targets:
                        targets.append(sid)
                except ValueError:
                    continue
        if not targets:
            return await interaction.followup.send("No valid user IDs provided.", ephemeral=not post)

        from ..db import connect as _db_connect
        import random as _random
        fallback_scope = scope.value if scope else booly_model.SCOPE_MENTION_GENERAL

        lines = []
        with _db_connect() as con:
            for uid in targets:
                cur = con.execute("SELECT content FROM booly_messages WHERE scope=? AND user_id=?", (booly_model.SCOPE_PERSONAL, uid))
                rows = [r[0] for r in cur.fetchall()]
                line = None
                if rows:
                    line = _random.choice(rows)
                else:
                    cur = con.execute("SELECT content FROM booly_messages WHERE scope=? AND user_id IS NULL", (fallback_scope,))
                    grows = [r[0] for r in cur.fetchall()]
                    if grows:
                        line = _random.choice(grows)
                if not line:
                    line = f"<@{uid}>"
                else:
                    line = f"<@{uid}> " + line
                lines.append(line)

        await interaction.followup.send("\n".join(lines), ephemeral=not post)


    @app_commands.describe(message_id="ID of the message to delete")
    @_require_mod()
    async def booly_delete(self, interaction: discord.Interaction,
        message_id: int,
    , post: bool = False):
        await interaction.response.defer(ephemeral=not post, thinking=False)
        deleted = booly_model.delete_message(message_id)
        if not deleted:
            return await interaction.followup.send(S("mod.booly.not_found", id=message_id), ephemeral=not post)
        self._reload_booly_cache()
        await interaction.followup.send(S("mod.booly.deleted", id=message_id), ephemeral=not post)

    @booly.command(name="view", description="View a booly message by ID.")
    @app_commands.describe(message_id="ID of the message to view")
    @_require_mod()
    async def booly_view(
        self,
        interaction: discord.Interaction,
        message_id: int,
    ):
        await interaction.response.defer(ephemeral=not post, thinking=False)
        message = booly_model.fetch_message(message_id)
        if not message:
            return await interaction.followup.send(S("mod.booly.not_found", id=message_id), ephemeral=not post)
        scope_label = _SCOPE_LABELS.get(message.scope, message.scope)
        if message.scope == booly_model.SCOPE_PERSONAL and message.user_id:
            user_text = f"<@{message.user_id}>"
        else:
            user_text = _SCOPE_LABELS.get(message.scope, S("mod.booly.scope.global"))
        embed = discord.Embed(
            title=S("mod.booly.view.title", id=message_id),
            description=message.content,
        )
        embed.add_field(name=S("mod.booly.view.scope"), value=scope_label, inline=True)
        embed.add_field(name=S("mod.booly.view.user"), value=user_text, inline=True)
        embed.set_footer(text=S("mod.booly.view.timestamps", created=message.created_at, updated=message.updated_at))
        await interaction.followup.send(embed=embed, ephemeral=not post)


async def setup(bot: commands.Bot):
    await bot.add_cog(ModCog(bot))