from __future__ import annotations

from typing import Iterable, Optional

import discord

from ..strings import S
from ..utils.modlog import color_for_temperature, temperature_label
from ..utils.time import now_local


def build_modlog_embed(
    *,
    user: discord.Member,
    rule: str,
    temperature: int,
    reason: str,
    details: Optional[str],
    actions: Iterable[str],
    actor: discord.Member,
    evidence_url: Optional[str],
) -> discord.Embed:
    embed = discord.Embed(
        title=S("modlog.embed.title", temp=temperature_label(temperature)),
        color=color_for_temperature(temperature),
        timestamp=now_local(),
    )
    embed.add_field(
        name=S("modlog.embed.user"),
        value=f"{user.mention} (`{user.id}`)",
        inline=False,
    )
    embed.add_field(name=S("modlog.embed.rule"), value=rule, inline=True)
    embed.add_field(name=S("modlog.embed.temperature"), value=str(temperature), inline=True)
    embed.add_field(name=S("modlog.embed.reason"), value=reason[:1000], inline=False)
    if details:
        embed.add_field(name=S("modlog.embed.details"), value=details[:1000], inline=False)
    action_text = "\n".join(actions)[:1000] if actions else None
    if action_text:
        embed.add_field(name=S("modlog.embed.actions"), value=action_text, inline=False)
    embed.set_footer(text=S("modlog.embed.footer", actor=str(actor), actor_id=actor.id))
    if evidence_url:
        embed.set_image(url=evidence_url)
    return embed


def build_dm_embed(
    *,
    user: discord.Member,
    rule: str,
    temperature: int,
    reason: str,
    details: Optional[str],
    actions: Iterable[str],
) -> discord.Embed:
    embed = discord.Embed(
        title=S("modlog.dm.title"),
        description=temperature_label(temperature),
        color=color_for_temperature(temperature),
    )
    embed.add_field(name=S("modlog.dm.rule"), value=rule, inline=True)
    embed.add_field(name=S("modlog.dm.status"), value=S("modlog.dm.status_open"), inline=False)
    embed.add_field(name=S("modlog.dm.reason"), value=reason[:1000] if reason else "-", inline=False)
    if details:
        embed.add_field(name=S("modlog.dm.detail"), value=details[:1000], inline=False)
    action_text = "\n".join(actions) if actions else S("modlog.dm.actions_warning")
    embed.add_field(name=S("modlog.dm.actions"), value=action_text[:1000], inline=False)
    return embed


def build_relay_embed(message: discord.Message) -> discord.Embed:
    embed = discord.Embed(
        title=S("modlog.relay.title"),
        description=(message.content[:2000] if message.content else " "),
        color=discord.Color.blurple(),
        timestamp=now_local(),
    )
    embed.set_footer(
        text=S("modlog.relay.footer", author=str(message.author), author_id=message.author.id)
    )
    if message.attachments:
        links = "\n".join(a.url for a in message.attachments)
        embed.add_field(name=S("modlog.relay.attachments"), value=links[:1000], inline=False)
    return embed
