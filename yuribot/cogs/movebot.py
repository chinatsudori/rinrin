from __future__ import annotations
import asyncio
import io
import logging
from typing import Optional, Tuple, Union, List, Dict
from difflib import SequenceMatcher

import discord
from discord.ext import commands
from discord import app_commands

from ..strings import S 

log = logging.getLogger(__name__)

GuildTextish = Union[discord.TextChannel, discord.Thread, discord.ForumChannel]


async def _resolve_messageable_from_id(
    bot: commands.Bot, gid: int, ident: int
) -> Optional[GuildTextish]:
    log.debug("resolve_messageable: gid=%s ident=%s", gid, ident)
    ch = bot.get_channel(ident)
    if isinstance(ch, (discord.TextChannel, discord.ForumChannel)) and ch.guild and ch.guild.id == gid:
        log.debug("resolve_messageable: cache hit (text/forum) %s (%s)", ch.id, type(ch).__name__)
        return ch
    thr = bot.get_channel(ident)
    if isinstance(thr, discord.Thread) and thr.guild and thr.guild.id == gid:
        log.debug("resolve_messageable: cache hit (thread) %s", thr.id)
        return thr
    try:
        fetched = await bot.fetch_channel(ident)
        if isinstance(fetched, (discord.TextChannel, discord.Thread, discord.ForumChannel)) and fetched.guild and fetched.guild.id == gid:
            log.debug("resolve_messageable: fetch hit %s (%s)", fetched.id, type(fetched).__name__)
            return fetched
        log.debug("resolve_messageable: fetched object not usable: %s", type(fetched).__name__)
    except Exception as e:
        log.debug("resolve_messageable: fetch_channel failed: %r", e)
    return None


def _parent_for_destination(dest: GuildTextish) -> Optional[Union[discord.TextChannel, discord.ForumChannel]]:
    if isinstance(dest, discord.TextChannel):
        return dest
    if isinstance(dest, discord.Thread):
        return dest.parent if isinstance(dest.parent, (discord.TextChannel, discord.ForumChannel)) else None
    if isinstance(dest, discord.ForumChannel):
        return dest
    return None


async def _get_or_create_webhook(
    parent: Union[discord.TextChannel, discord.ForumChannel],
    me: discord.Member,
    *,
    name: str = "YuriBot Relay"
) -> Optional[discord.Webhook]:
    try:
        log.debug("webhook: checking/creating on parent=%s", parent.id)
        hooks = await parent.webhooks()
        for wh in hooks:
            if wh.user and wh.user.id == me.id:
                log.debug("webhook: reusing webhook id=%s", wh.id)
                return wh
        wh = await parent.create_webhook(name=name)
        log.debug("webhook: created webhook id=%s", wh.id)
        return wh
    except discord.Forbidden:
        log.debug("webhook: forbidden on parent=%s", getattr(parent, "id", "n/a"))
        return None
    except Exception as e:
        log.debug("webhook: unexpected error: %r", e)
        return None


async def _send_copy(
    destination: Union[discord.TextChannel, discord.Thread],
    source_msg: discord.Message,
    *,
    use_webhook: bool,
    webhook: Optional[discord.Webhook],
    include_header: bool,
):
    reply_prefix = ""
    try:
        if source_msg.reference and source_msg.reference.message_id:
            ref: Optional[discord.Message] = getattr(source_msg.reference, "resolved", None)
            if ref is None:
                try:
                    ref = await source_msg.channel.fetch_message(source_msg.reference.message_id)
                except Exception:
                    ref = None
            if ref is not None:
                snippet = (ref.content or "").strip().replace("\n", " ")
                if len(snippet) > 140:
                    snippet = snippet[:137] + "…"
                if not snippet and ref.attachments:
                    snippet = S("move_any.reply.attach_only")

                reply_prefix = S(
                    "move_any.reply.header",
                    author=ref.author.display_name,
                    jump=ref.jump_url,
                    snippet=snippet
                )
    except Exception:
        reply_prefix = ""

    content = source_msg.content or ""
    if include_header:
        jump = source_msg.jump_url
        ts = f"<t:{int(source_msg.created_at.timestamp())}:F>"
        author = source_msg.author.display_name
        header = S("move_any.header", author=author, ts=ts, jump=jump)
        body = "\n".join([p for p in (reply_prefix, header, content) if p]).strip()
    else:
        body = "\n".join([p for p in (reply_prefix, content) if p]).strip()

    files: List[discord.File] = []
    for att in source_msg.attachments:
        try:
            b = await att.read()
            files.append(discord.File(io.BytesIO(b), filename=att.filename))
        except Exception as e:
            log.debug("copy: attachment read failed msg=%s att=%s err=%r", source_msg.id, att.filename, e)

    if source_msg.stickers:
        sticker_lines = []
        for s in source_msg.stickers:
            if getattr(s, "url", None):
                sticker_lines.append(S("move_any.sticker.line_with_url", name=s.name, url=s.url))
            else:
                sticker_lines.append(S("move_any.sticker.line_no_url", name=s.name))
        if sticker_lines:
            body += ("\n\n" if body else "") + "\n".join(sticker_lines)

    common_kwargs = {"content": body or None, "allowed_mentions": discord.AllowedMentions.none()}
    if files:
        common_kwargs["files"] = files

    if use_webhook and webhook is not None:
        try:
            if isinstance(destination, discord.Thread):
                await webhook.send(
                    username=source_msg.author.display_name,
                    avatar_url=source_msg.author.display_avatar.url,
                    thread=destination,
                    wait=True,
                    **common_kwargs,
                )
            else:
                await webhook.send(
                    username=source_msg.author.display_name,
                    avatar_url=source_msg.author.display_avatar.url,
                    wait=True,
                    **common_kwargs,
                )
        except Exception as e:
            log.debug("send_copy: webhook send failed msg=%s err=%r (fallback)", source_msg.id, e)
            await destination.send(**common_kwargs)
    else:
        await destination.send(**common_kwargs)


async def _maybe_create_destination_thread(
    destination: GuildTextish,
    *,
    dest_thread_title: Optional[str],
) -> Tuple[Optional[Union[discord.TextChannel, discord.Thread]], Optional[str]]:
    if isinstance(destination, discord.Thread):
        return destination, None

    if isinstance(destination, discord.ForumChannel):
        if not dest_thread_title:
            return None, S("move_any.error.forum_needs_title")
        try:
            created = await destination.create_thread(
                name=dest_thread_title,
                content=S("move_any.thread.created_body"),
            )
            return created, None
        except discord.Forbidden:
            return None, S("move_any.error.forbidden_forum")
        except discord.HTTPException as e:
            return None, S("move_any.error.create_forum_failed", err=str(e))

    if isinstance(destination, discord.TextChannel):
        if dest_thread_title:
            try:
                starter = await destination.send(S("move_any.thread.starter_msg", title=dest_thread_title))
                created = await destination.create_thread(name=dest_thread_title, message=starter)
                return created, None
            except discord.Forbidden:
                return None, S("move_any.error.forbidden_thread")
            except discord.HTTPException as e:
                return None, S("move_any.error.create_thread_failed", err=str(e))
        else:
            return destination, None

    return None, S("move_any.error.unsupported_destination")


def _collapse_ws(s: str) -> str:
    return " ".join(s.split())

def _strip_possible_header(s: str) -> str:
    lines = s.splitlines()
    if not lines:
        return s
    first = lines[0]
    if "discord.com/channels/" in first or "http" in first:
        return "\n".join(lines[1:]).strip()
    if " — " in first and (":" in first or any(m in first.lower() for m in ("am", "pm", "utc"))):
        return "\n".join(lines[1:]).strip()
    return s

def _normalize_content(raw: str, *, allow_header: bool, ignore_case: bool, collapse_ws: bool) -> str:
    s = raw or ""
    if allow_header:
        s = _strip_possible_header(s)
    if collapse_ws:
        s = _collapse_ws(s)
    if ignore_case:
        s = s.lower()
    return s.strip()

def _attach_sig(msg: discord.Message) -> str:
    if not msg.attachments:
        return ""
    parts = []
    for a in msg.attachments:
        size = getattr(a, "size", None)
        parts.append(f"{a.filename}:{size if size is not None else 'na'}")
    return "|".join(parts)

def _fuzzy_ratio(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()



class MoveAnyCog(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot

    group = app_commands.Group(name="threadtools", description="Thread & channel utilities")

    @group.command(name="movebot", description="Copy messages from a channel/thread to a channel/thread (by IDs).")
    @app_commands.describe(
        source_id="ID of source TextChannel or Thread",
        destination_id="ID of destination TextChannel/Thread/ForumChannel",
        dest_thread_title="If destination is a Forum, title for the new post (or a new thread title in a TextChannel).",
        use_webhook="Preserve author name & avatar via webhook when possible",
        backlink="Include a header with author/time/jump URL at the top of each copied message (default: true).",
        delete_original="Delete original messages after successful copy",
        limit="Max number of messages to copy (oldest first). Leave empty for all.",
        before="Only copy messages created before this message ID or jump URL (in the source).",
        after="Only copy messages created after this message ID or jump URL (in the source).",
        dry_run="Count messages only; don’t send anything.",
        debug="Include a short failure digest and emit detailed logs.",
    )
    async def move_any(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        dest_thread_title: Optional[str] = None,
        use_webhook: bool = True,
        backlink: bool = True,
        delete_original: bool = False,
        limit: Optional[int] = None,
        before: Optional[str] = None,
        after: Optional[str] = None,
        dry_run: bool = False,
        debug: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)
        log.debug("movebot: invoked by %s (%s) guild=%s", interaction.user, interaction.user.id, interaction.guild_id)

        def _parse_id(s: str) -> Optional[int]:
            s = s.strip()
            try:
                return int(s)
            except Exception:
                try:
                    return int(s.rstrip("/").split("/")[-1])
                except Exception:
                    return None

        src_id = _parse_id(source_id)
        dst_id = _parse_id(destination_id)
        log.debug("movebot: parsed ids src=%s dst=%s", src_id, dst_id)
        if not src_id or not dst_id:
            return await interaction.followup.send(S("move_any.error.bad_ids"), ephemeral=True)

        source = await _resolve_messageable_from_id(self.bot, interaction.guild_id, src_id)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send(S("move_any.error.bad_source_type"), ephemeral=True)

        destination_raw = await _resolve_messageable_from_id(self.bot, interaction.guild_id, dst_id)
        if not isinstance(destination_raw, (discord.TextChannel, discord.Thread, discord.ForumChannel)):
            return await interaction.followup.send(S("move_any.error.bad_dest_type"), ephemeral=True)

        me = interaction.guild.me
        src_perms = source.permissions_for(me)
        log.debug("movebot: src perms read_history=%s manage_messages=%s", src_perms.read_message_history, src_perms.manage_messages)
        if not src_perms.read_message_history:
            return await interaction.followup.send(S("move_any.error.need_read_history"), ephemeral=True)

        destination, err = await _maybe_create_destination_thread(destination_raw, dest_thread_title=dest_thread_title)
        log.debug("movebot: destination prepared err=%r type=%s", err, type(destination).__name__ if destination else None)
        if err:
            return await interaction.followup.send(err, ephemeral=True)
        assert destination is not None

        dst_perms = destination.permissions_for(me)
        log.debug("movebot: dst perms send_messages=%s attach_files=%s", dst_perms.send_messages, dst_perms.attach_files)
        if not dst_perms.send_messages:
            return await interaction.followup.send(S("move_any.error.need_send_messages"), ephemeral=True)
        if not dst_perms.attach_files:
            return await interaction.followup.send(S("move_any.error.need_attach_files"), ephemeral=True)

        async def _resolve_msg(ref: Optional[str]) -> Optional[discord.Message]:
            if not ref:
                return None
            mid: Optional[int] = None
            try:
                mid = int(ref)
            except Exception:
                try:
                    mid = int(ref.rstrip("/").split("/")[-1])
                except Exception:
                    return None
            try:
                return await source.fetch_message(mid)
            except Exception:
                return None

        before_msg = await _resolve_msg(before)
        after_msg = await _resolve_msg(after)
        log.debug(
            "movebot: history window before=%s after=%s limit=%s",
            getattr(before_msg, "id", None),
            getattr(after_msg, "id", None),
            limit,
        )

        ALLOWED_TYPES = {
            discord.MessageType.default,
            discord.MessageType.reply,
        }

        to_copy: List[discord.Message] = []
        try:
            async for m in source.history(limit=limit, oldest_first=True, before=before_msg, after=after_msg):
                if m.type not in ALLOWED_TYPES:
                    if debug:
                        log.debug("movebot: skipping message id=%s type=%s", m.id, m.type)
                    continue
                to_copy.append(m)
        except discord.Forbidden:
            log.debug("movebot: forbidden reading source history")
            return await interaction.followup.send(S("move_any.error.forbidden_read_source"), ephemeral=True)

        log.debug("movebot: matched messages=%d", len(to_copy))
        if not to_copy:
            return await interaction.followup.send(S("move_any.info.none_matched"), ephemeral=True)

        if dry_run:
            where = destination.name
            return await interaction.followup.send(
                S("move_any.info.dry_run", count=len(to_copy), src=source.name, dst=where),
                ephemeral=True
            )

        webhook: Optional[discord.Webhook] = None
        if use_webhook:
            parent = _parent_for_destination(destination)
            can_hooks = parent and parent.permissions_for(me).manage_webhooks
            log.debug("movebot: webhook requested; can_manage_webhooks=%s", bool(can_hooks))
            if parent and can_hooks:
                webhook = await _get_or_create_webhook(parent, me)
            if webhook is None:
                log.debug("movebot: webhook unavailable; falling back to bot identity")
                await interaction.followup.send(S("move_any.info.webhook_fallback"), ephemeral=True)
                use_webhook = False

        copied = 0
        failed: List[tuple[int, str]] = []
        for i, msg in enumerate(to_copy, 1):
            try:
                await _send_copy(
                    destination,
                    msg,
                    use_webhook=use_webhook,
                    webhook=webhook,
                    include_header=backlink,
                )
                copied += 1
                if debug:
                    log.debug("movebot: copied msg id=%s", msg.id)
            except Exception as e:
                failed.append((msg.id, repr(e)))
                log.debug("movebot: FAILED copy msg id=%s err=%r", msg.id, e)
            if (i % 5) == 0:
                await asyncio.sleep(0.7)

        deleted = 0
        if delete_original and copied:
            if not src_perms.manage_messages:
                log.debug("movebot: cannot delete originals (missing manage_messages)")
                await interaction.followup.send(S("move_any.notice.cant_delete_source"), ephemeral=True)
            else:
                for msg in to_copy:
                    if any(fid == msg.id for fid, _ in failed):
                        continue
                    try:
                        await msg.delete()
                        deleted += 1
                    except Exception as e:
                        log.debug("movebot: delete failed msg id=%s err=%r", msg.id, e)
                    await asyncio.sleep(0.2)

        dest_name = destination.name
        summary = S(
            "move_any.summary",
            copied=copied,
            total=len(to_copy),
            src=source.name,
            dst=dest_name,
        )
        if failed:
            summary += " " + S("move_any.summary_failed_tail", failed=len(failed))
        if delete_original:
            summary += " " + S("move_any.summary_deleted_tail", deleted=deleted)

        if debug and failed:
            top = "\n".join(f"- {mid}: {err}" for mid, err in failed[:10])
            summary += f"\n```\nFailures (first {min(10, len(failed))}):\n{top}\n```"

        await interaction.followup.send(summary, ephemeral=True)

    @group.command(
        name="pinmatch",
        description="Mirror pins from a source channel/thread into a destination by content matching (no mapping needed)."
    )
    @app_commands.describe(
        source_id="ID (or jump URL) of the source TextChannel/Thread with pins.",
        destination_id="ID (or jump URL) of the destination TextChannel/Thread to pin in.",
        search_depth="How many destination messages to scan (default 3000).",
        allow_header="Set true if copies may include a header/backlink you want ignored.",
        ignore_case="Case-insensitive content match (default true).",
        collapse_ws="Collapse whitespace for matching (default true).",
        min_fuzzy="Minimum fuzzy ratio (0.0–1.0) if exact content match fails (default 0.88).",
        ts_slack_seconds="Timestamp slack for choosing among candidates (default 240s).",
    )
    async def pinmatch(
        self,
        interaction: discord.Interaction,
        source_id: str,
        destination_id: str,
        search_depth: int = 3000,
        allow_header: bool = True,
        ignore_case: bool = True,
        collapse_ws: bool = True,
        min_fuzzy: float = 0.88,
        ts_slack_seconds: int = 240,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)

        def _parse_id(s: str) -> Optional[int]:
            s = s.strip()
            try:
                return int(s)
            except Exception:
                try:
                    return int(s.rstrip("/").split("/")[-1])
                except Exception:
                    return None

        gid = interaction.guild_id
        src_ident = _parse_id(source_id)
        dst_ident = _parse_id(destination_id)
        if not src_ident or not dst_ident:
            return await interaction.followup.send("Bad IDs. Pass channel/thread IDs or jump URLs.", ephemeral=True)

        source = await _resolve_messageable_from_id(self.bot, gid, src_ident)
        destination = await _resolve_messageable_from_id(self.bot, gid, dst_ident)
        if not isinstance(source, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send("Source must be a TextChannel or Thread.", ephemeral=True)
        if not isinstance(destination, (discord.TextChannel, discord.Thread)):
            return await interaction.followup.send("Destination must be a TextChannel or Thread.", ephemeral=True)

        me = interaction.guild.me
        if not source.permissions_for(me).read_message_history:
            return await interaction.followup.send("I need **Read Message History** in the source.", ephemeral=True)
        if not (destination.permissions_for(me).read_message_history and destination.permissions_for(me).manage_messages):
            return await interaction.followup.send("I need **Read Message History** and **Manage Messages** in the destination.", ephemeral=True)

        try:
            src_pins = await source.pins()
        except discord.Forbidden:
            return await interaction.followup.send("Forbidden reading source pins.", ephemeral=True)

        if not src_pins:
            return await interaction.followup.send("No pins in the source.", ephemeral=True)

        dest_msgs: List[discord.Message] = []
        async for m in destination.history(limit=search_depth, oldest_first=False):
            dest_msgs.append(m)

        by_content: Dict[str, List[discord.Message]] = {}
        by_attach: Dict[str, List[discord.Message]] = {}
        for dm in dest_msgs:
            if dm.type != discord.MessageType.default:
                continue
            key = _normalize_content(dm.content or "", allow_header=allow_header, ignore_case=ignore_case, collapse_ws=collapse_ws)
            by_content.setdefault(key, []).append(dm)
            sig = _attach_sig(dm)
            if sig:
                by_attach.setdefault(sig, []).append(dm)

        pinned = 0
        misses: List[int] = []

        for sm in src_pins:
            if sm.type != discord.MessageType.default:
                misses.append(sm.id)
                continue

            src_key = _normalize_content(sm.content or "", allow_header=False, ignore_case=ignore_case, collapse_ws=collapse_ws)
            src_sig = _attach_sig(sm)
            timestamp = int(sm.created_at.timestamp())

            candidates: List[discord.Message] = []

            if src_key:
                candidates = list(by_content.get(src_key, []))

            if src_sig:
                with_attach = by_attach.get(src_sig, [])
                if candidates:
                    ids = {m.id for m in candidates}
                    both = [m for m in with_attach if m.id in ids]
                    candidates = both or candidates
                else:
                    candidates = list(with_attach)

            if not candidates and src_key:
                best: Optional[tuple[discord.Message, float]] = None
                for dm in dest_msgs:
                    if dm.type != discord.MessageType.default:
                        continue
                    dkey = _normalize_content(dm.content or "", allow_header=allow_header, ignore_case=ignore_case, collapse_ws=collapse_ws)
                    if not dkey:
                        continue
                    r = _fuzzy_ratio(src_key, dkey)
                    if r >= min_fuzzy and (best is None or r > best[1]):
                        best = (dm, r)
                if best:
                    candidates = [best[0]]

            if not candidates:
                misses.append(sm.id)
                continue

            candidates.sort(key=lambda m: abs(int(m.created_at.timestamp()) - timestamp))
            target = candidates[0]

            try:
                await target.pin()
                pinned += 1
                await asyncio.sleep(0.3)
            except Exception:
                misses.append(sm.id)

        summary = f"Pinned **{pinned}** out of **{len(src_pins)}** source pin(s) in {destination.mention}."
        if misses:
            sample = "\n".join(f"- {mid}" for mid in misses[:10])
            summary += f"\nMissed **{len(misses)}** (first 10 IDs):\n```\n{sample}\n```"
        await interaction.followup.send(summary, ephemeral=True)


async def setup(bot: commands.Bot):
    await bot.add_cog(MoveAnyCog(bot))
