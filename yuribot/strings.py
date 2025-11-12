from __future__ import annotations
from typing import Any, Mapping, Dict
import random

# ===============================================================
# Persona / flavor toggles
# ===============================================================
RIN_PERSONA_ENABLED = True  # master switch (False = fully neutral)
RIN_FLAVOR_PROB = 0.0  # 0.0-1.0 chance to append a short quip

# Eligible prefixes (light, SFW sass)
_RIN_ALLOW_PREFIXES: tuple[str, ...] = (
    "common.",
    "poll.",
    "tools.",
    "welcome.",
    "fun.",
    "move_any.",
    "mu.",
)

# Never flavor these (serious/audit)
_RIN_DENY_PREFIXES: tuple[str, ...] = (
    "botlog.",
    "modlog.",
    "timeout.",
    "admin.",
)

# Tiny quip pools
_RIN_Q: Dict[str, list[str]] = {
    "ok": ["ok ok~", "kinda slayed ngl", "noted 💅", "heard ya", "mkay~"],
    "oops": ["eep—my bad", "uhh yikes", "scuffed…", "whoopsies", "brb crying"],
    "hint": [
        "you got this",
        "i believe in u",
        "pro gamer move time",
        "brain on pls",
        "tiny hint: read closely",
    ],
}


def _rin_pick(kind: str) -> str:
    pool = _RIN_Q.get(kind, [])
    return random.choice(pool) if pool else ""


# ===============================================================
# Storage + formatting helpers
# ===============================================================
class _NeutralMap(dict[str, str]):
    """
    Accepts values as either strings or mappings.
    If a mapping is provided, prefer 'neutral'; else first string value.
    """

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, self._flatten(value))

    def update(self, other: Mapping[str, Any] | None = None, /, **kwargs: Any) -> None:  # type: ignore[override]
        if other:
            for k, v in other.items():
                super().__setitem__(k, self._flatten(v))
        for k, v in kwargs.items():
            super().__setitem__(k, self._flatten(v))

    @staticmethod
    def _flatten(value: Any) -> str:
        if isinstance(value, str):
            return value
        if isinstance(value, Mapping):
            if "neutral" in value and isinstance(value["neutral"], str):
                return value["neutral"]
            try:
                first_val = next(iter(value.values()))
                return first_val if isinstance(first_val, str) else ""
            except StopIteration:
                return ""
        return str(value)


_STRINGS: dict[str, str] = _NeutralMap()


def _eligible_for_flavor(key: str) -> bool:
    if not RIN_PERSONA_ENABLED:
        return False
    if any(key.startswith(p) for p in _RIN_DENY_PREFIXES):
        return False
    return any(key.startswith(p) for p in _RIN_ALLOW_PREFIXES)


def _pepper(text: str, key: str) -> str:
    """Append a tiny quip to eligible messages (never alters placeholders)."""
    if not _eligible_for_flavor(key) or random.random() > RIN_FLAVOR_PROB:
        return text
    lower = text.lower()
    if any(
        w in lower
        for w in (
            "error",
            "failed",
            "couldn't",
            "couldn't",
            "invalid",
            "missing",
            "forbidden",
        )
    ):
        quip = _rin_pick("oops")
    elif any(w in lower for w in ("try", "hint", "help", "use", "provide", "format")):
        quip = _rin_pick("hint")
    else:
        quip = _rin_pick("ok") or "ok~"
    return f"{text} {quip}" if text.endswith((".", "!", "…")) else f"{text} — {quip}"


def S(key: str, /, **fmt: Any) -> str:
    """Lookup + format with optional persona quip. Safe on format errors."""
    template = _STRINGS.get(key, key)
    try:
        text = template.format(**fmt) if fmt else template
    except Exception:
        text = template
    return _pepper(text, key)


# Optional alias
T = S


# ===============================================================
# String table
# ===============================================================

_STRINGS.update(
    {
        # ---------------- Common ----------------
        "common.guild_only": "This command can only be used in a server.",
        "common.need_manage_server": "You need **Manage Server** (or higher) permission.",
        "common.need_manage_server_v2": "You need **Manage Server** (or higher) permission.",
        "common.error_generic": "Something went wrong. Try again or ping a moderator.",
        # ---------------- Admin ----------------
        "admin.welcome.set_ok": "Welcome messages will post in {channel} using image `{filename}`.",
        "admin.botlogs.set_ok": "Bot logs will be posted in {channel}.",
        "admin.modlogs.set_ok": "Mod logs will be posted in {channel}.",
        "admin.setup.configured": (
            "Configured **{club}** (club #{id}).\n"
            "- Announcements: {ann}\n"
            "- Planning forum: {planning}\n"
            "- Polls: {polls}\n"
            "- Discussion forum: {discussion}"
        ),
        "admin.mu_forum.set_ok": "MangaUpdates forum set to {channel}.",
        # ---------------- Botlog (audit) ----------------
        "botlog.common.none": "(none)",
        "botlog.common.unknown": "(unknown)",
        "botlog.title.message_created": "Message Created",
        "botlog.title.message_deleted": "Message Deleted",
        "botlog.title.message_edited": "Message Edited",
        "botlog.title.bulk_delete": "Bulk Message Delete",
        "botlog.title.invite_created": "Invite Created",
        "botlog.title.invite_deleted": "Invite Deleted",
        "botlog.title.member_join": "Member Join",
        "botlog.title.member_leave": "Member Leave",
        "botlog.title.nick_change": "Nickname Change",
        "botlog.title.member_roles_updated": "Member Roles Updated",
        "botlog.title.timeout_updated": "Member Timeout Updated",
        "botlog.title.member_banned": "Member Banned",
        "botlog.title.member_unbanned": "Member Unbanned",
        "botlog.title.role_created": "Role Created",
        "botlog.title.role_deleted": "Role Deleted",
        "botlog.title.role_updated": "Role Updated",
        "botlog.title.channel_created": "Channel Created",
        "botlog.title.channel_deleted": "Channel Deleted",
        "botlog.title.channel_updated": "Channel Updated",
        "botlog.title.emoji_created": "Emoji Created",
        "botlog.title.emoji_deleted": "Emoji Deleted",
        "botlog.title.emoji_renamed": "Emoji Renamed",
        "botlog.title.voice_join": "Voice Join",
        "botlog.title.voice_leave": "Voice Leave",
        "botlog.title.voice_move": "Voice Move",
        "botlog.field.author": "Author",
        "botlog.field.channel": "Channel",
        "botlog.field.content": "Content",
        "botlog.field.attachments": "Attachments",
        "botlog.field.deleted_attachments": "Deleted Attachments",
        "botlog.field.jump": "Jump",
        "botlog.field.before": "Before",
        "botlog.field.after": "After",
        "botlog.field.count": "Count",
        "botlog.field.code": "Code",
        "botlog.field.inviter": "Inviter",
        "botlog.field.max_uses": "Max Uses",
        "botlog.field.max_age_seconds": "Max Age (s)",
        "botlog.field.user": "User",
        "botlog.field.roles_added": "Added",
        "botlog.field.roles_removed": "Removed",
        "botlog.field.role": "Role",
        "botlog.field.changes": "Changes",
        "botlog.field.emojis": "Emojis",
        "botlog.field.from": "From",
        "botlog.field.to": "To",
        "botlog.change.role_name": "Name: **{before}** → **{after}**",
        "botlog.change.role_color": "Color: {before} → {after}",
        "botlog.change.role_perms": "Permissions changed",
        "botlog.change.channel_name": "Name: **{before}** → **{after}**",
        "botlog.change.channel_topic": "Topic changed",
        "botlog.change.channel_nsfw": "NSFW: {before} → {after}",
        # ---------------- Modlog ----------------
        "modlog.err.perms": "Insufficient permissions.",
        "modlog.err.no_channel": "Mod logs channel not set. Run `/set_mod_logs` first.",
        "modlog.err.bad_channel": "Configured mod logs channel is invalid. Re-run `/set_mod_logs`.",
        "modlog.done": "Logged.",
        "modlog.temp.gentle": "🟢 Gentle Nudge",
        "modlog.temp.formal": "💙 Formal Warning",
        "modlog.temp.escalated": "💜 Escalated Warning",
        "modlog.temp.critical": "❤️ Critical / Harmful Behavior",
        "modlog.temp.unknown": "Temp {n}",
        "modlog.embed.title": "Moderation — {temp}",
        "modlog.embed.user": "User",
        "modlog.embed.rule": "Rule",
        "modlog.embed.temperature": "Temperature",
        "modlog.embed.reason": "Reason",
        "modlog.embed.details": "Details",
        "modlog.embed.actions": "Actions",
        "modlog.embed.footer": "Actor: {actor} ({actor_id})",
        "modlog.dm.title": "Moderation Notice",
        "modlog.dm.rule": "Rule",
        "modlog.dm.status": "Status",
        "modlog.dm.status_open": "Open — You can reply to this DM to discuss or request mediation. A moderator will review.",
        "modlog.dm.reason": "Reason",
        "modlog.dm.detail": "Detail",
        "modlog.dm.actions": "Actions",
        "modlog.dm.actions_warning": "Warning recorded",
        "modlog.dm.could_not_dm": "Could not DM {user} (privacy settings).",
        "modlog.reason.timeout_default": "Timed out by moderator.",
        "modlog.reason.ban_default": "Banned by moderator.",
        "modlog.action.timeout.denied_perm": "Timeout requested ({m}m) — **denied** (missing permission).",
        "modlog.action.timeout.ok": "Timeout: {m} minutes",
        "modlog.action.timeout.forbidden": "Timeout requested ({m}m) — **forbidden**.",
        "modlog.action.timeout.http": "Timeout requested ({m}m) — **HTTP error**: {err}",
        "modlog.action.ban.denied_perm": "Ban requested — **denied** (missing permission).",
        "modlog.action.ban.ok": "Ban: **applied**",
        "modlog.action.ban.forbidden": "Ban requested — **forbidden**.",
        "modlog.action.ban.http": "Ban requested — **HTTP error**: {err}",
        "modlog.relay.title": "User Reply (DM)",
        "modlog.relay.footer": "From: {author} ({author_id})",
        "modlog.relay.attachments": "Attachments",
        "modlog.relay.closed": "Relay for {user} closed.",
        "mod.booly.need_user": "Personal scope requires selecting a user.",
        "mod.booly.none": "No messages stored for this scope.",
        "mod.booly.more": "…and {count} more.",
        "mod.booly.title.personal": "Booly messages for {user}",
        "mod.booly.title.scope": "{scope} booly messages",
        "mod.booly.added": "Created booly message #{id}.",
        "mod.booly.updated": "Updated booly message #{id}.",
        "mod.booly.deleted": "Deleted booly message #{id}.",
        "mod.booly.not_found": "No booly message found with ID #{id}.",
        "mod.booly.view.title": "Booly message #{id}",
        "mod.booly.view.scope": "Scope",
        "mod.booly.view.user": "User",
        "mod.booly.view.timestamps": "Created: {created} | Updated: {updated}",
        "mod.booly.scope.global": "Global",
        # ---------------- Music ----------------
        # ---------------- Native Polls ----------------
        "poll.native.group_desc": "Create native Discord polls",
        "poll.native.create_desc": "Create a native poll (up to 6 options) with a custom duration (in hours).",
        "poll.native.arg.question": "Poll question (1-300 chars)",
        "poll.native.arg.opt1": "Option 1",
        "poll.native.arg.opt2": "Option 2",
        "poll.native.arg.opt3": "Option 3 (optional)",
        "poll.native.arg.opt4": "Option 4 (optional)",
        "poll.native.arg.opt5": "Option 5 (optional)",
        "poll.native.arg.opt6": "Option 6 (optional)",
        "poll.native.arg.hours": "How long the poll runs (hours, 1-168). Default 48 (=2 days).",
        "poll.native.arg.multi": "Allow users to select multiple options?",
        "poll.native.arg.ephemeral": "Post ephemerally to the invoker only?",
        "poll.native.err.need_two": "Provide at least **2** options.",
        "poll.native.err.too_many": "Provide **{n}** options or fewer.",
        "poll.native.err.create_failed": "Couldn't create the poll: {err}",
        # ---------------- Timeout (moderation) ----------------
        "timeout.error.self": "You can't timeout yourself.",
        "timeout.error.owner": "You can't timeout the server owner.",
        "timeout.error.actor_perms": "You need **Moderate Members** (or higher) permission.",
        "timeout.error.bot_perms": "I'm missing the **Moderate Members** permission.",
        "timeout.error.bot_hierarchy": "My top role is not above the target's top role.",
        "timeout.error.actor_hierarchy": "Your top role must be above the target's top role.",
        "timeout.error.min_duration": "Duration must be at least **1 minute**.",
        "timeout.error.forbidden_apply": "Forbidden: I lack permission to timeout that member.",
        "timeout.error.http_apply": "HTTP error applying timeout: {err}",
        "timeout.error.forbidden_remove": "Forbidden: I lack permission to remove timeout.",
        "timeout.error.http_remove": "HTTP error removing timeout: {err}",
        "timeout.dm.title": "You've been timed out in {guild}",
        "timeout.dm.no_reason": "No reason provided.",
        "timeout.dm.field.duration": "Duration",
        "timeout.dm.value.duration": "{d}d {h}h {m}m {s}s",
        "timeout.dm.field.until": "Until (UTC)",
        "timeout.audit.default_reason": "Timed out by moderator.",
        "timeout.audit.remove_reason": "Timeout removed by moderator.",
        "timeout.log.title": "Member Timed Out",
        "timeout.log.field.user": "User",
        "timeout.log.field.by": "By",
        "timeout.log.field.duration": "Duration",
        "timeout.log.field.until": "Until (UTC)",
        "timeout.log.field.reason": "Reason",
        "timeout.done": "Timed out {user} for **{d}d {h}h {m}m {s}s** (until <t:{until_ts}:F> UTC).",
        "timeout.remove.title": "Timeout Removed",
        "timeout.remove.done": "Removed timeout from {user}.",
        # ---------------- Welcome ----------------
        "welcome.title": "Welcome!",
        "welcome.desc": "Hey {mention}, you're our **{ordinal}** member. Glad you're here.",
        "welcome.content": "{mention}",
        # ---------------- Fun: Coin ----------------
        "fun.coin.title": "🪙 Coin Flip",
        "fun.coin.results": "{heads} Heads, {tails} Tails",
        "fun.coin.sequence": "{seq}",
        "fun.coin.limit": "You can flip between 1 and {max} coins.",
        "fun.coin.invalid_count": "Enter a number between 1 and {max}.",
        # ---------------- Fun: Dice ----------------
        "fun.dice.title": "🎲 Dice Roll",
        "fun.dice.rolls_line": "{spec}: {rolls}{mod_text} → **{total}**",
        "fun.dice.mod_text": " (modifier {mod})",
        "fun.dice.total": "Grand total: **{total}**",
        "fun.dice.limit": "Too many dice requested (max {max_dice} dice total).",
        "fun.dice.invalid_spec": "Couldn't parse `{text}`. Try formats like `d20`, `2d6`, or `3d8+2`.",
        # ---------------- MoveAny / ThreadTools ----------------
        "move_any.header": "**{author}** — {ts}\n{jump}",
        "move_any.sticker.line_with_url": "[Sticker: {name}]({url})",
        "move_any.sticker.line_no_url": "(Sticker: {name})",
        "move_any.thread.created_body": "Post created by bot to receive copied messages.",
        "move_any.thread.starter_msg": "Starting thread **{title}** for copied messages…",
        "move_any.error.bad_ids": "Invalid source_id or destination_id.",
        "move_any.error.bad_ids_neutral": "Bad IDs. Pass channel/thread IDs or jump URLs.",
        "move_any.error.bad_source_type": "Source must be a Text Channel or Thread.",
        "move_any.error.bad_dest_type": "Destination must be a Text Channel, Thread, or Forum Channel.",
        "move_any.error.bad_dest_type_text_or_thread": "Destination must be a Text Channel or Thread.",
        "move_any.error.need_read_history": "I need **Read Message History** in the source.",
        "move_any.error.need_send_messages": "I need **Send Messages** in the destination.",
        "move_any.error.need_attach_files": "I need **Attach Files** in the destination.",
        "move_any.error.forbidden_read_source": "Forbidden to read the source history.",
        "move_any.error.forum_needs_title": "Destination is a forum. Please provide `dest_thread_title` to create a post.",
        "move_any.error.forbidden_forum": "Forbidden: cannot create a post in that forum.",
        "move_any.error.create_forum_failed": "Failed to create forum post: {err}",
        "move_any.error.forbidden_thread": "Forbidden: cannot create a thread in that channel.",
        "move_any.error.create_thread_failed": "Failed to create thread: {err}",
        "move_any.error.unsupported_destination": "Unsupported destination channel type.",
        "move_any.info.none_matched": "No messages matched that range.",
        "move_any.info.dry_run": "Dry run: would copy **{count}** message(s) from **{src}** → **{dst}**.",
        "move_any.info.webhook_fallback": "Couldn't use a webhook (missing permission or create failed); falling back to normal sending.",
        "move_any.notice.cant_delete_source": "Note: Could not delete originals (missing **Manage Messages** in source).",
        "move_any.summary": "Copied **{copied}/{total}** message(s) from **{src}** → **{dst}**.",
        "move_any.summary_failed_tail": "Failed: {failed}.",
        "move_any.summary_deleted_tail": "Deleted original: {deleted}.",
        "move_any.reply.header": "Replying to **{author}** · {jump}\n> {snippet}",
        "move_any.reply.attach_only": "(attachment)",
        "move_any.pin.summary": "Pinned **{pinned}** out of **{total}** source pin(s) in {dst}.",
        "move_any.pin.summary_misses_tail": "\nMissed **{missed}** (first {shown} IDs):\n```\n{sample}\n```",
        # ---------------- MangaUpdates ----------------
        "mu.link.need_forum": "Please run this inside a **Forum post** or pass the `thread:` option with a forum post.",
        "mu.link.no_results": "No results on MangaUpdates for **{q}**.",
        "mu.link.no_aliases": "—",
        "mu.link.linked_ok": "Linked **{title}** (id `{sid}`) to forum post **{thread}**.\nAliases: {aliases}",
        "mu.unlink.need_thread": "Run this inside a forum post, or pass a `thread:` to choose one.",
        "mu.unlink.done": "Unlinked **{count}** mapping(s).",
        "mu.status.none": "No threads are currently linked.",
        "mu.status.line": "- **{title}** (`{sid}`) → {thread}",
        "mu.check.need_thread": "Use this inside a **forum post thread**, or pass `thread:`.",
        "mu.check.not_linked": "This thread is not linked to any MangaUpdates series. Use `/mu link` here.",
        "mu.check.posted": "Posted **{count}** new release(s).",
        "mu.check.no_new": "No new releases — posted the latest known chapter instead.",
        "mu.update.title": "{series} — {chbits}",
        "mu.update.footer": "MangaUpdates",
        "mu.batch.title": "{series}: {n} new chapter(s)",
        "mu.batch.footer": "MangaUpdates • batch",
        "mu.batch.line": "• {chbits}{maybe_url}",
        "mu.latest.title": "{series} — Latest: {chbits}",
        "mu.latest.footer": "Latest known (no new posts)",
        "mu.release.generic": "New release",
        "mu.release.group": "Group: **{group}**",
        "mu.release.date_rel": "Date: <t:{ts}:D>",
        "mu.release.date_raw": "Date: {date}",
        "mu.release.title": "{series} — {chbits}",
        "mu.release.footer": "MangaUpdates • Release ID {rid}",
        "mu.error.generic": "MangaUpdates error: {msg}",
        "mu.error.no_releases": "No releases were found for that series.",
        "mu.error.search_http": "MangaUpdates search failed (HTTP {code}).",
        "mu.error.series_http": "MangaUpdates series {sid} failed (HTTP {code}).",
        "mu.error.releases_http": "MangaUpdates releases for {sid} failed (HTTP {code}).",
        # ---------------- Tools: Timestamp ----------------
        "tools.timestamp.invalid_dt": "Invalid date/time. Use `YYYY-MM-DD` and `HH:MM` (or `HH:MM:SS`).",
        "tools.timestamp.build_failed": "Could not build that date/time. Double-check values.",
        "tools.timestamp.title": "🕰 Timestamp Builder",
        "tools.timestamp.copy_field": "Copy-paste",
        "tools.timestamp.footer": "Local input: {local_iso}  •  TZ: {tz}",
        "tools.timestamp.label.relative": "Relative",
        "tools.timestamp.label.full": "Full",
        "tools.timestamp.label.short_dt": "Short DT",
        "tools.timestamp.label.date": "Date",
        "tools.timestamp.label.date_short": "Date (short)",
        "tools.timestamp.label.time": "Time",
        "tools.timestamp.label.time_short": "Time (short)",
        # ---------------- Role Welcome ----------------
        "rolewelcome.title": "Welcome to book club!",
        "rolewelcome.desc": (
            "You've just been granted access. Take a minute to read pinned messages, "
            "introduce yourself, and check the channels unlocked for you."
        ),
        "rolewelcome.footer": "{guild}",
        # ---------------- Archive ----------------
        "archive.backfill.already_running": "An archive task is already running for this server.",
        "archive.backfill.starting": "Starting archive task. This will take a long time. I will send updates as I go.",
        "archive.backfill.found_channels": "Found {count} text-based channels and threads to scan.",
        "archive.backfill.progress_update": "Progress: Archived {count} new messages from {channel}...",
        "archive.backfill.complete": "Archive task complete. Scanned {channels} channels and archived {messages} new messages.",
        "archive.backfill.error": "An error occurred during the archive: {err}",
        # Common
        "common.guild_only": "This only works in a server.",
        # Hints / field help
        "birthday.hint.mmdd": "Birthday in MM-DD format (e.g. 04-13)",
        "birthday.hint.mmdd_optional": "Optional MM-DD to update",
        "birthday.hint.tz": "Optional IANA timezone",
        "birthday.hint.tz_optional": "Optional IANA timezone to update",
        "birthday.hint.user_mod": "(Mods only) Set for this user",
        "birthday.hint.user_mod_view": "(Mods only) View this user's birthday; otherwise shows your own",
        "birthday.hint.user_required_or_self": "User to edit (mods can edit others; defaults to yourself)",
        "birthday.hint.user_optional": "User to list (omit to list all — mods only)",
        "birthday.hint.closeness_optional": "Optional closeness level 1..5 to update",
        # Labels
        "birthday.label.closeness": "closeness",
        "birthday.label.last": "last",
        # Parse errors (raised as ValueError codes)
        "birthday.err.mmdd_format": "Use MM-DD format, e.g. 04-13",
        "birthday.err.mmdd_digits": "Month and day must be numbers",
        "birthday.err.mmdd_month": "Month must be 1-12",
        "birthday.err.mmdd_day": "Day must be 1-31",
        "birthday.err.mmdd_invalid": "Invalid calendar date",
        # Permission / resolution
        "birthday.err.perms_other": "You don't have permission to act on another user.",
        "birthday.err.resolve_self": "Couldn't resolve your member identity.",
        "birthday.err.closeness_range": "Closeness must be between 1 and 5.",
        # Basic flows
        "birthday.saved": "🎂 Saved: **{m}-{d}** (TZ: `{tz}`). I'll DM on the day.",
        "birthday.saved.for": " (for {user})",
        "birthday.removed": "✅ Removed your birthday entry.",
        "birthday.none_self": "No birthday on file. Use `/birthday set`.",
        "birthday.none_other": "{user} doesn't have a birthday set.",
        "birthday.none_target_first": "Set a birthday for that user first with `/birthday set`.",
        # View / list
        "birthday.view.line": "🎂 **{m}-{d}** (TZ: `{tz}`){last_part}{close_part}",
        # (we use a simpler direct format in cog, but retain keys for consistency)
        "birthday.list.title_all": "Stored birthdays (this server)",
        "birthday.list.title_user": "Stored birthday for {user}",
        "birthday.list.empty": "No stored birthdays found.",
        "birthday.list.more": "…and {count} more.",
        # Edit
        "birthday.edit.ok": "✅ Updated.",
        "birthday.edit.noop": "No changes provided.",
        # Closeness
        "birthday.closeness.set": "✅ Set closeness to **{level}**",
        "birthday.closeness.view": "Closeness level: **{level}**",
    }
)
