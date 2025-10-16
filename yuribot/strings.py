from __future__ import annotations

from typing import Any, Mapping


class _NeutralMap(dict[str, str]):

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
            # Fallback to first value in the mapping, if it's a string
            try:
                first_val = next(iter(value.values()))
                return first_val if isinstance(first_val, str) else ""
            except StopIteration:
                return ""
        # Unknown type → stringify conservatively
        return str(value)


# Global storage for strings
_STRINGS: dict[str, str] = _NeutralMap()


def S(key: str, /, **fmt: Any) -> str:
    text = _STRINGS.get(key, key)
    if fmt:
        try:
            return text.format(**fmt)
        except Exception:
            # Return the raw template if formatting failed (missing arg, etc.)
            return text
    return text


# Optional short alias
T = S

_STRINGS.update({
    # ---- Common ----
    "common.guild_only": "This command can only be used in a server.",
    "common.need_manage_server": "You need **more permissions for that.**.",

    # ---- Activity ----
    "activity.leaderboard.title": "Activity Leaderboard",
    "activity.leaderboard.row": "{i}. {name} — **{count}**",
    "activity.leaderboard.empty": "No data.",
    "activity.leaderboard.footer_month": "Top {limit} — {month}",
    "activity.leaderboard.footer_all": "Top {limit} — all time",
    "activity.me.title": "Your activity — {user}",
    "activity.me.month": "This month ({month})",
    "activity.me.total": "Total",
    "activity.me.recent": "Recent months",
    "activity.none_yet": "No Data.",
    "activity.reset.need_month": "Provide `month` (YYYY-MM) for monthly reset.",
    "activity.reset.done": "Stats wiped.",

    # ---- Admin ----
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

    # ---- Botlog (audit) ----
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

    # ---- Collection ----
    "collection.error.no_cfg_with_hint": "No config for club '{club}'. Run /club setup.",
    "collection.error.no_cfg": "No config for club '{club}'.",
    "collection.error.no_open": "No open collection.",
    "collection.reply.opened": "{club} collection opened (#{id}).",
    "collection.reply.closed": "{club} collection #{id} closed.",
    "collection.announce.open": (
        "**{club} submissions open** — closes <t:{closes_unix}:R>.\n"
        "Post new entries in **{planning_name}**; the bot will auto-register them."
    ),
    "collection.embed.title": "{club} — Submissions (Collection #{id}: {status})",
    "collection.embed.item_name": "{i}. {title}",
    "collection.embed.item_value": "{link} • by <@{author_id}> • thread: <#{thread_id}>",
    "collection.error.no_windows": "No collection windows yet.",
    "collection.error.no_submissions": "No submissions in the current collection.",
    "collection.common.no_link": "(no link)",
    "collection.thread.registered": "Registered this **{club_upper}** submission for the current collection.",

    # ---- Emoji / Sticker stats ----
    "emoji.title": "Emoji usage — {month}",
    "emoji.none_for_month": "No emoji usage for **{month}**.",
    "emoji.row": "{display} — **{count}** ({src})",
    "emoji.src.message": "message",
    "emoji.src.reaction": "reaction",

    "sticker.title": "Sticker usage — {month}",
    "sticker.none_for_month": "No sticker usage for **{month}**.",
    "sticker.row": "{name} — **{count}**",

    # ---- Modlog ----
    "modlog.error.perms": "Insufficient permissions.",
    "modlog.error.no_channel_set": "Mod logs channel not set. Run `/set_mod_logs` first.",
    "modlog.error.bad_channel_config": "Configured mod logs channel is invalid. Re-run `/set_mod_logs`.",
    "modlog.reply.logged": "Logged.",

    "modlog.title": "Moderation Action",
    "modlog.field.user": "User",
    "modlog.field.rule": "Rule",
    "modlog.field.offense": "Offense",
    "modlog.field.action": "Action",
    "modlog.field.details": "Details",
    "modlog.footer.actor": "Actor: {actor} ({actor_id})",

    "modlog.history.none": "No entries.",
    "modlog.history.title": "Modlog — {user} (`{user_id}`)",
    "modlog.history.row": "**Rule:** {rule}\n**Offense:** {offense}\n**Action:** {action}\n**When:** {when}\n**By:** <@{actor_id}>",
    "modlog.history.details": "**Details:** {details}",
    "modlog.history.evidence": "**Evidence:** {url}",

    # ---- Movie club ----
    "movie.location.default": "Projection Booth",
    "movie.error.perms": "You need **Manage Server** or **Manage Events** to schedule movie showings.",
    "movie.error.forbidden": "I’m missing permission to create scheduled events in this server.",
    "movie.error.http": "Discord API error while creating events: {error}",
    "movie.event.name_morning": "{title} — Morning Showing",
    "movie.event.name_evening": "{title} — Evening Showing",
    "movie.desc.header": "Movie Night: {title}",
    "movie.desc.link": "Link: {link}",
    "movie.scheduled.title": "🎬 Movie Scheduled",
    "movie.scheduled.desc": "**{title}**\nMorning: <t:{am}:F>\nEvening: <t:{pm}:F>",
    "movie.field.venue": "Venue",
    "movie.field.location": "Location",
    "movie.field.duration": "Duration",
    "movie.value.duration_min": "{minutes} min",
    "movie.field.link": "Link",
    "movie.field.events": "Events",
    "movie.value.events_links": "[Morning]({am_url}) • [Evening]({pm_url})",

    # ---- Music ----
    "music.duration.live": "live/unknown",
    "music.error.join_first": "Join a voice channel first (or pass one).",
    "music.error.resolve": "Failed to resolve audio: `{error}`",
    "music.error.no_audio": "No playable audio found.",
    "music.error.nothing_playing": "Nothing is playing.",
    "music.error.nothing_to_resume": "Nothing to resume.",
    "music.error.not_connected": "Not connected.",
    "music.joined": "Joined **{name}**.",
    "music.left": "Left voice. Queue cleared.",
    "music.paused": "Paused.",
    "music.resumed": "Resumed.",
    "music.skipped": "Skipped.",
    "music.stopped": "Stopped playback and cleared queue.",
    "music.now": "**Now playing:** {title} ({duration})\n{url}",
    "music.queue.empty": "Queue is empty.",
    "music.queue.line": "{idx}. {title} ({duration})",
    "music.queue.more": "\n…and {more} more.",
    "music.queue.where_now": "now",
    "music.queue.where_pos": "position **#{pos}**",
    "music.queued.single": "Queued **{title}** ({duration}) — {where}.",
    "music.queued.bulk": "Queued **{count}** tracks{more_text}.",

    # ---- Polls ----
    "poll.create.error.no_cfg": "No config for club '{club}'. Run /club setup.",
    "poll.create.error.no_collection": "No collection found.",
    "poll.create.error.no_valid_numbers": "No valid numbers.",
    "poll.create.error.bad_channel": "Polls channel invalid.",
    "poll.create.title": "📊 {club} Poll",
    "poll.create.desc": "From collection #{cid}",
    "poll.options_title": "Options",
    "poll.option.bullet": "• {label}",
    "poll.create.posted": "Poll #{id} posted in {channel}.",
    "poll.close.not_found": "Poll not found.",
    "poll.close.results_header": "**Results:**",
    "poll.close.result_line": "{label}: **{count}**",
    "poll.close.closed": "Poll #{id} closed.",

    # ---- Series ----
    "series.error.no_cfg": "No config for club '{club}'.",
    "series.error.no_collection": "No collection found.",
    "series.error.bad_number": "Invalid number.",
    "series.set_from_number.ok": "{club}: Active series set to **{title}** (series #{id}).",
    "series.set_manual.ok": "{club}: Active series set to **{title}** (series #{id}).",
    "series.list.none": "No series yet.",
    "series.list.title": "{club} — Series",
    "series.list.row_title": "#{id} — {title} [{status}]",
    "series.list.no_link": "(no link)",
    "series.plan.error.not_found": "Series not found.",
    "series.plan.error.no_active": "No active series.",
    "series.plan.label": "Ch. {s}–{e}",
    "series.plan.event_name": "{title} — {label} Discussion",
    "series.plan.desc_with_link": "Discuss {title} {label}. Link: {link}",
    "series.plan.desc_no_link": "Discuss {title} {label}.",
    "series.plan.location": "YuriCafe",
    "series.plan.summary": (
        "{club}: Created {created}/{total} discussion events for **{title}** "
        "starting <t:{first_ts}:F>, cadence every {cadence} day(s)."
    ),
    "series.plan.summary_fail_tail": " {fail} failed.",

    # ---- Discuss thread auto-post ----
    "discuss.thread.title": "{title} — {label} Discussion",
    "discuss.thread.body.header": "Discussion for **{title} {label}**.",
    "discuss.thread.body.ref": "Reference: {link}",
    "discuss.thread.body.event": "Event link: {url}",

    # ---- Stats (/ping, /uptime, /botinfo) ----
    "stats.common.na": "n/a",
    "stats.ping.message": "🏓 **Ping**\n• Gateway: `{gw_ms} ms`\n• Round-trip: `{rt_ms} ms`",
    "stats.uptime.title": "⏱️ Uptime",
    "stats.uptime.field.uptime": "Uptime",
    "stats.uptime.field.since": "Since (UTC)",
    "stats.botinfo.title": "Bot Info",
    "stats.botinfo.field.guilds": "Guilds",
    "stats.botinfo.field.members": "Members (cached)",
    "stats.botinfo.field.humans_bots": "Humans / Bots",
    "stats.botinfo.value.humans_bots": "{humans} / {bots}",
    "stats.botinfo.field.commands": "Commands",
    "stats.botinfo.field.shard": "Shard",
    "stats.botinfo.field.gateway": "Gateway Ping",
    "stats.botinfo.field.memory": "Memory",
    "stats.botinfo.field.cpu": "CPU",
    "stats.botinfo.field.runtime": "Runtime",
    "stats.botinfo.value.runtime": "py {py} · discord.py {dpy}",

    # ---- Timeout (moderation) ----
    "timeout.error.self": "You can’t timeout yourself.",
    "timeout.error.owner": "You can’t timeout the server owner.",
    "timeout.error.actor_perms": "You need **Moderate Members** (or higher) permission.",
    "timeout.error.bot_perms": "I’m missing the **Moderate Members** permission.",
    "timeout.error.bot_hierarchy": "My top role is not above the target’s top role.",
    "timeout.error.actor_hierarchy": "Your top role must be above the target’s top role.",
    "timeout.error.min_duration": "Duration must be at least **1 minute**.",
    "timeout.error.forbidden_apply": "Forbidden: I lack permission to timeout that member.",
    "timeout.error.http_apply": "HTTP error applying timeout: {err}",
    "timeout.error.forbidden_remove": "Forbidden: I lack permission to remove timeout.",
    "timeout.error.http_remove": "HTTP error removing timeout: {err}",

    "timeout.dm.title": "You’ve been timed out in {guild}",
    "timeout.dm.no_reason": "No reason provided.",
    "timeout.dm.field.duration": "Duration",
    "timeout.dm.value.duration": "{d}d {h}h {m}m",
    "timeout.dm.field.until": "Until (UTC)",
    "timeout.audit.default_reason": "Timed out by moderator.",
    "timeout.audit.remove_reason": "Timeout removed by moderator.",
    "timeout.log.title": "Member Timed Out",
    "timeout.log.field.user": "User",
    "timeout.log.field.by": "By",
    "timeout.log.field.duration": "Duration",
    "timeout.log.field.until": "Until (UTC)",
    "timeout.log.field.reason": "Reason",
    "timeout.done": "Timed out {user} for **{d}d {h}h {m}m** (until <t:{until_ts}:F> UTC).",
    "timeout.remove.title": "Timeout Removed",
    "timeout.remove.done": "Removed timeout from {user}.",

    # ---- Welcome ----
    "welcome.title": "Welcome!",
    "welcome.desc": "Hey {mention}, you’re our **{ordinal}** member. Glad you’re here.",
    "welcome.content": "{mention}",
    # Coin
    "fun.coin.title": "🪙 Coin Flip",
    "fun.coin.results": "{heads} Heads, {tails} Tails",
    "fun.coin.sequence": "{seq}",
    "fun.coin.limit": "You can flip between 1 and {max} coins.",
    "fun.coin.invalid_count": "Enter a number between 1 and {max}.",

    # Dice
    "fun.dice.title": "🎲 Dice Roll",
    "fun.dice.rolls_line": "{spec}: {rolls}{mod_text} → **{total}**",
    "fun.dice.mod_text": " (modifier {mod})",
    "fun.dice.total": "Grand total: **{total}**",
    "fun.dice.limit": "Too many dice requested (max {max_dice} dice total).",
    "fun.dice.invalid_spec": "Couldn’t parse `{text}`. Try formats like `d20`, `2d6`, or `3d8+2`.",
    "move_any.header":              "**{author}** — {ts}\n{jump}",
    "move_any.sticker.line_with_url": "[Sticker: {name}]({url})",
    "move_any.sticker.line_no_url":   "(Sticker: {name})",
    #movebot
    "move_any.thread.created_body":   "Post created by bot to receive copied messages.",
    "move_any.thread.starter_msg":    "Starting thread **{title}** for copied messages…",

    "move_any.error.bad_ids":         "Invalid source_id or destination_id.",
    "move_any.error.bad_source_type": "Source must be a Text Channel or Thread.",
    "move_any.error.bad_dest_type":   "Destination must be a Text Channel, Thread, or Forum Channel.",
    "move_any.error.need_read_history": "I need **Read Message History** in the source.",
    "move_any.error.need_send_messages": "I need **Send Messages** in the destination.",
    "move_any.error.need_attach_files":  "I need **Attach Files** in the destination.",
    "move_any.error.forbidden_read_source": "Forbidden to read the source history.",
    "move_any.error.forum_needs_title": "Destination is a forum. Please provide `dest_thread_title` to create a post.",
    "move_any.error.forbidden_forum":  "Forbidden: cannot create a post in that forum.",
    "move_any.error.create_forum_failed": "Failed to create forum post: {err}",
    "move_any.error.forbidden_thread": "Forbidden: cannot create a thread in that channel.",
    "move_any.error.create_thread_failed": "Failed to create thread: {err}",
    "move_any.error.unsupported_destination": "Unsupported destination channel type.",

    "move_any.info.none_matched":     "No messages matched that range.",
    "move_any.info.dry_run":          "Dry run: would copy **{count}** message(s) from **{src}** → **{dst}**.",
    "move_any.info.webhook_fallback": "Couldn’t use a webhook (missing permission or create failed); falling back to normal sending.",

    "move_any.notice.cant_delete_source": "Note: Could not delete originals (missing **Manage Messages** in source).",

    "move_any.summary":               "Copied **{copied}/{total}** message(s) from **{src}** → **{dst}**.",
    "move_any.summary_failed_tail":   "Failed: {failed}.",
    "move_any.summary_deleted_tail":  "Deleted original: {deleted}.",
    "move_any.reply.header": "Replying to **{author}** · {jump}\n> {snippet}",
    "move_any.reply.attach_only": "(attachment)",
})



