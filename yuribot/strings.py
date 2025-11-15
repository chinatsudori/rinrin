from __future__ import annotations
from typing import Any, Mapping

# ===============================================================
# Persona / flavor toggles (hard switch for full-string variants)
# ===============================================================
RIN_MODE: str = "rin"  # "neutral" | "rin"


def set_rin_mode(on: bool) -> None:
    """Enable Rin mode when True, neutral when False."""
    global RIN_MODE
    RIN_MODE = "rin" if on else "neutral"


# ===============================================================
# Storage + formatting helpers (variant-aware)
# ===============================================================
class _VariantMap(dict[str, Any]):
    """
    Accept values as:
      - plain strings (treated as {'neutral': value})
      - mappings with 'neutral' and/or 'rin' keys
    """

    @staticmethod
    def _coerce(value: Any) -> Mapping[str, str]:
        if isinstance(value, str):
            return {"neutral": value}
        if isinstance(value, Mapping):
            out: dict[str, str] = {}
            for k, v in value.items():
                if isinstance(v, str):
                    out[str(k)] = v
            if "neutral" not in out:
                try:
                    first_val = next(iter(out.values()))
                    out["neutral"] = first_val if isinstance(first_val, str) else ""
                except StopIteration:
                    out["neutral"] = ""
            return out
        return {"neutral": str(value)}

    def __setitem__(self, key: str, value: Any) -> None:
        super().__setitem__(key, self._coerce(value))

    def update(self, other: Mapping[str, Any] | None = None, /, **kwargs: Any) -> None:  # type: ignore[override]
        if other:
            for k, v in other.items():
                super().__setitem__(k, self._coerce(v))
        for k, v in kwargs.items():
            super().__setitem__(k, self._coerce(v))


_STRINGS: dict[str, Mapping[str, str]] = _VariantMap()


def _pick_template(key: str) -> str:
    entry = _STRINGS.get(key)
    if not entry:
        return key
    # Prefer exact mode, else neutral, else any
    return (
        entry.get(RIN_MODE) or entry.get("neutral") or next(iter(entry.values()), key)
    )


def S(key: str, /, **fmt: Any) -> str:
    """Lookup + format (variant-aware). Safe on format errors."""
    template = _pick_template(key)
    try:
        return template.format(**fmt) if fmt else template
    except Exception:
        return template


# Optional alias
T = S

# ===============================================================
# String table
# ===============================================================

_STRINGS.update(
    {
        # ---------------- Common ----------------
        "common.guild_only": {
            "neutral": "This command can only be used in a server.",
            "rin": "server-only, bestie. dms can't handle this one~",
        },
        "common.need_manage_server": {
            "neutral": "You need **Manage Server** (or higher) permission.",
            "rin": "you'll need **Manage Server** (or higher). ask a grown-up mod ✋",
        },
        "common.need_manage_server_v2": {
            "neutral": "You need **Manage Server** (or higher) permission.",
            "rin": "permission check failed: no **Manage Server**. go poke staff~",
        },
        "common.error_generic": {
            "neutral": "Something went wrong. Try again or ping a moderator.",
            "rin": "scuffed on my end. try again, or ping a mod and i'll behave, promise.",
        },
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
        "poll.native.group_desc": {
            "neutral": "Create native Discord polls",
            "rin": "spin up native discord polls like a pro (its me i’m the pro).",
        },
        "poll.native.create_desc": {
            "neutral": "Create a native poll (up to 6 options) with a custom duration (in hours).",
            "rin": "make a native poll (max 6 options) and set hours.",
        },
        "poll.native.arg.question": {
            "neutral": "Poll question (1-300 chars)",
            "rin": "poll question (1–300 chars, no essays pls)",
        },
        "poll.native.arg.opt1": {
            "neutral": "Option 1",
            "rin": "option 1",
        },
        "poll.native.arg.opt2": {
            "neutral": "Option 2",
            "rin": "option 2",
        },
        "poll.native.arg.opt3": {
            "neutral": "Option 3 (optional)",
            "rin": "option 3 (optional, for indecisive cuties)",
        },
        "poll.native.arg.opt4": {
            "neutral": "Option 4 (optional)",
            "rin": "option 4 (optional)",
        },
        "poll.native.arg.opt5": {
            "neutral": "Option 5 (optional)",
            "rin": "option 5 (optional)",
        },
        "poll.native.arg.opt6": {
            "neutral": "Option 6 (optional)",
            "rin": "option 6 (optional, final boss of choices)",
        },
        "poll.native.arg.hours": {
            "neutral": "How long the poll runs (hours, 1-168). Default 48 (=2 days).",
            "rin": "duration in hours (1–168). default 48 (=2 days). i can count, shocker.",
        },
        "poll.native.arg.multi": {
            "neutral": "Allow users to select multiple options?",
            "rin": "allow multi-select? yes = chaos, no = order.",
        },
        "poll.native.arg.ephemeral": {
            "neutral": "Post ephemerally to the invoker only?",
            "rin": "post ephemerally (only you see)? secret democracy.",
        },
        "poll.native.err.need_two": {
            "neutral": "Provide at least **2** options.",
            "rin": "need **2+** options. single-option polls are just statements.",
        },
        "poll.native.err.too_many": {
            "neutral": "Provide **{n}** options or fewer.",
            "rin": "too many. stick to **{n}** or fewer before my eyes cross.",
        },
        "poll.native.err.create_failed": {
            "neutral": "Couldn't create the poll: {err}",
            "rin": "poll creation borked: {err}",
        },
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
        "welcome.title": {
            "neutral": "Welcome!",
            "rin": "welcomeee ~",
        },
        "welcome.desc": {
            "neutral": "Hey {mention}, you're our **{ordinal}** member. Glad you're here.",
            "rin": "hey {mention}! you’re member **{ordinal}**. get comfy ~.",
        },
        "welcome.content": {
            "neutral": "{mention}",
            "rin": "{mention} ✨",
        },
        # ---------------- Fun: Coin ----------------
        "fun.coin.title": {
            "neutral": "🪙 Coin Flip",
            "rin": "🪙 coin flip (i didn't looksies)",
        },
        "fun.coin.results": {
            "neutral": "{heads} Heads, {tails} Tails",
            "rin": "{heads} heads, {tails} tails — math checks out.",
        },
        "fun.coin.sequence": {
            "neutral": "{seq}",
            "rin": "{seq}  ← looks random enough. pseudo, but cute.",
        },
        "fun.coin.limit": {
            "neutral": "You can flip between 1 and {max} coins.",
            "rin": "pick 1..{max} coins. ask ookami to make me richer if you need more.",
        },
        "fun.coin.invalid_count": {
            "neutral": "Enter a number between 1 and {max}.",
            "rin": "number pls. between 1 and {max}. just numbers.",
        },
        # ---------------- Fun: Dice ----------------
        "fun.dice.title": {
            "neutral": "🎲 Dice Roll",
            "rin": "🎲 dice time",
        },
        "fun.dice.rolls_line": {
            "neutral": "{spec}: {rolls}{mod_text} → **{total}**",
            "rin": "{spec}: {rolls}{mod_text} → **{total}**",
        },
        "fun.dice.mod_text": {
            "neutral": " (modifier {mod})",
            "rin": " (mod {mod})",
        },
        "fun.dice.total": {
            "neutral": "Grand total: **{total}**",
            "rin": "total total: **{total}** — math checked out this time.",
        },
        "fun.dice.limit": {
            "neutral": "Too many dice requested (max {max_dice} dice total).",
            "rin": "that's a lot of dice… most i can hold is {max_dice}. i'm small ok.",
        },
        "fun.dice.invalid_spec": {
            "neutral": "Couldn't parse `{text}`. Try formats like `d20`, `2d6`, or `3d8+2`.",
            "rin": "what's `{text}` supposed to be? try `d20`, `2d6`, or `3d8+2` like a normal gremlin.",
        },
        # ---------------- Fun: rinrinator ----------------
        "fun.akinator.question_title": {
            "neutral": "{mode} — Question {n}",
            "rin": "{mode} — question {n} (don’t overthink it)",
        },
        "fun.akinator.footer": {
            "neutral": "Still considering {count} characters.",
            "rin": "thinking… {count} suspects in the lineup.",
        },
        "fun.akinator.candidates_title": {
            "neutral": "Short list",
            "rin": "short list. shorter than my patience.",
        },
        "fun.akinator.candidates": {
            "neutral": "Top picks: {names}",
            "rin": "prime suspects: {names}",
        },
        "fun.akinator.waiting_guess": {
            "neutral": "I'm thinking…",
            "rin": "loading brain.exe…",
        },
        "fun.akinator.guess_title": {
            "neutral": "{mode} thinks it knows!",
            "rin": "{mode} has a galaxy-brain moment.",
        },
        "fun.akinator.guess_text": {
            "neutral": "I'm about {pct}% sure it's **{name}** from *{series}*.",
            "rin": "{pct}% sure it’s **{name}** from *{series}*. if i’m wrong, you saw nothing.",
        },
        "fun.akinator.reason_title": {
            "neutral": "Why I'm guessing",
            "rin": "why i think i cooked",
        },
        "fun.akinator.no_guess": {
            "neutral": "I couldn't form a confident guess, but here's what I narrowed down.",
            "rin": "no perfect match, but here's my suspects board with yarn strings.",
        },
        "fun.akinator.cancelled": {
            "neutral": "{mode} session closed. Thanks for playing!",
            "rin": "{mode} session yeeted. thx for playing, come bully me again later.",
        },
        "fun.akinator.timeout": {
            "neutral": "{mode} timed out. Run /rinrinator to start a fresh round.",
            "rin": "{mode} got bored and napped. try `/rinrinator` again.",
        },
        "fun.akinator.not_owner": {
            "neutral": "Only {user} can answer for this round.",
            "rin": "hands off, only {user} can push the shiny buttons this round.",
        },
        "fun.akinator.session_closed": {
            "neutral": "This session already ended — run /rinrinator to start again.",
            "rin": "this session's toast — `/rinrinator` for a redo.",
        },
        "fun.akinator.button.guess": {
            "neutral": "Show Guess",
            "rin": "show the risky guess",
        },
        "fun.akinator.button.end": {
            "neutral": "End Game",
            "rin": "end it (mercy)",
        },
        # ---------------- Music ----------------
        "music.track.unknown": {
            "neutral": "Unknown track",
            "rin": "unknown track",
        },
        "music.controller.not_connected": {
            "neutral": "Player is not connected.",
            "rin": "player isnt connected.",
        },
        "music.controller.join_voice": {
            "neutral": "Join my voice channel to use the controller!",
            "rin": "hop in my voice channel if you want the shiny buttons.",
        },
        "music.error.other_client": {
            "neutral": "Another voice client is already running here.",
            "rin": "there's another voice bot hogging the aux.",
        },
        "music.error.join_voice_first": {
            "neutral": "Join a voice channel first!",
            "rin": "join a voice channel first, captain solo.",
        },
        "music.controller.now_playing": {
            "neutral": "Now Playing",
            "rin": "now spinning",
        },
        "music.controller.now_playing_desc": {
            "neutral": "{track}\nRequested by {requester}",
            "rin": "{track}\nrequested by {requester} (taste noted)",
        },
        "music.controller.field_duration": {
            "neutral": "Duration",
            "rin": "length",
        },
        "music.controller.idle": {
            "neutral": "Player Idle",
            "rin": "idle. feed me songs.",
        },
        "music.controller.idle_hint": {
            "neutral": "Add something to the queue with /play",
            "rin": "queue something with /play. i don't DJ dead air.",
        },
        "music.controller.field_loop": {
            "neutral": "Loop",
            "rin": "loop",
        },
        "music.controller.loop_on": {
            "neutral": "On",
            "rin": "on",
        },
        "music.controller.loop_off": {
            "neutral": "Off",
            "rin": "off",
        },
        "music.controller.field_volume": {
            "neutral": "Volume",
            "rin": "volume",
        },
        "music.controller.volume_value": {
            "neutral": "{percent}%",
            "rin": "{percent}%",
        },
        "music.controller.field_up_next": {
            "neutral": "Up Next",
            "rin": "up next",
        },
        "music.controller.up_next_line": {
            "neutral": "{idx}. {track} — {requester}",
            "rin": "{idx}. {track} — {requester}",
        },
        "music.queue.line_now": {
            "neutral": "Now: {track}",
            "rin": "now: {track}",
        },
        "music.queue.line_entry": {
            "neutral": "{idx}. {track} — {duration}",
            "rin": "{idx}. {track} — {duration}",
        },
        "music.queue.embed_title": {
            "neutral": "Queue",
            "rin": "queue",
        },
        "music.cmd.play": {
            "neutral": "Play a song or playlist from YouTube",
            "rin": "play a song/playlist from youtube",
        },
        "music.cmd.pause": {
            "neutral": "Pause the current track",
            "rin": "pause the track",
        },
        "music.cmd.resume": {
            "neutral": "Resume playback",
            "rin": "resume the vibes",
        },
        "music.cmd.skip": {
            "neutral": "Skip the current track",
            "rin": "skip the track",
        },
        "music.cmd.stop": {
            "neutral": "Stop playback and clear the queue",
            "rin": "stop + clear the queue",
        },
        "music.cmd.leave": {
            "neutral": "Disconnect the bot from voice",
            "rin": "kick me from voice",
        },
        "music.cmd.nowplaying": {
            "neutral": "Show the current track",
            "rin": "show what's playing",
        },
        "music.cmd.queue": {
            "neutral": "Show the queue",
            "rin": "show the queue",
        },
        "music.cmd.volume": {
            "neutral": "Set the player volume (1-150)",
            "rin": "set volume (1-150)",
        },
        "music.cmd.controller": {
            "neutral": "Post or refresh the music controller",
            "rin": "post/refresh the controller",
        },
        "music.cmd.playlist": {
            "neutral": "Manage server playlists",
            "rin": "manage server playlists",
        },
        "music.cmd.playlist_save": {
            "neutral": "Save the current queue as a playlist",
            "rin": "save current queue as a playlist",
        },
        "music.cmd.playlist_load": {
            "neutral": "Load a saved playlist into the queue",
            "rin": "load a saved playlist",
        },
        "music.cmd.playlist_delete": {
            "neutral": "Delete a saved playlist",
            "rin": "delete a saved playlist",
        },
        "music.error.no_matches": {
            "neutral": "No matches found.",
            "rin": "found nothing. try a better query.",
        },
        "music.info.queued_single": {
            "neutral": "Queued {title}",
            "rin": "queued {title}",
        },
        "music.info.queued_multi": {
            "neutral": "Queued {count} tracks",
            "rin": "queued {count} tracks",
        },
        "music.error.nothing_playing": {
            "neutral": "Nothing is playing.",
            "rin": "nothing’s playing.",
        },
        "music.info.paused": {
            "neutral": "Paused ⏸️",
            "rin": "paused ⏸️",
        },
        "music.info.resumed": {
            "neutral": "Resumed ▶️",
            "rin": "resumed ▶️",
        },
        "music.error.nothing_to_skip": {
            "neutral": "Nothing to skip.",
            "rin": "nothing to skip.",
        },
        "music.info.skipped": {
            "neutral": "Skipped ⏭️",
            "rin": "skipped ⏭️",
        },
        "music.error.not_connected": {
            "neutral": "I'm not connected.",
            "rin": "i'm not connected.",
        },
        "music.info.stopped": {
            "neutral": "Stopped and cleared the queue.",
            "rin": "stopped + cleared the queue.",
        },
        "music.error.not_in_voice": {
            "neutral": "I'm not in a voice channel.",
            "rin": "i'm not in a voice channel.",
        },
        "music.info.disconnected": {
            "neutral": "Disconnected.",
            "rin": "disconnected.",
        },
        "music.error.queue_empty": {
            "neutral": "Queue is empty.",
            "rin": "queue's empty.",
        },
        "music.info.volume_set": {
            "neutral": "Volume set to {level}%",
            "rin": "volume = {level}%",
        },
        "music.error.nothing_to_control": {
            "neutral": "Nothing to control yet.",
            "rin": "nothing to control yet.",
        },
        "music.info.controller_refreshed": {
            "neutral": "Controller refreshed.",
            "rin": "controller refreshed.",
        },
        "music.info.playlists_none": {
            "neutral": "No playlists saved yet.",
            "rin": "no playlists saved yet.",
        },
        "music.info.playlists_list": {
            "neutral": "Playlists: {names}",
            "rin": "playlists: {names}",
        },
        "music.error.nothing_to_save": {
            "neutral": "Nothing to save.",
            "rin": "nothing to save.",
        },
        "music.error.no_urls_to_save": {
            "neutral": "Unable to save tracks without URLs.",
            "rin": "can't save tracks without URLs.",
        },
        "music.info.playlist_saved": {
            "neutral": "Saved playlist **{name}** with {count} tracks.",
            "rin": "saved **{name}** with {count} tracks.",
        },
        "music.error.playlist_missing": {
            "neutral": "Playlist not found.",
            "rin": "playlist not found.",
        },
        "music.error.resolve_failed": {
            "neutral": "Unable to resolve any tracks from that playlist.",
            "rin": "couldn't resolve any tracks from that playlist.",
        },
        "music.info.playlist_loaded": {
            "neutral": "Loaded playlist **{name}** ({count} tracks).",
            "rin": "loaded **{name}** ({count} tracks).",
        },
        "music.info.playlist_deleted": {
            "neutral": "Deleted playlist **{name}**.",
            "rin": "deleted **{name}**.",
        },
        "music.cmd.group": {
            "neutral": "Music controls",
            "rin": "music controls",
        },
        "music.help.root": {
            "neutral": (
                "Use subcommands: play, pause, resume, skip, stop, leave, nowplaying, "
                "queue, volume, controller, playlist, node"
            ),
            "rin": (
                "use subcommands: play, pause, resume, skip, stop, leave, nowplaying, "
                "queue, volume, controller, playlist, node"
            ),
        },
        "music.node.cmd.group": {
            "neutral": "Lavalink node tools",
            "rin": "lavalink node tools",
        },
        "music.node.cmd.connect": {
            "neutral": "Connect to the Lavalink node now",
            "rin": "force a lavalink node connect now",
        },
        "music.node.none": {
            "neutral": "No nodes registered.",
            "rin": "no nodes registered.",
        },
        "music.node.status_line": {
            "neutral": "{ident} @ {host}:{port} — {status}",
            "rin": "{ident} @ {host}:{port} — {status}",
        },
        "music.node.connect_attempt": {
            "neutral": "Attempted node connect. Use `/music node` to check status.",
            "rin": "tried to connect the node. check `/music node` for status.",
        },
        "music.node.connect_failed": {
            "neutral": "Node connect attempt failed. Check bot logs.",
            "rin": "node connect failed. check the bot logs.",
        },
        # ---------------- MoveAny / ThreadTools ----------------
        "move_any.header": {
            "neutral": "**{author}** — {ts}\n{jump}",
            "rin": "**{author}** — {ts}\n{jump}",
        },
        "move_any.sticker.line_with_url": {
            "neutral": "[Sticker: {name}]({url})",
            "rin": "[sticker: {name}]({url})",
        },
        "move_any.sticker.line_no_url": {
            "neutral": "(Sticker: {name})",
            "rin": "(sticker: {name})",
        },
        "move_any.thread.created_body": {
            "neutral": "Post created by bot to receive copied messages.",
            "rin": "i made this post to hold copied messages. like a tidy gremlin.",
        },
        "move_any.thread.starter_msg": {
            "neutral": "Starting thread **{title}** for copied messages…",
            "rin": "spinning up **{title}** to stash the copy pasta…",
        },
        "move_any.error.bad_ids": {
            "neutral": "Invalid source_id or destination_id.",
            "rin": "those ids were crunchy. give me valid source/destination pls.",
        },
        "move_any.error.bad_ids_neutral": {
            "neutral": "Bad IDs. Pass channel/thread IDs or jump URLs.",
            "rin": "bad ids. channel/thread ids or jump urls only, ty.",
        },
        "move_any.error.bad_source_type": {
            "neutral": "Source must be a Text Channel or Thread.",
            "rin": "source needs to be a text channel or a thread. no, not a voice cave.",
        },
        "move_any.error.bad_dest_type": {
            "neutral": "Destination must be a Text Channel, Thread, or Forum Channel.",
            "rin": "destination must be text channel, thread, or forum. pick one, chaos goblin.",
        },
        "move_any.error.bad_dest_type_text_or_thread": {
            "neutral": "Destination must be a Text Channel or Thread.",
            "rin": "destination must be a text channel or thread. forums be like: not today.",
        },
        "move_any.error.need_read_history": {
            "neutral": "I need **Read Message History** in the source.",
            "rin": "i need **Read Message History** in source, or i'm gonna be improving all night.",
        },
        "move_any.error.need_send_messages": {
            "neutral": "I need **Send Messages** in the destination.",
            "rin": "need **Send Messages** in destination. otherwise i mime.",
        },
        "move_any.error.need_attach_files": {
            "neutral": "I need **Attach Files** in the destination.",
            "rin": "need **Attach Files** permission. i can't duct-tape files.",
        },
        "move_any.error.forbidden_read_source": {
            "neutral": "Forbidden to read the source history.",
            "rin": "can't read source history. locked diary energy.",
        },
        "move_any.error.forum_needs_title": {
            "neutral": "Destination is a forum. Please provide `dest_thread_title` to create a post.",
            "rin": "destination is a forum. give `dest_thread_title` or no post for you.",
        },
        "move_any.error.forbidden_forum": {
            "neutral": "Forbidden: cannot create a post in that forum.",
            "rin": "no perms to post in that forum. i'm asking nicely.",
        },
        "move_any.error.create_forum_failed": {
            "neutral": "Failed to create forum post: {err}",
            "rin": "forum post went boom: {err}",
        },
        "move_any.error.forbidden_thread": {
            "neutral": "Forbidden: cannot create a thread in that channel.",
            "rin": "no perms to thread there. tragic.",
        },
        "move_any.error.create_thread_failed": {
            "neutral": "Failed to create thread: {err}",
            "rin": "thread creation faceplanted: {err}",
        },
        "move_any.error.unsupported_destination": {
            "neutral": "Unsupported destination channel type.",
            "rin": "that destination type? nah. pick a supported one.",
        },
        "move_any.info.none_matched": {
            "neutral": "No messages matched that range.",
            "rin": "nothing matched. your range is giving 'empty pantry'.",
        },
        "move_any.info.dry_run": {
            "neutral": "Dry run: would copy **{count}** message(s) from **{src}** → **{dst}**.",
            "rin": "dry run: i'd copy **{count}** msg(s) **{src}** → **{dst}**. no mess yet.",
        },
        "move_any.info.webhook_fallback": {
            "neutral": "Couldn't use a webhook (missing permission or create failed); falling back to normal sending.",
            "rin": "webhook said no (perms or creation fail). falling back to normal sending.",
        },
        "move_any.notice.cant_delete_source": {
            "neutral": "Note: Could not delete originals (missing **Manage Messages** in source).",
            "rin": "note: can't delete originals (need **Manage Messages** in source).",
        },
        "move_any.summary": {
            "neutral": "Copied **{copied}/{total}** message(s) from **{src}** → **{dst}**.",
            "rin": "copied **{copied}/{total}** msg(s) **{src}** → **{dst}**. tidy-ish.",
        },
        "move_any.summary_failed_tail": {
            "neutral": "Failed: {failed}.",
            "rin": "failed: {failed}. i will glare at them.",
        },
        "move_any.summary_deleted_tail": {
            "neutral": "Deleted original: {deleted}.",
            "rin": "deleted originals: {deleted}. squeaky clean.",
        },
        "move_any.reply.header": {
            "neutral": "Replying to **{author}** · {jump}\n> {snippet}",
            "rin": "replying to **{author}** · {jump}\n> {snippet}",
        },
        "move_any.reply.attach_only": {
            "neutral": "(attachment)",
            "rin": "(attachment-only. mysterious...)",
        },
        "move_any.pin.summary": {
            "neutral": "Pinned **{pinned}** out of **{total}** source pin(s) in {dst}.",
            "rin": "pinned **{pinned}/{total}** in {dst}. taste: immaculate.",
        },
        "move_any.pin.summary_misses_tail": {
            "neutral": "\nMissed **{missed}** (first {shown} IDs):\n```\n{sample}\n```",
            "rin": "\nmissed **{missed}** (first {shown} ids):\n```\n{sample}\n```",
        },
        # ---------------- MangaUpdates ----------------
        "mu.link.need_forum": {
            "neutral": "Please run this inside a **Forum post** or pass the `thread:` option with a forum post.",
            "rin": "run inside a **forum post**, or pass `thread:` with a forum post. rules, rules.",
        },
        "mu.link.no_results": {
            "neutral": "No results on MangaUpdates for **{q}**.",
            "rin": "nothing on MangaUpdates for **{q}**. unpopular? or hipster.",
        },
        "mu.link.no_aliases": {
            "neutral": "—",
            "rin": "—",
        },
        "mu.link.linked_ok": {
            "neutral": "Linked **{title}** (id `{sid}`) to forum post **{thread}**.\nAliases: {aliases}",
            "rin": "linked **{title}** (id `{sid}`) → **{thread}**.\naliases: {aliases}",
        },
        "mu.unlink.need_thread": {
            "neutral": "Run this inside a forum post, or pass a `thread:` to choose one.",
            "rin": "do it in a forum post, or pass `thread:` so i can aim properly.",
        },
        "mu.unlink.done": {
            "neutral": "Unlinked **{count}** mapping(s).",
            "rin": "unlinked **{count}** mapping(s). snip snip.",
        },
        "mu.status.none": {
            "neutral": "No threads are currently linked.",
            "rin": "no linked threads rn. tumbleweeds.",
        },
        "mu.status.line": {
            "neutral": "- **{title}** (`{sid}`) → {thread}",
            "rin": "- **{title}** (`{sid}`) → {thread}",
        },
        "mu.check.need_thread": {
            "neutral": "Use this inside a **forum post thread**, or pass `thread:`.",
            "rin": "needs a **forum post thread**, or pass `thread:`. paperwork moment.",
        },
        "mu.check.not_linked": {
            "neutral": "This thread is not linked to any MangaUpdates series. Use `/mu link` here.",
            "rin": "this thread isn't linked. try `/mu link` here and we’ll pretend it always was.",
        },
        "mu.check.posted": {
            "neutral": "Posted **{count}** new release(s).",
            "rin": "posted **{count}** new release(s). grindset.",
        },
        "mu.check.no_new": {
            "neutral": "No new releases — posted the latest known chapter instead.",
            "rin": "no fresh releases — tossed in the latest known chapter as a snack.",
        },
        "mu.update.title": {
            "neutral": "{series} — {chbits}",
            "rin": "{series} — {chbits}",
        },
        "mu.update.footer": {
            "neutral": "MangaUpdates",
            "rin": "MangaUpdates",
        },
        "mu.batch.title": {
            "neutral": "{series}: {n} new chapter(s)",
            "rin": "{series}: {n} new ch(s). speed-read responsibly.",
        },
        "mu.batch.footer": {
            "neutral": "MangaUpdates • batch",
            "rin": "MangaUpdates • batch",
        },
        "mu.batch.line": {
            "neutral": "• {chbits}{maybe_url}",
            "rin": "• {chbits}{maybe_url}",
        },
        "mu.latest.title": {
            "neutral": "{series} — Latest: {chbits}",
            "rin": "{series} — latest: {chbits}",
        },
        "mu.latest.footer": {
            "neutral": "Latest known (no new posts)",
            "rin": "latest known (no new posts). cope.",
        },
        "mu.release.generic": {
            "neutral": "New release",
            "rin": "new drop",
        },
        "mu.release.group": {
            "neutral": "Group: **{group}**",
            "rin": "group: **{group}**",
        },
        "mu.release.date_rel": {
            "neutral": "Date: <t:{ts}:D>",
            "rin": "date: <t:{ts}:D>",
        },
        "mu.release.date_raw": {
            "neutral": "Date: {date}",
            "rin": "date: {date}",
        },
        "mu.release.title": {
            "neutral": "{series} — {chbits}",
            "rin": "{series} — {chbits}",
        },
        "mu.release.footer": {
            "neutral": "MangaUpdates • Release ID {rid}",
            "rin": "MangaUpdates • release id {rid}",
        },
        "mu.error.generic": {
            "neutral": "MangaUpdates error: {msg}",
            "rin": "MangaUpdates error: {msg} (i shook it, didn't help).",
        },
        "mu.error.no_releases": {
            "neutral": "No releases were found for that series.",
            "rin": "no releases found. maybe it's cooking, maybe it's coping.",
        },
        "mu.error.search_http": {
            "neutral": "MangaUpdates search failed (HTTP {code}).",
            "rin": "search borked (HTTP {code}). internet goblins won.",
        },
        "mu.error.series_http": {
            "neutral": "MangaUpdates series {sid} failed (HTTP {code}).",
            "rin": "series {sid} said nope (HTTP {code}).",
        },
        "mu.error.releases_http": {
            "neutral": "MangaUpdates releases for {sid} failed (HTTP {code}).",
            "rin": "releases for {sid} faceplanted (HTTP {code}).",
        },
        # ---------------- Tools: Timestamp ----------------
        "tools.timestamp.invalid_dt": {
            "neutral": "Invalid date/time. Use `YYYY-MM-DD` and `HH:MM` (or `HH:MM:SS`).",
            "rin": "invalid date/time. format like `YYYY-MM-DD` + `HH:MM[:SS]`. make it neat and tidy.",
        },
        "tools.timestamp.build_failed": {
            "neutral": "Could not build that date/time. Double-check values.",
            "rin": "couldn't build that datetime. double-check the bits, pls.",
        },
        "tools.timestamp.title": {
            "neutral": "🕰 Timestamp Builder",
            "rin": "🕰 timestamp builder (timezones fear me, gaooo~)",
        },
        "tools.timestamp.copy_field": {
            "neutral": "Copy-paste",
            "rin": "copy pasta",
        },
        "tools.timestamp.footer": {
            "neutral": "Local input: {local_iso}  •  TZ: {tz}",
            "rin": "local: {local_iso}  •  tz: {tz}",
        },
        "tools.timestamp.label.relative": {
            "neutral": "Relative",
            "rin": "relative",
        },
        "tools.timestamp.label.full": {
            "neutral": "Full",
            "rin": "full",
        },
        "tools.timestamp.label.short_dt": {
            "neutral": "Short DT",
            "rin": "short dt",
        },
        "tools.timestamp.label.date": {
            "neutral": "Date",
            "rin": "date",
        },
        "tools.timestamp.label.date_short": {
            "neutral": "Date (short)",
            "rin": "date (short)",
        },
        "tools.timestamp.label.time": {
            "neutral": "Time",
            "rin": "time",
        },
        "tools.timestamp.label.time_short": {
            "neutral": "Time (short)",
            "rin": "time (short)",
        },
        # ---------------- Role Welcome ----------------
        "rolewelcome.title": "welcome to the yuri book club! 💖",
        "rolewelcome.desc": (
            "haiii {mention}!! rinrin here~\nI'm the super awesome girl created by queen mom Thea~"
            "\nsuper cool vice president blepblep asked me to pass this on, so listen up okay? <:henyaHeart:1432286471837978645>\n\n"
            "> *a message from blepblep:*\n"
            "> welcome to the book club, dear reader. we focus on stories told through comics - "
            "manga, manhwa, webtoons.\n"
            "Each week, members read a chosen series together and chat about it in our forums. "
            "Novels might join the lineup someday, but for now we're staying to comics'.\n\n"
            "how it all works:\n"
            "• suggest new reads or vote with ❤️ in **🗓️bookclub-planning**\n"
            "• the top 3 picks go into a weekend poll in **📢bookclub-announcements**\n"
            "• short series = one week of discussion, longer ones split or use our dual-book system (every two weeks)\n"
            "• polls + discussions usually open fridays 19:00 — but check pins for timezone tweaks."
            "\nMom taught me how to read in America\PST time, but I sometimes make mistakes too.\n\n"
            "chat zones!\n"
            "• **💬bookclub-general** - casual talk, memes, low-key pre-talk about the talk on the book you're reading you plan to have.\n"
            "• **📖bookclub-chapter-discussions** - discuss the book each week, but only if you're ready for spoilers, pls!\n"
            "• **📚ongoing-reading-room** - for unfinished series you just can't wait to gush about."
            "This is a new area we're trying out, so if any series is missing, ask a mod to add it ~\n\n"
            "if anything gets messy, ping the **Yuri Club Council** and we'll sort it out. "
            "rinrin personally guarantees at least 80% competence. <:sadcrydepression:1432289105131081738>\n\n"
            "sooo yeah! read the pins, say hi, and dive into comfy chaos with us. "
            "\nblepblep says we're happy you're here- and i second that x 100! welcome to **{guild}** 🌸"
        ),
        "rolewelcome.footer": "delivered by rinrin • {guild}",
        # ---------------- Archive ----------------
        "archive.backfill.already_running": "An archive task is already running for this server.",
        "archive.backfill.starting": "Starting archive task. This will take a long time. I will send updates as I go.",
        "archive.backfill.found_channels": "Found {count} text-based channels and threads to scan.",
        "archive.backfill.progress_update": "Progress: Archived {count} new messages from {channel}...",
        "archive.backfill.complete": "Archive task complete. Scanned {channels} channels and archived {messages} new messages.",
        "archive.backfill.error": "An error occurred during the archive: {err}",
        # Hints / field help
        "birthday.hint.mmdd": {
            "neutral": "Birthday in MM-DD format (e.g. 04-13)",
            "rin": "birthday as MM-DD (e.g. 04-13). no slashes, no chaos.",
        },
        "birthday.hint.mmdd_optional": {
            "neutral": "Optional MM-DD to update",
            "rin": "optional MM-DD to update (precision beats guesses. you wouldnt want me to be wrong RIGHT?).",
        },
        "birthday.hint.tz": {
            "neutral": "Optional IANA timezone",
            "rin": "optional IANA timezone (like `America/LA`).",
        },
        "birthday.hint.tz_optional": {
            "neutral": "Optional IANA timezone to update",
            "rin": "optional IANA timezone to tweak later.",
        },
        "birthday.hint.user_mod": {
            "neutral": "(Mods only) Set for this user",
            "rin": "(mods) set for this user.",
        },
        "birthday.hint.user_mod_view": {
            "neutral": "(Mods only) View this user's birthday; otherwise shows your own",
            "rin": "(mods) view their birthday; non-mods see their own. fair.",
        },
        "birthday.hint.user_required_or_self": {
            "neutral": "User to edit (mods can edit others; defaults to yourself)",
            "rin": "who to edit (mods can target others; default = you).",
        },
        "birthday.hint.user_optional": {
            "neutral": "User to list (omit to list all — mods only)",
            "rin": "user to list (omit = list all, but only if you're modly moddess).",
        },
        "birthday.hint.closeness_optional": {
            "neutral": "Optional closeness level 1..5 to update",
            "rin": "optional closeness 1..5 (don't pick 6, rebel. only mom gets to be 6.).",
        },
        # Labels
        "birthday.label.closeness": {
            "neutral": "closeness",
            "rin": "closeness",
        },
        "birthday.label.last": {
            "neutral": "last",
            "rin": "last",
        },
        # Parse errors
        "birthday.err.mmdd_format": {
            "neutral": "Use MM-DD format, e.g. 04-13",
            "rin": "format is MM-DD, like `04-13`. not rocket surgery.",
        },
        "birthday.err.mmdd_digits": {
            "neutral": "Month and day must be numbers",
            "rin": "month/day must be digits. letters are cute, but not valid.",
        },
        "birthday.err.mmdd_month": {
            "neutral": "Month must be 1-12",
            "rin": "month 1..12 only. 13 is halloween, not a month.",
        },
        "birthday.err.mmdd_day": {
            "neutral": "Day must be 1-31",
            "rin": "day 1..31. february will judge you.",
        },
        "birthday.err.mmdd_invalid": {
            "neutral": "Invalid calendar date",
            "rin": "that's not a real date. calendar says no.",
        },
        # Permission / resolution
        "birthday.err.perms_other": {
            "neutral": "You don't have permission to act on another user.",
            "rin": "nope—can't edit other people without mod juice.",
        },
        "birthday.err.resolve_self": {
            "neutral": "Couldn't resolve your member identity.",
            "rin": "couldn't figure out who you are. relog? blink twice?",
        },
        "birthday.err.closeness_range": {
            "neutral": "Closeness must be between 1 and 5.",
            "rin": "closeness is 1..5. pick inside the box.",
        },
        # Basic flows
        "birthday.saved": {
            "neutral": "🎂 Saved: **{m}-{d}** (TZ: `{tz}`). I'll DM on the day.",
            "rin": "🎂 saved: **{m}-{d}** (tz `{tz}`). i'll ping you on the day.",
        },
        "birthday.saved.for": {
            "neutral": " (for {user})",
            "rin": " (for {user})",
        },
        "birthday.removed": {
            "neutral": "✅ Removed your birthday entry.",
            "rin": "ok got it. cake perms revoked.",
        },
        "birthday.none_self": {
            "neutral": "No birthday on file. Use `/birthday set`.",
            "rin": "no birthday on file. try `/birthday set` and impress me.",
        },
        "birthday.none_other": {
            "neutral": "{user} doesn't have a birthday set.",
            "rin": "{user} hasn’t set a birthday yet. poke them.",
        },
        "birthday.none_target_first": {
            "neutral": "Set a birthday for that user first with `/birthday set`.",
            "rin": "set their birthday first with `/birthday set`. steps, then cake.",
        },
        # View / list
        "birthday.view.line": {
            "neutral": "🎂 **{m}-{d}** (TZ: `{tz}`){last_part}{close_part}",
            "rin": "🎂 **{m}-{d}** (tz `{tz}`){last_part}{close_part}",
        },
        "birthday.list.title_all": {
            "neutral": "Stored birthdays (this server)",
            "rin": "stored birthdays (this server)",
        },
        "birthday.list.title_user": {
            "neutral": "Stored birthday for {user}",
            "rin": "stored birthday for {user}",
        },
        "birthday.list.empty": {
            "neutral": "No stored birthdays found.",
            "rin": "no birthdays stored. tragic, honestly.",
        },
        "birthday.list.more": {
            "neutral": "…and {count} more.",
            "rin": "…and {count} more. scroll brain engaged.",
        },
        # Edit
        "birthday.edit.ok": {
            "neutral": "✅ Updated.",
            "rin": "updated. neat and tidy.",
        },
        "birthday.edit.noop": {
            "neutral": "No changes provided.",
            "rin": "no changes? i'll sit back down then.",
        },
        # Closeness
        "birthday.closeness.set": {
            "neutral": "✅ Set closeness to **{level}**",
            "rin": "closeness now **{level}**. don’t make it weird.",
        },
        "birthday.closeness.view": {
            "neutral": "Closeness level: **{level}**",
            "rin": "closeness: **{level}**",
        },
        # Voice Stats
        "voice_stats.err.no_log_channel": "Bot log channel is not configured. Use `/admin set_bot_logs` first.",
        "voice_stats.err.bad_log_channel": "Configured bot log channel is not a valid text channel.",
        "voice_stats.err.no_log_perms": "I'm missing 'Read Message History' permission in {channel}. Please grant it and try again.",
        "voice_stats.starting": "Starting voice session backfill. I will scan {channel} for all new voice logs. This may take a while.",
        "voice_stats.no_new_logs": "No new voice logs found to process.",
        "voice_stats.complete": "Backfill complete. Processed {events} new voice events and created/updated {count} sessions.",
        "voice_stats.error": "An error occurred during the backfill: {err}",
    }
)
