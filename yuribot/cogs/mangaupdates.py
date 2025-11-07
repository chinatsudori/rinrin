from __future__ import annotations
# mangaupdates_cog.py

import asyncio
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import discord
from discord import app_commands
from discord.ext import commands, tasks

from ..models import mangaupdates as mu_models
from ..strings import S
from ..ui.mangaupdates import build_batch_embed, build_release_embed
from ..utils.mangaupdates import (
    FIRST_RUN_SEED_ALL,
    FILTER_ENGLISH_ONLY,
    FORUM_TAG_PRIORITY,
    POLL_SECONDS,
    WEAK_MATCH_THRESHOLD,
    MUClient,
    WatchEntry,
    best_match_score,
    fetch_cover_image,
    forum_post_name,
    is_english_release,
    load_state,
    map_mu_to_forum_tags,
    normalize_release_record,
    release_timestamp,
    resolve_mu_forum,
    save_state,
    scrape_mu_tags_and_type,
    seconds_from_any,
    series_id_title_from_result,
    stringify_aliases,
)




class MUWatcher(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = load_state()
        self._session: Optional[aiohttp.ClientSession] = None
        self._task = self.poll_updates.start()

    def cog_unload(self):
        try:
            if self._task:
                self._task.cancel()
        finally:
            if self._session and not self._session.closed:
                asyncio.create_task(self._session.close())

    async def _session_ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers={"User-Agent": "rinrin/1.0 (discord bot)"})
        return self._session

    group = app_commands.Group(name="mu", description="MangaUpdates watcher")

    # -------------------------------------------------------------------------
    # Create a new forum post by searching a MU title (existing behavior)
    # -------------------------------------------------------------------------
    @group.command(
        name="link",
        description="Link a series and create a forum post in the configured MU forum for updates.",
    )
    @app_commands.describe(series="Series name or alias (MangaUpdates)")
    async def link(self, interaction: discord.Interaction, series: str):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        forum = resolve_mu_forum(interaction.guild)
        if not isinstance(forum, discord.ForumChannel):
            return await interaction.followup.send(S("mu.link.forum_missing"), ephemeral=True)

        session = await self._session_ensure()
        client = MUClient(session)

        try:
            results = await client.search_series(series)
        except Exception as e:
            return await interaction.followup.send(S("mu.error.generic", msg=str(e)), ephemeral=True)

        if not results:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        # Score top 5 with aliases
        scored: List[Tuple[dict, float, List[str]]] = []
        for r in results[:5]:
            sid, title = series_id_title_from_result(r)
            if not sid:
                continue
            aliases = []
            score = 0.0
            try:
                full = await client.get_series(sid)
                raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                aliases = stringify_aliases(raw_aliases)
                score = best_match_score(series, title, aliases)
            except Exception:
                score = best_match_score(series, title, [])
            scored.append(({"sid": sid, "title": title}, score, aliases))

        if not scored:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        scored.sort(key=lambda t: t[1], reverse=True)
        choice, best_score, aliases = scored[0]

        # --- Optional polish: ambiguity guard
        if best_score < WEAK_MATCH_THRESHOLD:
            lines = []
            for cand, sc, _als in scored:
                sid = cand["sid"]
                title = cand["title"]
                lines.append(f"- **{title}** — id `{sid}` · score {sc:.2f}")
            msg = (
                "Match is ambiguous. Re-run with an exact MU id using `/mu link` (or use `/mu attach` in a thread):\n"
                + "\n".join(lines)
            )
            return await interaction.followup.send(msg, ephemeral=True)

        sid = choice["sid"]
        title = choice["title"]

        # Already linked?
        gid = str(interaction.guild_id)
        already = None
        for e in self.state.get(gid, {}).get("entries", []):
            if str(e.get("series_id")) == str(sid):
                already = e
                break
        if already:
            t_id = int(already.get("thread_id"))
            return await interaction.followup.send(
                S("mu.link.already_linked", title=title, sid=sid, thread=f"<#{t_id}>"),
                ephemeral=True,
            )

        # Pull full to get cover + tags
        full_json: dict = {}
        try:
            full_json = await client.get_series(sid)
        except Exception:
            full_json = {}

        type_tag, mu_tags = await scrape_mu_tags_and_type(session, sid, full_json)
        forum_tag_names = map_mu_to_forum_tags(mu_tags)

        tag_name_to_obj = {t.name.lower(): t for t in getattr(forum, "available_tags", [])}
        desired_order = []
        if type_tag and type_tag in tag_name_to_obj:
            desired_order.append(type_tag)
        for name in FORUM_TAG_PRIORITY:
            if name in {"manga", "manhwa", "manhua", "webtoon"}:
                continue
            if name in forum_tag_names and name in tag_name_to_obj:
                desired_order.append(name)
        applied = [tag_name_to_obj[n] for n in desired_order[:5]]

        cover_file: Optional[discord.File] = await fetch_cover_image(session, full_json)

        mu_url = full_json.get("url") or full_json.get("series_url") or f"https://www.mangaupdates.com/series.html?id={sid}"
        first_msg = f"Discussion thread for **{title}**\nLink: {mu_url}"
        thread_name = forum_post_name(series)

        try:
            if cover_file:
                created_any = await forum.create_thread(
                    name=thread_name,
                    content=first_msg,
                    file=cover_file,
                    applied_tags=applied or None,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
            else:
                created_any = await forum.create_thread(
                    name=thread_name,
                    content=first_msg,
                    applied_tags=applied or None,
                    allowed_mentions=discord.AllowedMentions.none(),
                )
        except discord.Forbidden:
            return await interaction.followup.send("Forbidden creating a forum post here (check permissions).", ephemeral=True)
        except discord.HTTPException as e:
            return await interaction.followup.send(f"HTTP error creating forum post: {e}", ephemeral=True)

        thread_obj = created_any.thread if hasattr(created_any, "thread") else created_any  # type: ignore

        try:
            mu_models.mu_register_thread_series(interaction.guild_id, thread_obj.id, sid, title)
        except Exception:
            pass  # best-effort

        g = self.state.setdefault(gid, {"entries": []})
        g["entries"] = [e for e in g["entries"] if int(e.get("thread_id")) != thread_obj.id]
        g["entries"].append(
            {
                "series_id": sid,
                "series_title": title,
                "aliases": aliases,
                "forum_channel_id": forum.id,
                "thread_id": thread_obj.id,
                "last_release_id": None,
                "last_release_ts": None,
            }
        )
        save_state(self.state)

        alias_preview = (", ".join(aliases[:8]) + (" …" if len(aliases) > 8 else "")) if aliases else S("mu.link.no_aliases")
        applied_names = ", ".join([t.name for t in applied]) if applied else "none"
        await interaction.followup.send(
            S("mu.link.linked_ok", title=title, sid=sid, thread=thread_obj.name, aliases=alias_preview)
            + "\n→ " + thread_obj.mention + f"\nTags applied: {applied_names}",
            ephemeral=True,
        )

    # -------------------------------------------------------------------------
    # Manual attach: link THIS thread to a MU series (no new post)
    # -------------------------------------------------------------------------
    @group.command(name="attach", description="Link THIS forum thread to a MangaUpdates series (no new post).")
    @app_commands.describe(series="Series id or name. If a name, I’ll search and show choices if ambiguous.")
    async def attach(self, interaction: discord.Interaction, series: str):
        if not interaction.guild or not isinstance(interaction.channel, discord.Thread):
            return await interaction.response.send_message("Run this inside the forum thread you want to attach.", ephemeral=True)

        await interaction.response.defer(ephemeral=True)

        session = await self._session_ensure()
        client = MUClient(session)

        # Accept direct numeric id
        sid = None
        title = "Unknown"

        if series.isdigit():
            sid = series
            try:
                full = await client.get_series(sid)
                title = str(full.get("title") or "Unknown")
            except Exception as e:
                return await interaction.followup.send(f"MU id `{sid}` not found: {e}", ephemeral=True)
        else:
            # Search with ambiguity guard
            try:
                results = await client.search_series(series)
            except Exception as e:
                return await interaction.followup.send(f"Search failed: {e}", ephemeral=True)
            if not results:
                return await interaction.followup.send(f"No MU results for `{series}`.", ephemeral=True)

            scored: List[Tuple[dict, float, List[str]]] = []
            for r in results[:5]:
                rsid, rtitle = series_id_title_from_result(r)
                if not rsid:
                    continue
                aliases = []
                score = 0.0
                try:
                    full = await client.get_series(rsid)
                    raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                    aliases = stringify_aliases(raw_aliases)
                    score = best_match_score(series, rtitle, aliases)
                except Exception:
                    score = best_match_score(series, rtitle, [])
                scored.append(({"sid": rsid, "title": rtitle}, score, aliases))

            scored.sort(key=lambda t: t[1], reverse=True)
            choice, best_score, _ = scored[0]

            if best_score < WEAK_MATCH_THRESHOLD:
                lines = []
                for cand, sc, _als in scored:
                    rsid = cand["sid"]
                    rtitle = cand["title"]
                    lines.append(f"- **{rtitle}** — id `{rsid}` · score {sc:.2f}")
                msg = "Match is ambiguous. Re-run with an exact MU id:\n" + "\n".join(lines)
                return await interaction.followup.send(msg, ephemeral=True)

            sid = choice["sid"]
            title = choice["title"]

        # Save in DB + state
        try:
            mu_models.mu_register_thread_series(interaction.guild_id, interaction.channel.id, sid, title)
        except Exception:
            pass

        gid = str(interaction.guild_id)
        e = {
            "series_id": sid,
            "series_title": title,
            "aliases": [],
            "forum_channel_id": interaction.channel.parent_id or 0,
            "thread_id": interaction.channel.id,
            "last_release_id": None,
            "last_release_ts": None,
        }
        g = self.state.setdefault(gid, {"entries": []})
        g["entries"] = [x for x in g["entries"] if int(x.get("thread_id")) != interaction.channel.id]
        g["entries"].append(e)
        save_state(self.state)

        await interaction.followup.send(f"Linked this thread to **{title}** (MU id {sid}).", ephemeral=True)

    # -------------------------------------------------------------------------
    # Unlink current (or selected) thread
    # -------------------------------------------------------------------------
    @group.command(name="unlink", description="Unlink the current forum post (or selected thread) from MangaUpdates.")
    @app_commands.describe(thread="Forum post to unlink (defaults to current)")
    async def unlink(self, interaction: discord.Interaction, thread: Optional[discord.Thread] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        tgt = thread or (interaction.channel if isinstance(interaction.channel, discord.Thread) else None)
        if not isinstance(tgt, discord.Thread):
            return await interaction.response.send_message(S("mu.unlink.need_thread"), ephemeral=True)

        gid = str(interaction.guild_id)
        g = self.state.setdefault(gid, {"entries": []})
        before = len(g["entries"])
        g["entries"] = [e for e in g["entries"] if int(e.get("thread_id")) != tgt.id]
        save_state(self.state)

        diff = before - len(g["entries"])
        await interaction.response.send_message(S("mu.unlink.done", count=diff), ephemeral=True)

    # -------------------------------------------------------------------------
    # Status
    # -------------------------------------------------------------------------
    @group.command(name="status", description="Show current watches for this server.")
    async def status(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        gid = str(interaction.guild_id)
        g = self.state.get(gid, {}).get("entries", [])
        if not g:
            return await interaction.response.send_message(S("mu.status.none"), ephemeral=True)

        lines = []
        for e in g:
            title = e.get("series_title", "Unknown")
            sid = e.get("series_id")
            tid = int(e.get("thread_id"))
            lines.append(S("mu.status.line", title=discord.utils.escape_markdown(title), sid=sid, thread=f"<#{tid}>"))
        await interaction.response.send_message("\n".join(lines), ephemeral=True)

    # -------------------------------------------------------------------------
    # Export mapping (thread ↔ title ↔ MU URL)
    # -------------------------------------------------------------------------
    @group.command(name="export_links", description="Export CSV of forum threads ↔ MU title ↔ MU link.")
    async def export_links(self, interaction: discord.Interaction):
        if not interaction.guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)

        import csv
        from io import StringIO, BytesIO

        rows = mu_models.mu_list_links_for_guild(interaction.guild_id)
        if not rows:
            return await interaction.response.send_message("No links found.", ephemeral=True)

        buf = StringIO()
        w = csv.writer(buf)
        w.writerow(["guild_id", "thread_id", "thread_name", "series_id", "series_title", "mu_url"])
        for (thread_id, series_id, series_title) in rows:
            th = interaction.guild.get_thread(thread_id) or self.bot.get_channel(thread_id)
            tname = th.name if isinstance(th, discord.Thread) else ""
            url = f"https://www.mangaupdates.com/series.html?id={series_id}"
            w.writerow([interaction.guild_id, thread_id, tname, series_id, series_title, url])

        data = buf.getvalue().encode("utf-8")
        file = discord.File(BytesIO(data), filename=f"mu-links-{interaction.guild_id}.csv")
        await interaction.response.send_message(file=file, ephemeral=True)

    # -------------------------------------------------------------------------
    # Force check for this thread — build DB, post ONLY new updates
    # -------------------------------------------------------------------------
    @group.command(
        name="check",
        description="Force a check for this thread; only post if there are NEW releases.",
    )
    @app_commands.describe(thread="Forum post to check (defaults to the current thread)")
    async def check(self, interaction: discord.Interaction, thread: Optional[discord.Thread] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        tgt = thread or (interaction.channel if isinstance(interaction.channel, discord.Thread) else None)
        if not isinstance(tgt, discord.Thread) or not isinstance(tgt.parent, discord.ForumChannel):
            return await interaction.response.send_message(S("mu.check.need_thread"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        sid = mu_models.mu_get_thread_series(tgt.id, interaction.guild_id)
        we: Optional[WatchEntry] = None

        gid = str(interaction.guild_id)
        entries = self.state.get(gid, {}).get("entries", [])
        for e in entries:
            if int(e.get("thread_id")) == tgt.id:
                we = WatchEntry(
                    series_id=str(e["series_id"]),
                    series_title=e.get("series_title", "Unknown"),
                    aliases=e.get("aliases", []) or [],
                    forum_channel_id=int(e["forum_channel_id"]),
                    thread_id=int(e["thread_id"]),
                    last_release_id=e.get("last_release_id"),
                    last_release_ts=e.get("last_release_ts"),
                )
                break

        if sid is None and we is None:
            return await interaction.followup.send(S("mu.check.not_linked"), ephemeral=True)
        if sid is None and we is not None:
            sid = we.series_id
        if we is None:
            we = WatchEntry(series_id=str(sid), series_title="Unknown", aliases=[], forum_channel_id=tgt.parent.id, thread_id=tgt.id)

        session = await self._session_ensure()
        client = MUClient(session)

        try:
            rels = await client.get_series_releases(sid, page=1, per_page=25)
        except Exception as e:
            return await interaction.followup.send(S("mu.error.generic", msg=str(e)), ephemeral=True)

        results = rels.get("results", []) if isinstance(rels, dict) else rels
        if not results:
            return await interaction.followup.send(S("mu.error.no_releases"), ephemeral=True)

        # Normalize + persist into DB
        results = [normalize_release_record(sid, r) for r in results]
        mu_models.mu_bulk_upsert_releases(sid, results)

        # First-run behavior
        if we.last_release_ts is None and FIRST_RUN_SEED_ALL:
            # mark ALL as posted, set last_release_ts to newest, and bail
            newest_ts = max((release_timestamp(r) for r in results), default=-1)
            for r in results:
                mu_models.mu_mark_posted(interaction.guild_id, tgt.id, sid, int(r.get("release_id")))
            for e in self.state[gid]["entries"]:
                if int(e.get("thread_id")) == we.thread_id:
                    e["last_release_ts"] = newest_ts
                    break
            save_state(self.state)
            return await interaction.followup.send("Indexed existing releases. No new updates.", ephemeral=True)

        # Normal "post only new" path
        unposted = mu_models.mu_list_unposted_for_thread(
            interaction.guild_id, tgt.id, sid, english_only=FILTER_ENGLISH_ONLY
        )

        last_ts = we.last_release_ts if isinstance(we.last_release_ts, int) else -1
        seen: Set[int] = set()
        rels_to_post: List[dict] = []
        for tup in unposted:
            rid = int(tup[0])
            if rid in seen:
                continue
            seen.add(rid)
            rel = mu_models.mu_get_release(sid, rid) or {
                "release_id": rid,
                "title": tup[1], "raw_title": tup[2], "description": tup[3],
                "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                "group": tup[7], "url": tup[8], "release_ts": seconds_from_any(tup[9]),
            }
            rts = int(rel.get("release_ts") or -1)
            if last_ts >= 0 and rts >= 0 and rts <= last_ts:
                continue
            rels_to_post.append(rel)

        if rels_to_post:
            posted = await self._post_batch(tgt, we, rels_to_post)
            if posted > 0:
                max_posted_ts = max(int(r.get("release_ts") or -1) for r in rels_to_post[:posted])
                for r in rels_to_post[:posted]:
                    rid = int(r.get("release_id"))
                    mu_models.mu_mark_posted(interaction.guild_id, tgt.id, sid, rid)
                for e in self.state[gid]["entries"]:
                    if int(e["thread_id"]) == we.thread_id:
                        prev = e.get("last_release_ts") or -1
                        e["last_release_ts"] = int(max(prev, max_posted_ts))
                        break
                save_state(self.state)

            return await interaction.followup.send(S("mu.check.posted", count=posted), ephemeral=True)

        await interaction.followup.send(S("mu.check.no_new"), ephemeral=True)

    # -------------------------------------------------------------------------
    # Repair/index: scan forum, infer titles, store all releases, mark posted, NO messages
    # -------------------------------------------------------------------------
    @group.command(
        name="repair_index",
        description="Scan a forum, infer MU titles from thread names, index all releases WITHOUT posting (DB repair).",
    )
    @app_commands.describe(
        forum="Forum channel to scan (defaults to configured MU forum).",
        limit_threads="Max threads to process (0=all).",
        english_only="Only index English-tagged releases (heuristic).",
        dry_run="If true, do everything except write DB/state."
    )
    @app_commands.checks.has_permissions(manage_guild=True)
    async def repair_index(
        self,
        interaction: discord.Interaction,
        forum: Optional[discord.ForumChannel] = None,
        limit_threads: int = 0,
        english_only: bool = False,
        dry_run: bool = False,
    ):
        if not interaction.guild:
            return await interaction.response.send_message("Guild only.", ephemeral=True)
        await interaction.response.defer(ephemeral=True, thinking=True)

        fch = forum or resolve_mu_forum(interaction.guild)
        if not isinstance(fch, discord.ForumChannel):
            return await interaction.followup.send("MU forum not configured/found.", ephemeral=True)

        # Collect threads: active + archived (public)
        threads: list[discord.Thread] = list(fch.threads)
        try:
            async for th in fch.archived_threads(limit=None, private=False):
                threads.append(th)
        except Exception:
            pass

        # Dedupe
        seen = set()
        uniq = []
        for th in threads:
            if th.id not in seen:
                uniq.append(th)
                seen.add(th.id)
        if limit_threads > 0:
            uniq = uniq[:limit_threads]

        session = await self._session_ensure()
        client = MUClient(session)

        processed = 0
        attached = 0
        indexed = 0
        ambiguous: List[str] = []

        for th in uniq:
            processed += 1
            name = th.name or ""
            # Skip if already mapped
            sid = mu_models.mu_get_thread_series(th.id, interaction.guild_id)
            if sid is None:
                # Infer MU series by title — with ambiguity guard
                try:
                    results = await client.search_series(name)
                except Exception:
                    continue
                if not results:
                    ambiguous.append(f"- `{th.id}` **{name}** — no results")
                    continue

                # score top5
                scored: List[Tuple[dict, float, List[str]]] = []
                for r in results[:5]:
                    rsid, rtitle = series_id_title_from_result(r)
                    if not rsid:
                        continue
                    aliases = []
                    score = 0.0
                    try:
                        full = await client.get_series(rsid)
                        raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                        aliases = stringify_aliases(raw_aliases)
                        score = best_match_score(name, rtitle, aliases)
                    except Exception:
                        score = best_match_score(name, rtitle, [])
                    scored.append(({"sid": rsid, "title": rtitle}, score, aliases))

                scored.sort(key=lambda t: t[1], reverse=True)
                choice, best_score, _ = scored[0]

                if best_score < WEAK_MATCH_THRESHOLD:
                    lines = []
                    for cand, sc, _als in scored:
                        rsid = cand["sid"]
                        rtitle = cand["title"]
                        lines.append(f"    - **{rtitle}** — id `{rsid}` · score {sc:.2f}")
                    ambiguous.append(f"- `{th.id}` **{name}** is ambiguous:\n" + "\n".join(lines))
                    continue

                sid = choice["sid"]
                title = choice["title"]

                if not dry_run:
                    try:
                        full = await client.get_series(sid)
                        title = str(full.get("title") or title or "Unknown")
                    except Exception:
                        pass
                    try:
                        mu_models.mu_register_thread_series(interaction.guild_id, th.id, sid, title)
                        attached += 1
                    except Exception:
                        pass
                else:
                    attached += 1  # would-attach

            # Pull releases and index them — never post
            sid = sid or mu_models.mu_get_thread_series(th.id, interaction.guild_id)
            if not sid:
                continue

            try:
                rels = await client.get_series_releases(sid, page=1, per_page=50)
            except Exception:
                continue

            items = rels.get("results", []) if isinstance(rels, dict) else []
            if english_only:
                items = [r for r in items if is_english_release(r)]
            if not items:
                continue

            if not dry_run:
                mu_models.mu_bulk_upsert_releases(sid, items)
                # Mark ALL as posted in this thread so the watcher won’t flood
                max_ts = max(int(r.get("release_ts") or -1) for r in items)
                for r in items:
                    rid = int(r.get("release_id"))
                    mu_models.mu_mark_posted(interaction.guild_id, th.id, sid, rid)

                # Update state.last_release_ts
                gid = str(interaction.guild_id)
                g = self.state.setdefault(gid, {"entries": []})
                found = False
                for e in g["entries"]:
                    if int(e.get("thread_id")) == th.id:
                        prev = int(e.get("last_release_ts") or -1)
                        e["last_release_ts"] = max(prev, max_ts)
                        found = True
                        break
                if not found:
                    g["entries"].append({
                        "series_id": sid,
                        "series_title": name,
                        "aliases": [],
                        "forum_channel_id": fch.id,
                        "thread_id": th.id,
                        "last_release_id": None,
                        "last_release_ts": max_ts,
                    })
                save_state(self.state)

            indexed += 1

        msg_lines = [
            ("DRY RUN — " if dry_run else "") + f"Processed **{processed}** threads in <#{fch.id}>.",
            f"Attached mappings: **{attached}**.",
            f"Indexed releases (no posts): **{indexed}**.",
        ]
        if ambiguous:
            preview = "\n".join(ambiguous[:10])
            more = len(ambiguous) - 10
            if more > 0:
                preview += f"\n… and {more} more ambiguous threads."
            msg_lines.append("\nAmbiguous or no-result threads:\n" + preview)

        await interaction.followup.send("\n".join(msg_lines), ephemeral=True)

    # -------------------------------------------------------------------------
    # Posting helpers
    # -------------------------------------------------------------------------
    async def _post_release(self, thread: discord.Thread, we: WatchEntry, rel: dict) -> bool:
        if FILTER_ENGLISH_ONLY and not is_english_release(rel):
            return False

        em = build_release_embed(we, rel)
        try:
            await thread.send(embed=em, allowed_mentions=discord.AllowedMentions.none())
            return True
        except Exception:
            return False

    async def _post_batch(self, thread: discord.Thread, we: WatchEntry, rels: List[dict]) -> int:
        items: List[dict] = []
        for r in rels:
            if FILTER_ENGLISH_ONLY and not is_english_release(r):
                continue
            items.append(r)
        if not items:
            return 0

        if len(items) == 1:
            ok = await self._post_release(thread, we, items[0])
            return 1 if ok else 0

        em = build_batch_embed(we, items)
        try:
            await thread.send(embed=em, allowed_mentions=discord.AllowedMentions.none())
            return len(items)
        except Exception:
            return 0

    # -------------------------------------------------------------------------
    # Poll loop — only posts new (first-run obeys FIRST_RUN_SEED_ALL)
    # -------------------------------------------------------------------------
    @tasks.loop(seconds=POLL_SECONDS)
    async def poll_updates(self):
        if not self.bot.is_ready():
            return

        all_entries: List[Tuple[int, str, WatchEntry]] = []
        for gid, blob in list(self.state.items()):
            for e in blob.get("entries", []):
                try:
                    we = WatchEntry(
                        series_id=str(e["series_id"]),
                        series_title=e.get("series_title", "Unknown"),
                        aliases=e.get("aliases", []) or [],
                        forum_channel_id=int(e["forum_channel_id"]),
                        thread_id=int(e["thread_id"]),
                        last_release_id=e.get("last_release_id"),
                        last_release_ts=e.get("last_release_ts"),
                    )
                except Exception:
                    continue
                all_entries.append((int(gid), f"{gid}:{we.thread_id}", we))

        if not all_entries:
            return

        session = await self._session_ensure()
        client = MUClient(session)

        for gid, _key, we in all_entries:
            thread = self.bot.get_channel(we.thread_id)
            if not isinstance(thread, discord.Thread) or not isinstance(getattr(thread, "parent", None), discord.ForumChannel):
                self._prune_entry(gid, we.thread_id)
                continue

            sid = mu_models.mu_get_thread_series(we.thread_id, gid) or we.series_id

            try:
                rels = await client.get_series_releases(sid, page=1, per_page=25)
            except Exception:
                continue

            results = rels.get("results", []) if isinstance(rels, dict) else rels
            if not results:
                continue

            mu_models.mu_bulk_upsert_releases(sid, results)

            if we.last_release_ts is None and FIRST_RUN_SEED_ALL:
                newest_ts = max((release_timestamp(r) for r in results), default=-1)
                for r in results:
                    mu_models.mu_mark_posted(gid, we.thread_id, sid, int(r.get("release_id")))
                try:
                    for ee in self.state[str(gid)]["entries"]:
                        if int(ee["thread_id"]) == we.thread_id:
                            ee["last_release_ts"] = newest_ts
                            break
                    save_state(self.state)
                except Exception:
                    pass
                continue  # skip posting on the first cycle

            unposted = mu_models.mu_list_unposted_for_thread(
                gid, we.thread_id, sid, english_only=FILTER_ENGLISH_ONLY
            )

            last_ts = we.last_release_ts if isinstance(we.last_release_ts, int) else -1
            seen: Set[int] = set()
            rels_to_post: List[dict] = []
            for tup in unposted:
                rid = int(tup[0])
                if rid in seen:
                    continue
                seen.add(rid)
                rel = mu_models.mu_get_release(sid, rid) or {
                    "release_id": rid,
                    "title": tup[1], "raw_title": tup[2], "description": tup[3],
                    "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                    "group": tup[7], "url": tup[8], "release_ts": seconds_from_any(tup[9]),
                }
                rts = int(rel.get("release_ts") or -1)
                if last_ts >= 0 and rts >= 0 and rts <= last_ts:
                    continue
                rels_to_post.append(rel)

            if rels_to_post:
                try:
                    posted = await self._post_batch(thread, we, rels_to_post)
                    if posted > 0:
                        max_posted_ts = max(int(r.get("release_ts") or -1) for r in rels_to_post[:posted])
                        for r in rels_to_post[:posted]:
                            rid = int(r.get("release_id"))
                            mu_models.mu_mark_posted(gid, we.thread_id, sid, rid)
                        for ee in self.state[str(gid)]["entries"]:
                            if int(ee["thread_id"]) == we.thread_id:
                                prev = ee.get("last_release_ts") or -1
                                ee["last_release_ts"] = int(max(prev, max_posted_ts))
                                break
                        save_state(self.state)
                except Exception:
                    pass

    @poll_updates.before_loop
    async def _before_poll(self):
        await self.bot.wait_until_ready()

    def _prune_entry(self, guild_id: int, thread_id: int):
        gid = str(guild_id)
        g = self.state.get(gid)
        if not g:
            return
        g["entries"] = [e for e in g.get("entries", []) if int(e.get("thread_id")) != thread_id]
        save_state(self.state)


async def setup(bot: commands.Bot):
    await bot.add_cog(MUWatcher(bot))