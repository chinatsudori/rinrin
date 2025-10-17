from __future__ import annotations
import asyncio
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands

from ..strings import S

API_BASE = "https://api.mangaupdates.com/v1"
DATA_FILE = Path("./data/mu_watch.json")
POLL_SECONDS = 4 * 60 * 60  # 4 hours


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _strip(s: Optional[str]) -> str:
    return (s or "").strip()


def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s or "").strip().lower()


def _best_match_score(query: str, title: str, aliases: List[str]) -> float:
    q = _norm(query)
    cands = [_norm(title)] + [_norm(a) for a in aliases]
    best = 0.0
    for c in cands:
        if q == c:
            return 1.0
        if c.startswith(q) or q.startswith(c):
            best = max(best, 0.9)
        if q in c or c in q:
            best = max(best, 0.8)
    return best

def _coerce_int(v) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None

def _sid_title_from_result(r: dict) -> Tuple[Optional[int], str]:
    rec = r.get("record") or {}
    sid = (
        r.get("series_id")
        or r.get("id")
        or rec.get("series_id")
        or rec.get("id")
    )
    sid = _coerce_int(sid)
    title = (
        r.get("title")
        or rec.get("title")
        or "Unknown"
    )
    return sid, title

def _stringify_aliases(raw) -> List[str]:
    """Normalize MU alias lists to a de-duplicated list[str]."""
    out: List[str] = []
    for x in (raw or []):
        if isinstance(x, str):
            s = x.strip()
        elif isinstance(x, dict):
            s = (x.get("name") or x.get("title") or x.get("value") or x.get("text") or "").strip()
        else:
            s = ""
        if s:
            out.append(s)
    seen = set()
    uniq: List[str] = []
    for s in out:
        k = s.casefold()
        if k not in seen:
            seen.add(k)
            uniq.append(s)
    return uniq

def _is_english_release(rel: dict) -> bool:

    def _is_en(s: str) -> bool:
        s = (s or "").strip().lower()
        return s in {"english", "en", "eng"}

    for k in ("language", "lang", "lang_name"):
        v = rel.get(k)
        if isinstance(v, str) and _is_en(v):
            return True

    v = rel.get("languages") or rel.get("langs")
    if isinstance(v, list):
        for item in v:
            if isinstance(item, str) and _is_en(item):
                return True
            if isinstance(item, dict):
                if any(_is_en(str(item.get(f, ""))) for f in ("name", "code", "lang", "language")):
                    return True

    grp = rel.get("group") or rel.get("group_name") or {}
    if isinstance(grp, dict):
        if any(_is_en(str(grp.get(f, ""))) for f in ("language", "lang", "name")):
            return True

    return False


def _format_rel_bits(rel: dict) -> Tuple[str, str]:
    vol = _strip(str(rel.get("volume") or ""))
    ch = _strip(str(rel.get("chapter") or ""))
    sub = _strip(str(rel.get("subchapter") or ""))
    bits = []
    if vol:
        bits.append(f"v{vol}")
    if ch:
        bits.append(f"ch {ch}")
    if sub:
        bits.append(sub)
    chbits = " • ".join(bits) if bits else S("mu.release.generic")

    group_raw = rel.get("group") or rel.get("group_name") or ""
    if isinstance(group_raw, dict):
        group = _strip(group_raw.get("name") or group_raw.get("group_name") or "")
    else:
        group = _strip(str(group_raw))

    url = _strip(rel.get("url") or rel.get("release_url") or "")
    extras = []
    if group:
        extras.append(S("mu.release.group", group=discord.utils.escape_markdown(group)))

    rdate = rel.get("release_date") or rel.get("date")
    if rdate:
        try:
            dt = datetime.fromisoformat(str(rdate).replace("Z", "+00:00"))
            extras.append(S("mu.release.date_rel", ts=int(dt.timestamp())))
        except Exception:
            extras.append(S("mu.release.date_raw", date=str(rdate)))

    if url:
        extras.append(url)

    return chbits, "\n".join(extras) if extras else ""


class MUClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session

    async def search_series(self, term: str) -> List[dict]:
        url = f"{API_BASE}/series/search"
        payload = {"search": term}
        async with self.session.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                raise RuntimeError(S("mu.error.search_http", code=resp.status))
            data = await resp.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        return results or []

    async def get_series(self, series_id: int) -> dict:
        url = f"{API_BASE}/series/{series_id}"
        async with self.session.get(url, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                raise RuntimeError(S("mu.error.series_http", sid=series_id, code=resp.status))
            return await resp.json()

    async def get_releases_for_series(self, series_id: int, page: int = 1, per_page: int = 50) -> dict:
        url = f"{API_BASE}/series/{series_id}/releases"
        params = {"page": page, "per_page": per_page}
        async with self.session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=20)) as resp:
            if resp.status != 200:
                raise RuntimeError(S("mu.error.releases_http", sid=series_id, code=resp.status))
            return await resp.json()


@dataclass
class WatchEntry:
    series_id: int
    series_title: str
    aliases: List[str]
    forum_channel_id: int
    thread_id: int
    last_release_id: Optional[int] = None


def _load_state() -> Dict[str, dict]:
    if DATA_FILE.exists():
        try:
            with DATA_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def _save_state(state: Dict[str, dict]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as f:
        json.dump(state, f, indent=2)


class MUWatcher(commands.Cog):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = _load_state()
        self._session: Optional[aiohttp.ClientSession] = None
        self._task = self.poll_updates.start()

    def cog_unload(self):
        if self._task:
            self._task.cancel()
        # close session cleanly
        if self._session and not self._session.closed:
            asyncio.create_task(self._session.close())

    async def _session_ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession()
        return self._session

    group = app_commands.Group(name="mu", description="MangaUpdates watcher")

    @group.command(
        name="link",
        description="Link this forum post (or a target thread) to a series and start posting new releases.",
    )
    @app_commands.describe(
        series="Series name or alias (MangaUpdates)",
        thread="Target forum post (use if you aren't running this inside the post)",
    )
    async def link(
        self,
        interaction: discord.Interaction,
        series: str,
        thread: Optional[discord.Thread] = None,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        target_thread: Optional[discord.Thread] = thread
        if target_thread is None and isinstance(interaction.channel, discord.Thread):
            target_thread = interaction.channel

        if not isinstance(target_thread, discord.Thread) or not isinstance(target_thread.parent, discord.ForumChannel):
            return await interaction.response.send_message(S("mu.link.need_forum"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        session = await self._session_ensure()
        client = MUClient(session)

        results = await client.search_series(series)
        if not results:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        top = results[:5]
        scored: List[Tuple[dict, float, List[str]]] = []
        for r in top:
            sid, title = _sid_title_from_result(r)
            if sid is None:
                continue
            aliases: List[str] = []
            try:
                full = await client.get_series(int(sid))
                raw_aliases = (
                    full.get("associated_names")
                    or full.get("associated")
                    or full.get("associated_names_ascii")
                    or []
                )
                aliases = _stringify_aliases(raw_aliases)
                score = _best_match_score(series, title, aliases)

            except Exception:
                score = _best_match_score(series, title, [])
            scored.append(({"sid": sid, "title": title}, score, aliases))

        if not scored:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        scored.sort(key=lambda t: t[1], reverse=True)
        choice, score, aliases = scored[0]
        sid = int(choice["sid"])
        title = choice["title"]

        gid = str(interaction.guild_id)
        g = self.state.setdefault(gid, {"entries": []})
        g["entries"] = [e for e in g["entries"] if int(e.get("thread_id")) != target_thread.id]
        g["entries"].append(
            {
                "series_id": sid,
                "series_title": title,
                "aliases": aliases,
                "forum_channel_id": target_thread.parent.id,
                "thread_id": target_thread.id,
                "last_release_id": None,
            }
        )
        _save_state(self.state)

        alias_preview = (", ".join(aliases[:8]) + (" …" if len(aliases) > 8 else "")) if aliases else S("mu.link.no_aliases")
        await interaction.followup.send(
            S("mu.link.linked_ok", title=title, sid=sid, thread=target_thread.name, aliases=alias_preview),
            ephemeral=True,
        )

    @group.command(name="unlink", description="Unlink the current forum post (or selected thread) from MangaUpdates.")
    @app_commands.describe(thread="Forum post to unlink (defaults to current)")
    async def unlink(self, interaction: discord.Interaction, thread: Optional[discord.Thread] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        tgt = thread
        if tgt is None and isinstance(interaction.channel, discord.Thread):
            tgt = interaction.channel

        if not isinstance(tgt, discord.Thread):
            return await interaction.response.send_message(S("mu.unlink.need_thread"), ephemeral=True)

        gid = str(interaction.guild_id)
        g = self.state.setdefault(gid, {"entries": []})
        before = len(g["entries"])
        g["entries"] = [e for e in g["entries"] if int(e.get("thread_id")) != tgt.id]
        _save_state(self.state)

        diff = before - len(g["entries"])
        await interaction.response.send_message(S("mu.unlink.done", count=diff), ephemeral=True)

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

    @group.command(
        name="check",
        description="Force a check for this thread; if no update, post the latest known chapter.",
    )
    @app_commands.describe(thread="Forum post to check (defaults to the current thread)")
    async def check(self, interaction: discord.Interaction, thread: Optional[discord.Thread] = None):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        tgt = thread
        if tgt is None and isinstance(interaction.channel, discord.Thread):
            tgt = interaction.channel

        if not isinstance(tgt, discord.Thread) or not isinstance(tgt.parent, discord.ForumChannel):
            return await interaction.response.send_message(S("mu.check.need_thread"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        gid = str(interaction.guild_id)
        entries = self.state.get(gid, {}).get("entries", [])
        entry = None
        for e in entries:
            if int(e.get("thread_id")) == tgt.id:
                entry = e
                break
        if not entry:
            return await interaction.followup.send(S("mu.check.not_linked"), ephemeral=True)

        we = WatchEntry(
            series_id=int(entry["series_id"]),
            series_title=entry.get("series_title", "Unknown"),
            aliases=entry.get("aliases", []) or [],
            forum_channel_id=int(entry["forum_channel_id"]),
            thread_id=int(entry["thread_id"]),
            last_release_id=entry.get("last_release_id"),
        )

        session = await self._session_ensure()
        client = MUClient(session)
        try:
            rels = await client.get_releases_for_series(we.series_id, page=1, per_page=25)
        except Exception as e:
            return await interaction.followup.send(S("mu.error.generic", msg=str(e)), ephemeral=True)

        results = rels.get("results", []) if isinstance(rels, dict) else rels
        if not results:
            return await interaction.followup.send(S("mu.error.no_releases"), ephemeral=True)

        # English-only
        results_en = [r for r in results if _is_english_release(r)]
        if not results_en:
            return await interaction.followup.send(S("mu.error.no_releases_lang"), ephemeral=True)

        def _rid(x) -> Optional[int]:
            v = x.get("id") or x.get("release_id")
            try:
                return int(v)
            except Exception:
                return None

        top_new = []
        top_seen = we.last_release_id or 0
        for r in results_en:
            rid = _rid(r)
            if rid is None:
                continue
            top_seen = max(top_seen, rid)
            if we.last_release_id is None or rid > (we.last_release_id or 0):
                top_new.append(r)

        if top_new:
            for r in sorted(top_new, key=lambda x: _rid(x) or 0):
                await self._post_release(tgt, we, r)

            for e in self.state[gid]["entries"]:
                if int(e.get("thread_id")) == we.thread_id:
                    e["last_release_id"] = top_seen
                    break
            _save_state(self.state)
            return await interaction.followup.send(S("mu.check.posted", count=len(top_new)), ephemeral=True)

        latest = max(results_en, key=lambda x: _rid(x) or 0)
        chbits, extras = _format_rel_bits(latest)
        embed = discord.Embed(
            title=S("mu.latest.title", series=we.series_title, chbits=chbits),
            description=extras or None,
            color=discord.Color.dark_gray(),
            timestamp=_now_utc(),
        )
        embed.set_footer(text=S("mu.latest.footer"))
        try:
            await tgt.send(embed=embed)
        except Exception:
            pass

        if we.last_release_id is None:
            for e in self.state[gid]["entries"]:
                if int(e.get("thread_id")) == we.thread_id:
                    e["last_release_id"] = _rid(latest)
                    break
            _save_state(self.state)

        await interaction.followup.send(S("mu.check.no_new"), ephemeral=True)

    @tasks.loop(seconds=POLL_SECONDS)
    async def poll_updates(self):
        if not self.bot.is_ready():
            return

        all_entries: List[Tuple[int, str, WatchEntry]] = []
        for gid, blob in list(self.state.items()):
            for e in blob.get("entries", []):
                try:
                    we = WatchEntry(
                        series_id=int(e["series_id"]),
                        series_title=e.get("series_title", "Unknown"),
                        aliases=e.get("aliases", []) or [],
                        forum_channel_id=int(e["forum_channel_id"]),
                        thread_id=int(e["thread_id"]),
                        last_release_id=e.get("last_release_id"),
                    )
                except Exception:
                    continue
                all_entries.append((int(gid), f"{gid}:{we.thread_id}", we))

        if not all_entries:
            return

        session = await self._session_ensure()
        client = MUClient(session)

        for gid, key, we in all_entries:
            thread = self.bot.get_channel(we.thread_id)
            if not isinstance(thread, discord.Thread) or not isinstance(getattr(thread, "parent", None), discord.ForumChannel):
                self._prune_entry(gid, we.thread_id)
                continue

            try:
                rels = await client.get_releases_for_series(we.series_id, page=1, per_page=25)
            except Exception:
                continue

            results = rels.get("results", []) if isinstance(rels, dict) else rels
            if not results:
                continue

            raw_results = rels.get("results", []) if isinstance(rels, dict) else rels
            results_en = [r for r in raw_results or [] if _is_english_release(r)]
            if not results_en:
                continue


            def _rid(x) -> Optional[int]:
                v = x.get("id") or x.get("release_id")
                try:
                    return int(v)
                except Exception:
                    return None

            new_items = []
            top_seen = we.last_release_id or 0
            for r in results_en:
                rid = _rid(r)
                if rid is None:
                    continue
                top_seen = max(top_seen, rid)
                if we.last_release_id is None or rid > we.last_release_id:
                    new_items.append(r)

            gid_str = str(gid)
            guild_blob = self.state.setdefault(gid_str, {"entries": []})

            if we.last_release_id is None:
                for ee in guild_blob["entries"]:
                    if int(ee["thread_id"]) == we.thread_id:
                        ee["last_release_id"] = top_seen
                        break
                _save_state(self.state)
                continue

            for r in sorted(new_items, key=lambda x: _rid(x) or 0):
                try:
                    await self._post_release(thread, we, r)
                except Exception:
                    pass

            for ee in guild_blob["entries"]:
                if int(ee["thread_id"]) == we.thread_id:
                    ee["last_release_id"] = top_seen
                    break
            _save_state(self.state)

    @poll_updates.before_loop
    async def _before_poll(self):
        await self.bot.wait_until_ready()

    def _prune_entry(self, guild_id: int, thread_id: int):
        gid = str(guild_id)
        g = self.state.get(gid)
        if not g:
            return
        g["entries"] = [e for e in g.get("entries", []) if int(e.get("thread_id")) != thread_id]
        _save_state(self.state)

    async def _post_release(self, thread: discord.Thread, we: WatchEntry, rel: dict):
        rid = rel.get("id") or rel.get("release_id")
        chbits, extras = _format_rel_bits(rel)

        embed = discord.Embed(
            title=S("mu.release.title", series=we.series_title, chbits=chbits),
            description=extras or None,
            color=discord.Color.blurple(),
            timestamp=_now_utc(),
        )
        embed.set_footer(text=S("mu.release.footer", rid=rid))
        await thread.send(embed=embed)


async def setup(bot: commands.Bot):
    await bot.add_cog(MUWatcher(bot))
