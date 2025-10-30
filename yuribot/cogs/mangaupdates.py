# mangaupdates.py
from __future__ import annotations

import asyncio
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Set

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands

from .. import models
from ..strings import S

API_BASE = "https://api.mangaupdates.com/v1"
DATA_FILE = Path("./data/mu_watch.json")
POLL_SECONDS = 4 * 60 * 60  # 4 hours

# Turn EN filter on/off (heuristic)
FILTER_ENGLISH_ONLY = False

# Max bytes we'll attempt to upload for a cover image (8 MiB is Discord's base limit)
MAX_COVER_BYTES = 7_500_000


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


def _sid_title_from_result(r: dict) -> Tuple[Optional[str], str]:
    rec = r.get("record") or {}
    sid = r.get("series_id") or r.get("id") or rec.get("series_id") or rec.get("id")
    sid = str(sid) if sid is not None else None
    title = r.get("title") or rec.get("title") or "Unknown"
    return sid, title


def _stringify_aliases(raw) -> List[str]:
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


def _forum_post_name(s: str) -> str:
    name = re.sub(r"\s+", " ", (s or "").strip())
    if not name:
        name = "Untitled"
    return name[:100]


_CH_PATTERNS = [
    r"(?:\bch(?:apter)?|\bc)\.?\s*(\d+(?:\.\d+)?)",      # ch 25 / c25 / ch.25
    r"\b(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)",     # 21-25 / 21–25
]
_VOL_PATTERNS = [
    r"(?:\bvol(?:ume)?|\bv)\.?\s*(\d+(?:\.\d+)?)",      # v5 / vol 5
]

def _find_all_numbers(text: str, patterns: List[str]) -> List[float]:
    nums: List[float] = []
    for pat in patterns:
        for m in re.finditer(pat, text, flags=re.I):
            for g in m.groups():
                if not g:
                    continue
                try:
                    nums.append(float(g))
                except Exception:
                    pass
    return nums

def _extract_max_chapter(text: str) -> Tuple[str, str]:
    nums = _find_all_numbers(text, _CH_PATTERNS)
    if not nums:
        return "", ""
    mx = max(nums)
    s = f"{mx}".rstrip("0").rstrip(".")
    if "." in s:
        ch, sub = s.split(".", 1)
        return ch, sub
    return s, ""

def _extract_max_volume(text: str) -> str:
    nums = _find_all_numbers(text, _VOL_PATTERNS)
    if not nums:
        return ""
    mx = max(nums)
    return f"{mx}".rstrip("0").rstrip(".")


def _parse_ts(value: Optional[str]) -> Optional[int]:
    """Parse an ISO/RFC date string to epoch seconds; return None if unknown."""
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return int(dt.timestamp())
    except Exception:
        pass
    try:
        import email.utils as eut
        dt = eut.parsedate_to_datetime(str(value))
        return int(dt.timestamp())
    except Exception:
        return None


def _release_ts(rel: dict) -> int:
    v = rel.get("release_ts")
    if isinstance(v, (int, float)):
        return int(v)
    for key in ("release_date", "date", "pubDate", "pubdate"):
        if key in rel and rel[key]:
            ts = _parse_ts(rel[key])
            if ts is not None:
                return ts
    return -1


def _has_ch(x) -> int:
    if _strip(str(x.get("chapter") or "")):
        return 1
    title = f"{x.get('title','')} {x.get('raw_title','')} {x.get('description','')}".lower()
    return 1 if re.search(r"(?:\b(?:c|ch(?:apter)?)\.?\s*\d+(?:\.\d+)?)|(\b\d+(?:\.\d+)?\s*[-–—]\s*\d+(?:\.\d+)?\b)", title) else 0


def _format_rel_bits(rel: dict) -> Tuple[str, str]:
    vol = _strip(str(rel.get("volume") or ""))
    ch = _strip(str(rel.get("chapter") or ""))
    sub = _strip(str(rel.get("subchapter") or ""))

    if not (vol or ch):
        title_str = " ".join([
            str(rel.get("title", "")),
            str(rel.get("raw_title", "")),
            str(rel.get("description", "")),
        ])
        tl = title_str.lower()
        if not vol:
            vol = _extract_max_volume(tl)
        if not ch:
            ch, sub = _extract_max_chapter(tl)

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

    url = _strip(rel.get("url") or rel.get("release_url") or rel.get("link") or "")

    extras = []
    if group:
        extras.append(S("mu.release.group", group=discord.utils.escape_markdown(group)))

    rdate = rel.get("release_date") or rel.get("date") or rel.get("pubDate") or rel.get("pubdate")
    if rdate:
        try:
            dt = datetime.fromisoformat(str(rdate).replace("Z", "+00:00"))
            extras.append(S("mu.release.date_rel", ts=int(dt.timestamp())))
        except Exception:
            extras.append(S("mu.release.date_raw", date=str(rdate)))

    if url:
        extras.append(url)

    return chbits, "\n".join(extras) if extras else ""


# --- MU tags → Forum tags mapping helpers ------------------------------------

_MU_CANON_TAGS = [
    # Demographic
    "josei", "lolicon", "seinen", "shotacon", "shoujo", "shoujo ai", "shounen",
    "shounen ai", "yaoi", "yuri",
    # Genre
    "action", "adult", "adventure", "comedy", "doujinshi", "drama", "ecchi",
    "fantasy", "gender bender", "harem", "hentai", "historical", "horror",
    "martial arts", "mature", "mecha", "mystery", "psychological", "romance",
    "school life", "sci-fi", "slice of life", "smut", "sports", "supernatural",
    "tragedy", "isekai",
]

_FORUM_TAG_PRIORITY = [
    "manga", "manhwa", "manhua", "webtoon",
    "fantasy", "slice of life", "drama", "sci-fi", "mystery", "horror",
    "tragedy", "comedy", "isekai", "harem", "ecchi",
    "psychological", "violence/gore", "historical", "nsfw",
]

def _map_mu_to_forum(mu_tags: Set[str]) -> Set[str]:
    mt = {t.lower() for t in mu_tags}
    out: Set[str] = set()

    if "hentai" in mt:
        out.add("nsfw")
    if {"slice of life", "school life"} & mt:
        out.add("slice of life")
    if "psychological" in mt:
        out.add("psychological")
    if {"action", "martial arts", "mecha"} & mt:
        out.add("violence/gore")
    if "historical" in mt:
        out.add("historical")

    keep_map = {
        "fantasy": "fantasy",
        "drama": "drama",
        "sci-fi": "sci-fi",
        "mystery": "mystery",
        "horror": "horror",
        "tragedy": "tragedy",
        "comedy": "comedy",
        "isekai": "isekai",
        "harem": "harem",
        "ecchi": "ecchi",
    }
    for mu, forum_tag in keep_map.items():
        if mu in mt:
            out.add(forum_tag)

    return out


async def _scrape_mu_tags_and_type(
    session: aiohttp.ClientSession,
    sid: str,
    series_json: dict | None,
) -> Tuple[Optional[str], Set[str]]:
    mu_tags: Set[str] = set()
    type_tag: Optional[str] = None

    sj = series_json or {}
    for key in ("genres", "genre", "categories", "tags", "themes"):
        raw = sj.get(key)
        if isinstance(raw, list):
            for v in raw:
                if isinstance(v, str):
                    mu_tags.add(v.strip().lower())
                elif isinstance(v, dict):
                    name = (v.get("name") or v.get("title") or v.get("value") or "").strip()
                    if name:
                        mu_tags.add(name.lower())

    for key in ("type", "format", "media_type"):
        val = str(sj.get(key) or "").strip().lower()
        if val:
            if "manhwa" in val:
                type_tag = "manhwa"
            elif "manhua" in val:
                type_tag = "manhua"
            elif "webtoon" in val:
                type_tag = "webtoon"
            elif "manga" in val:
                type_tag = "manga"

    if not type_tag or not mu_tags:
        urls = [
            f"https://www.mangaupdates.com/series/{sid}",
            f"https://www.mangaupdates.com/series.html?id={sid}",
        ]
        page_text = ""
        for url in urls:
            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                    if resp.status == 200:
                        page_text = await resp.text()
                        if page_text:
                            break
            except Exception:
                continue

        text = re.sub(r"<[^>]+>", " ", page_text)
        text = re.sub(r"\s+", " ", text).lower()

        if not type_tag:
            if "manhwa" in text:
                type_tag = "manhwa"
            elif "manhua" in text:
                type_tag = "manhua"
            elif "webtoon" in text:
                type_tag = "webtoon"
            elif "manga" in text:
                type_tag = "manga"

        if not mu_tags:
            for t in _MU_CANON_TAGS:
                if t in text:
                    mu_tags.add(t)

    return type_tag, mu_tags


class MUClient:
    def __init__(self, session: aiohttp.ClientSession):
        self.session = session
        self._headers = {
            "Accept": "application/json, text/plain, */*",
            "User-Agent": "rinrin/1.0 (+discord bot; contact: you@example.com)",
        }
        self._corsish = {
            "Origin": "https://www.mangaupdates.com",
            "Referer": "https://www.mangaupdates.com/",
            "Content-Type": "application/json;charset=utf-8",
        }

    async def search_series(self, term: str) -> List[dict]:
        url = f"{API_BASE}/series/search"
        payload = {"search": term}
        async with self.session.post(
            url, json=payload,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={**self._headers, **self._corsish},
        ) as resp:
            if resp.status != 200:
                txt = (await resp.text())[:200]
                raise RuntimeError(S("mu.error.search_http", code=resp.status) + (f" ({txt})" if txt else ""))
            data = await resp.json()
        results = data.get("results", data) if isinstance(data, dict) else data
        return results or []

    async def get_series(self, series_id: str) -> dict:
        url = f"{API_BASE}/series/{series_id}"
        async with self.session.get(
            url,
            timeout=aiohttp.ClientTimeout(total=20),
            headers=self._headers,
        ) as resp:
            if resp.status != 200:
                txt = (await resp.text())[:200]
                raise RuntimeError(S("mu.error.series_http", sid=series_id, code=resp.status) + (f" ({txt})" if txt else ""))
            return await resp.json()

    async def get_series_releases(self, series_id: str, page: int = 1, per_page: int = 50) -> dict:
        """
        Try JSON endpoint; on failure, fall back to RSS and convert to JSON-like.
        """
        timeout = aiohttp.ClientTimeout(total=25)
        get_url = f"{API_BASE}/series/{series_id}/releases"
        params = {"page": page, "per_page": per_page}

        for i in range(1, 4):
            try:
                async with self.session.get(
                    get_url,
                    params=params,
                    timeout=timeout,
                    headers=self._headers,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        for r in data.get("results", []) if isinstance(data, dict) else []:
                            ts = _parse_ts(r.get("release_date") or r.get("date"))
                            if ts is not None:
                                r["release_ts"] = ts
                        return data
                    if resp.status in (429, 500, 502, 503, 504):
                        await asyncio.sleep(0.8 * i)
                        continue
                    break
            except asyncio.TimeoutError:
                if i < 3:
                    await asyncio.sleep(0.8 * i)
                    continue
                break

        return await self.get_series_releases_via_rss(series_id, limit=per_page)

    async def get_series_releases_via_rss(self, series_id: str | int, *, limit: int = 50) -> dict:
        url = f"{API_BASE}/series/{series_id}/rss"
        timeout = aiohttp.ClientTimeout(total=20)

        async with self.session.get(
            url,
            timeout=timeout,
            headers={
                **self._headers,
                "Accept": "application/rss+xml, application/xml;q=0.9, */*;q=0.8",
            },
        ) as resp:
            if resp.status != 200:
                txt = (await resp.text())[:300]
                raise RuntimeError(S("mu.error.releases_http", sid=str(series_id), code=resp.status) + (f" ({txt})" if txt else ""))

            xml_text = await resp.text()

        import email.utils as eut
        import xml.etree.ElementTree as ET

        try:
            root = ET.fromstring(xml_text)
        except Exception:
            xml_text_clean = xml_text.lstrip("\ufeff").strip()
            root = ET.fromstring(xml_text_clean)

        channel = None
        if root.tag.lower().endswith("rss"):
            channel = next((c for c in root if c.tag.lower().endswith("channel")), None)
        elif root.tag.lower().endswith("channel"):
            channel = root
        else:
            for c in root.iter():
                if c.tag.lower().endswith("channel"):
                    channel = c
                    break

        if channel is None:
            return {"results": []}

        items = [i for i in channel if i.tag.lower().endswith("item")]
        releases = []

        def _text(x, tag):
            n = next((c for c in x if c.tag.lower().endswith(tag)), None)
            return (n.text or "").strip() if n is not None and n.text else ""

        for it in items[:limit]:
            title = _text(it, "title")
            link = _text(it, "link")
            desc = _text(it, "description")
            pub = _text(it, "pubdate") or _text(it, "pubDate")

            ts = None
            try:
                dt = eut.parsedate_to_datetime(pub) if pub else None
                if dt is not None:
                    ts = int(dt.timestamp())
            except Exception:
                ts = None

            raw = f"{title} {desc}".strip()

            chapter, subchapter = _extract_max_chapter(raw)
            volume = _extract_max_volume(raw)

            m_group = re.search(r"\[(.*?)\]", title) or re.search(r"\[(.*?)\]", desc)
            group = (m_group.group(1).strip() if m_group else "")

            rid = None
            if link:
                m_id_in_link = re.search(r"(\d{6,})", link)
                if m_id_in_link:
                    rid = int(m_id_in_link.group(1))
            if rid is None:
                # fallback id: stable-ish hash on (title, link)
                rid = ts or int(abs(hash((title, link))) % 10_000_000_000)

            releases.append({
                "id": rid,
                "release_id": rid,
                "chapter": chapter or "",
                "volume": volume or "",
                "subchapter": subchapter or "",
                "group": group or "",
                "url": link or "",
                "release_date": (datetime.utcfromtimestamp(ts).isoformat() + "Z") if ts else "",
                "release_ts": ts if ts is not None else -1,
                "title": title or "",
                "raw_title": title or "",
                "description": desc or "",
                "lang_hint": raw.lower(),
            })

        return {"results": releases}


@dataclass
class WatchEntry:
    series_id: str
    series_title: str
    aliases: List[str]
    forum_channel_id: int
    thread_id: int
    last_release_id: Optional[int] = None      # kept for backward compat (unused for logic now)
    last_release_ts: Optional[int] = None      # used only to decide baseline init


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


def _is_english_release(rel: dict) -> bool:
    txt = f"{rel.get('title','')} {rel.get('raw_title','')} {rel.get('description','')} {rel.get('lang_hint','')}".lower()
    return any(k in txt for k in ("eng", "english", "[en]", "(en)"))


def _resolve_mu_forum(guild: discord.Guild) -> Optional[discord.ForumChannel]:
    ch_id = models.get_mu_forum_channel(guild.id)
    if ch_id:
        ch = guild.get_channel(ch_id)
        if isinstance(ch, discord.ForumChannel):
            return ch
    lname = "ongoing-reading-room"
    for ch in guild.channels:
        if isinstance(ch, discord.ForumChannel) and ch.name.lower().strip() == lname:
            return ch
    return None


async def _fetch_cover_image(session: aiohttp.ClientSession, series_json: dict) -> Optional[discord.File]:
    urls: List[str] = []
    for key in ("cover", "image", "image_url", "thumbnail", "thumbnail_url"):
        val = series_json.get(key)
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            urls.append(val)
        elif isinstance(val, dict):
            for v in val.values():
                if isinstance(v, str) and v.startswith(("http://", "https://")):
                    urls.append(v)

    seen = set()
    urls = [u for u in urls if not (u in seen or seen.add(u))]

    for url in urls:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    continue
                ctype = resp.headers.get("Content-Type", "").lower()
                if not any(x in ctype for x in ("image/", "jpeg", "png", "webp")):
                    continue
                raw = await resp.read()
                if len(raw) > MAX_COVER_BYTES:
                    continue
                ext = ".png"
                if "jpeg" in ctype or "jpg" in ctype:
                    ext = ".jpg"
                elif "webp" in ctype:
                    ext = ".webp"
                filename = f"cover{ext}"
                return discord.File(io.BytesIO(raw), filename=filename)
        except Exception:
            continue
    return None


class MUWatcher(commands.Cog):
    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.state = _load_state()
        self._session: Optional[aiohttp.ClientSession] = None
        self._task = self.poll_updates.start()

    def cog_unload(self):
        if self._task:
            self._task.cancel()
        if self._session and not self._session.closed:
            asyncio.create_task(self._session.close())

    async def _session_ensure(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(headers={"User-Agent": "rinrin/1.0 (discord bot)"})
        return self._session

    group = app_commands.Group(name="mu", description="MangaUpdates watcher")

    @group.command(
        name="link",
        description="Link a series and create a forum post in the configured MU forum for updates.",
    )
    @app_commands.describe(
        series="Series name or alias (MangaUpdates)",
    )
    async def link(
        self,
        interaction: discord.Interaction,
        series: str,
    ):
        if not interaction.guild:
            return await interaction.response.send_message(S("common.guild_only"), ephemeral=True)

        await interaction.response.defer(ephemeral=True, thinking=True)

        forum = _resolve_mu_forum(interaction.guild)
        if not isinstance(forum, discord.ForumChannel):
            return await interaction.followup.send(S("mu.link.forum_missing"), ephemeral=True)

        session = await self._session_ensure()
        client = MUClient(session)

        # Search & pick best match
        try:
            results = await client.search_series(series)
        except Exception as e:
            return await interaction.followup.send(S("mu.error.generic", msg=str(e)), ephemeral=True)

        if not results:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        top = results[:5]
        scored: List[Tuple[dict, float, List[str]]] = []
        for r in top:
            sid, title = _sid_title_from_result(r)
            if not sid:
                continue
            aliases = []
            score = 0.0
            try:
                full = await client.get_series(sid)
                raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                aliases = _stringify_aliases(raw_aliases)
                score = _best_match_score(series, title, aliases)
            except Exception:
                score = _best_match_score(series, title, [])
            scored.append(({"sid": sid, "title": title}, score, aliases))

        if not scored:
            return await interaction.followup.send(S("mu.link.no_results", q=series), ephemeral=True)

        scored.sort(key=lambda t: t[1], reverse=True)
        choice, score, aliases = scored[0]
        sid = choice["sid"]
        title = choice["title"]

        # Duplicate protection via JSON state (per guild)
        gid = str(interaction.guild_id)
        already = None
        for e in self.state.get(gid, {}).get("entries", []):
            if str(e.get("series_id")) == str(sid):
                already = e
                break
        if already:
            t_id = int(already.get("thread_id"))
            mention = f"<#{t_id}>"
            return await interaction.followup.send(
                S("mu.link.already_linked", title=title, sid=sid, thread=mention),
                ephemeral=True,
            )

        # Fetch full series for cover and tags
        full_json: dict = {}
        try:
            full_json = await client.get_series(sid)
        except Exception:
            full_json = {}

        type_tag, mu_tags = await _scrape_mu_tags_and_type(session, sid, full_json)
        forum_tag_names = _map_mu_to_forum(mu_tags)

        tag_name_to_obj = {t.name.lower(): t for t in getattr(forum, "available_tags", [])}
        desired_order = []
        if type_tag and type_tag in tag_name_to_obj:
            desired_order.append(type_tag)
        for name in _FORUM_TAG_PRIORITY:
            if name in {"manga", "manhwa", "manhua", "webtoon"}:
                continue
            if name in forum_tag_names and name in tag_name_to_obj:
                desired_order.append(name)
        applied = [tag_name_to_obj[n] for n in desired_order[:5]]

        cover_file: Optional[discord.File] = await _fetch_cover_image(session, full_json)

        mu_url = full_json.get("url") or full_json.get("series_url") or f"https://www.mangaupdates.com/series.html?id={sid}"
        first_msg = f"Discussion thread for **{title}**\nLink: {mu_url}"
        thread_name = _forum_post_name(series)

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

        if hasattr(created_any, "thread"):
            thread_obj = created_any.thread  # type: ignore[attr-defined]
        else:
            thread_obj = created_any  # type: ignore[assignment]

        # DB: map thread <-> series (title upsert)
        try:
            models.mu_register_thread_series(interaction.guild_id, thread_obj.id, sid, title)
        except Exception:
            pass  # best-effort; state file still preserves mapping for watcher

        # Persist JSON watch mapping (aliases/title/last_ts baseline)
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
        _save_state(self.state)

        alias_preview = (", ".join(aliases[:8]) + (" …" if len(aliases) > 8 else "")) if aliases else S("mu.link.no_aliases")
        applied_names = ", ".join([t.name for t in applied]) if applied else "none"
        await interaction.followup.send(
            S("mu.link.linked_ok", title=title, sid=sid, thread=thread_obj.name, aliases=alias_preview)
            + f"\n→ {thread_obj.mention}\nTags applied: {applied_names}",
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

        # Resolve series id (prefer DB mapping, fallback to JSON state)
        sid = models.mu_get_thread_series(tgt.id, interaction.guild_id)
        we: Optional[WatchEntry] = None

        gid = str(interaction.guild_id)
        entries = self.state.get(gid, {}).get("entries", [])

        # Try match JSON entry for title/aliases + last_ts baseline
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
            sid = we.series_id  # fallback to state
        if we is None:
            # build minimal entry for embeds
            we = WatchEntry(series_id=str(sid), series_title="Unknown", aliases=[], forum_channel_id=tgt.parent.id, thread_id=tgt.id)

        session = await self._session_ensure()
        client = MUClient(session)

        # Fetch releases → upsert to DB
        try:
            rels = await client.get_series_releases(sid, page=1, per_page=25)
        except Exception as e:
            return await interaction.followup.send(S("mu.error.generic", msg=str(e)), ephemeral=True)

        results = rels.get("results", []) if isinstance(rels, dict) else rels
        if not results:
            return await interaction.followup.send(S("mu.error.no_releases"), ephemeral=True)

        # Normalize timestamps & bulk upsert
        for r in results:
            if "release_ts" not in r or r["release_ts"] is None:
                r["release_ts"] = _release_ts(r)
        models.mu_bulk_upsert_releases(sid, results)

        # Baseline: if first time (state last_release_ts is None), silently mark newest as posted
        if we.last_release_ts is None:
            newest_ts = max((_release_ts(r) for r in results), default=-1)
            # find the id for newest_ts
            newest = None
            for r in results:
                if _release_ts(r) == newest_ts:
                    newest = r
                    break
            if newest is not None:
                models.mu_mark_posted(interaction.guild_id, tgt.id, sid, int(newest.get("release_id") or newest.get("id")))
                # persist to JSON state for future baseline checks
                for e in self.state[gid]["entries"]:
                    if int(e.get("thread_id")) == we.thread_id:
                        e["last_release_ts"] = newest_ts
                        break
                _save_state(self.state)

        # Determine unposted releases for this thread
        unposted = models.mu_list_unposted_for_thread(
            interaction.guild_id, tgt.id, sid, english_only=FILTER_ENGLISH_ONLY
        )

        if unposted:
            for tup in unposted:
                rid = int(tup[0])
                rel = models.mu_get_release(sid, rid) or {
                    "release_id": rid,
                    "title": tup[1], "raw_title": tup[2], "description": tup[3],
                    "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                    "group": tup[7], "url": tup[8], "release_ts": tup[9],
                }
                await self._post_release(tgt, we, rel)
                models.mu_mark_posted(interaction.guild_id, tgt.id, sid, rid)

            return await interaction.followup.send(S("mu.check.posted", count=len(unposted)), ephemeral=True)

        # No unposted: show latest known from DB
        latest_ts = models.mu_latest_release_ts(sid)
        if latest_ts == -1:
            return await interaction.followup.send(S("mu.error.no_releases"), ephemeral=True)

        # try to pick a release with max ts (we didn't store an index to map ts->id, so pick the last from API result)
        latest = max(results, key=lambda x: _release_ts(x))
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

        for gid, key, we in all_entries:
            thread = self.bot.get_channel(we.thread_id)
            if not isinstance(thread, discord.Thread) or not isinstance(getattr(thread, "parent", None), discord.ForumChannel):
                self._prune_entry(gid, we.thread_id)
                continue

            sid = models.mu_get_thread_series(we.thread_id, gid) or we.series_id

            # Fetch releases → upsert
            try:
                rels = await client.get_series_releases(sid, page=1, per_page=25)
            except Exception:
                continue

            results = rels.get("results", []) if isinstance(rels, dict) else rels
            if not results:
                continue

            for r in results:
                if "release_ts" not in r or r["release_ts"] is None:
                    r["release_ts"] = _release_ts(r)

            models.mu_bulk_upsert_releases(sid, results)

            # Baseline init if needed
            if we.last_release_ts is None:
                newest_ts = max((_release_ts(r) for r in results), default=-1)
                newest = None
                for r in results:
                    if _release_ts(r) == newest_ts:
                        newest = r
                        break
                if newest is not None:
                    models.mu_mark_posted(gid, we.thread_id, sid, int(newest.get("release_id") or newest.get("id")))
                    for ee in self.state[str(gid)]["entries"]:
                        if int(ee["thread_id"]) == we.thread_id:
                            ee["last_release_ts"] = newest_ts
                            break
                    _save_state(self.state)
                continue  # skip posting on the very first cycle

            # Post any unposted
            unposted = models.mu_list_unposted_for_thread(
                gid, we.thread_id, sid, english_only=FILTER_ENGLISH_ONLY
            )
            for tup in unposted:
                rid = int(tup[0])
                rel = models.mu_get_release(sid, rid) or {
                    "release_id": rid,
                    "title": tup[1], "raw_title": tup[2], "description": tup[3],
                    "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                    "group": tup[7], "url": tup[8], "release_ts": tup[9],
                }
                try:
                    await self._post_release(thread, we, rel)
                    models.mu_mark_posted(gid, we.thread_id, sid, rid)
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
        _
        
async def setup(bot: commands.Bot):
    await bot.add_cog(MUWatcher(bot))
