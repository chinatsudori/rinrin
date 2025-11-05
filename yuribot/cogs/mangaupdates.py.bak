# mangaupdates_cog.py
from __future__ import annotations

import asyncio
import io
import json
import re
import hashlib
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple, Set

import aiohttp
import discord
from discord.ext import commands, tasks
from discord import app_commands

from .. import models
from ..strings import S
from ..utils.storage import resolve_data_file

API_BASE = "https://api.mangaupdates.com/v1"
DATA_FILE = resolve_data_file("mu_watch.json")
POLL_SECONDS = 4 * 60 * 60  # 4 hours

# --- Behavior toggles ---------------------------------------------------------
# 1) If True, when a thread is first linked/seen, we index ALL releases, mark them posted,
#    and set last_release_ts to newest. No posts are sent on the first cycle.
FIRST_RUN_SEED_ALL = True

# 2) Only post English-tagged releases, based on heuristic sniffing.
FILTER_ENGLISH_ONLY = False

# 3) Weak match threshold for search/auto-infer; show top-5 candidates if below.
WEAK_MATCH_THRESHOLD = 0.80

# Max bytes we'll attempt to upload for a cover image (Discord base limit is ~8 MiB)
MAX_COVER_BYTES = 7_500_000

# ---- SQLite integer bounds (signed 64-bit)
MAX_SQLITE_INT = (1 << 63) - 1

# -----------------------------------------------------------------------------


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
    r"(?:\bch(?:apter)?|\bc)\.?\s*(\d+(?:\.\d+)?)",
    r"\b(\d+(?:\.\d+)?)\s*[-–—]\s*(\d+(?:\.\d+)?)",
]
_VOL_PATTERNS = [
    r"(?:\bvol(?:ume)?|\bv)\.?\s*(\d+(?:\.\d+)?)",
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


def _is_english_release(rel: dict) -> bool:
    txt = f"{rel.get('title','')} {rel.get('raw_title','')} {rel.get('description','')} {rel.get('lang_hint','')}".lower()
    return any(k in txt for k in ("eng", "english", "[en]", "(en)"))


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


# ---- Helpers to make releases SQLite-safe ------------------------------------

def _seconds_from_any(ts: int | float | None) -> int:
    """Normalize timestamps: if it looks like milliseconds, convert to seconds."""
    if ts is None:
        return -1
    try:
        v = int(ts)
    except Exception:
        return -1
    if v >= 100_000_000_000:  # probably ms
        v //= 1000
    return max(v, -1)


def _stable_63bit_id(key: str) -> int:
    """Deterministic signed-64 safe integer from a string key."""
    h = hashlib.sha1(key.encode("utf-8")).digest()
    v = int.from_bytes(h[:8], "big") & MAX_SQLITE_INT
    if v == 0:
        v = 1
    return v


def _normalize_release_record(series_id: str, r: dict) -> dict:
    """Return a copy of r with safe release_id and release_ts."""
    out = dict(r)
    ts = _release_ts(out)
    ts = _seconds_from_any(ts)
    out["release_ts"] = ts

    rid_raw = out.get("release_id", out.get("id"))
    rid: Optional[int] = None
    try:
        if isinstance(rid_raw, (int, float)) or (isinstance(rid_raw, str) and rid_raw.isdigit()):
            rid = int(rid_raw)
    except Exception:
        rid = None

    if rid is None or rid <= 0 or rid > MAX_SQLITE_INT:
        key = "|".join([
            str(series_id),
            str(out.get("id") or ""),
            str(out.get("release_id") or ""),
            str(out.get("title") or ""),
            str(out.get("url") or ""),
            str(ts),
        ])
        rid = _stable_63bit_id(key)

    out["release_id"] = rid
    return out


# --- MU tags → Forum tags mapping helpers ------------------------------------

_MU_CANON_TAGS = [
    "josei", "lolicon", "seinen", "shotacon", "shoujo", "shoujo ai", "shounen",
    "shounen ai", "yaoi", "yuri",
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
                        results = []
                        for r in data.get("results", []) if isinstance(data, dict) else []:
                            nr = _normalize_release_record(series_id, r)
                            results.append(nr)
                        return {"results": results}
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
                    try:
                        rid = int(m_id_in_link.group(1))
                    except Exception:
                        rid = None

            r = {
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
            }

            releases.append(_normalize_release_record(str(series_id), r))

        return {"results": releases}


@dataclass
class WatchEntry:
    series_id: str
    series_title: str
    aliases: List[str]
    forum_channel_id: int
    thread_id: int
    last_release_id: Optional[int] = None
    last_release_ts: Optional[int] = None


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

        forum = _resolve_mu_forum(interaction.guild)
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

        thread_obj = created_any.thread if hasattr(created_any, "thread") else created_any  # type: ignore

        try:
            models.mu_register_thread_series(interaction.guild_id, thread_obj.id, sid, title)
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
        _save_state(self.state)

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
                rsid, rtitle = _sid_title_from_result(r)
                if not rsid:
                    continue
                aliases = []
                score = 0.0
                try:
                    full = await client.get_series(rsid)
                    raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                    aliases = _stringify_aliases(raw_aliases)
                    score = _best_match_score(series, rtitle, aliases)
                except Exception:
                    score = _best_match_score(series, rtitle, [])
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
            models.mu_register_thread_series(interaction.guild_id, interaction.channel.id, sid, title)
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
        _save_state(self.state)

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
        _save_state(self.state)

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

        rows = models.mu_list_links_for_guild(interaction.guild_id)
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

        sid = models.mu_get_thread_series(tgt.id, interaction.guild_id)
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
        results = [_normalize_release_record(sid, r) for r in results]
        models.mu_bulk_upsert_releases(sid, results)

        # First-run behavior
        if we.last_release_ts is None and FIRST_RUN_SEED_ALL:
            # mark ALL as posted, set last_release_ts to newest, and bail
            newest_ts = max((_release_ts(r) for r in results), default=-1)
            for r in results:
                models.mu_mark_posted(interaction.guild_id, tgt.id, sid, int(r.get("release_id")))
            for e in self.state[gid]["entries"]:
                if int(e.get("thread_id")) == we.thread_id:
                    e["last_release_ts"] = newest_ts
                    break
            _save_state(self.state)
            return await interaction.followup.send("Indexed existing releases. No new updates.", ephemeral=True)

        # Normal "post only new" path
        unposted = models.mu_list_unposted_for_thread(
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
            rel = models.mu_get_release(sid, rid) or {
                "release_id": rid,
                "title": tup[1], "raw_title": tup[2], "description": tup[3],
                "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                "group": tup[7], "url": tup[8], "release_ts": _seconds_from_any(tup[9]),
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
                    models.mu_mark_posted(interaction.guild_id, tgt.id, sid, rid)
                for e in self.state[gid]["entries"]:
                    if int(e["thread_id"]) == we.thread_id:
                        prev = e.get("last_release_ts") or -1
                        e["last_release_ts"] = int(max(prev, max_posted_ts))
                        break
                _save_state(self.state)

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

        fch = forum or _resolve_mu_forum(interaction.guild)
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
            sid = models.mu_get_thread_series(th.id, interaction.guild_id)
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
                    rsid, rtitle = _sid_title_from_result(r)
                    if not rsid:
                        continue
                    aliases = []
                    score = 0.0
                    try:
                        full = await client.get_series(rsid)
                        raw_aliases = full.get("associated_names") or full.get("associated") or full.get("associated_names_ascii") or []
                        aliases = _stringify_aliases(raw_aliases)
                        score = _best_match_score(name, rtitle, aliases)
                    except Exception:
                        score = _best_match_score(name, rtitle, [])
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
                        models.mu_register_thread_series(interaction.guild_id, th.id, sid, title)
                        attached += 1
                    except Exception:
                        pass
                else:
                    attached += 1  # would-attach

            # Pull releases and index them — never post
            sid = sid or models.mu_get_thread_series(th.id, interaction.guild_id)
            if not sid:
                continue

            try:
                rels = await client.get_series_releases(sid, page=1, per_page=50)
            except Exception:
                continue

            items = rels.get("results", []) if isinstance(rels, dict) else []
            if english_only:
                items = [r for r in items if _is_english_release(r)]
            if not items:
                continue

            if not dry_run:
                models.mu_bulk_upsert_releases(sid, items)
                # Mark ALL as posted in this thread so the watcher won’t flood
                max_ts = max(int(r.get("release_ts") or -1) for r in items)
                for r in items:
                    rid = int(r.get("release_id"))
                    models.mu_mark_posted(interaction.guild_id, th.id, sid, rid)

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
                _save_state(self.state)

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
        if FILTER_ENGLISH_ONLY and not _is_english_release(rel):
            return False

        chbits, extras = _format_rel_bits(rel)
        ts_val = rel.get("release_ts")
        dt = None
        try:
            if isinstance(ts_val, (int, float)) and ts_val > 0:
                dt = datetime.fromtimestamp(int(ts_val), tz=timezone.utc)
        except Exception:
            dt = None

        em = discord.Embed(
            title=S("mu.update.title", series=we.series_title, chbits=chbits),
            description=extras or None,
            color=discord.Color.blurple(),
            timestamp=dt or _now_utc(),
        )
        em.set_footer(text=S("mu.update.footer"))

        try:
            await thread.send(embed=em, allowed_mentions=discord.AllowedMentions.none())
            return True
        except Exception:
            return False

    async def _post_batch(self, thread: discord.Thread, we: WatchEntry, rels: List[dict]) -> int:
        items: List[dict] = []
        for r in rels:
            if FILTER_ENGLISH_ONLY and not _is_english_release(r):
                continue
            items.append(r)
        if not items:
            return 0

        if len(items) == 1:
            ok = await self._post_release(thread, we, items[0])
            return 1 if ok else 0

        lines: List[str] = []
        MAX_LINES = 15
        for r in items[:MAX_LINES]:
            chbits, _ = _format_rel_bits(r)
            url = _strip(r.get("url") or "")
            maybe_url = f" ({url})" if url else ""
            lines.append(S("mu.batch.line", chbits=chbits, maybe_url=maybe_url))

        overflow = len(items) - len(lines)
        if overflow > 0:
            lines.append(f"... +{overflow} more")

        em = discord.Embed(
            title=S("mu.batch.title", series=we.series_title, n=len(items)),
            description="\n".join(lines),
            color=discord.Color.blurple(),
            timestamp=_now_utc(),
        )
        em.set_footer(text=S("mu.batch.footer"))

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

            sid = models.mu_get_thread_series(we.thread_id, gid) or we.series_id

            try:
                rels = await client.get_series_releases(sid, page=1, per_page=25)
            except Exception:
                continue

            results = rels.get("results", []) if isinstance(rels, dict) else rels
            if not results:
                continue

            models.mu_bulk_upsert_releases(sid, results)

            if we.last_release_ts is None and FIRST_RUN_SEED_ALL:
                newest_ts = max((_release_ts(r) for r in results), default=-1)
                for r in results:
                    models.mu_mark_posted(gid, we.thread_id, sid, int(r.get("release_id")))
                try:
                    for ee in self.state[str(gid)]["entries"]:
                        if int(ee["thread_id"]) == we.thread_id:
                            ee["last_release_ts"] = newest_ts
                            break
                    _save_state(self.state)
                except Exception:
                    pass
                continue  # skip posting on the first cycle

            unposted = models.mu_list_unposted_for_thread(
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
                rel = models.mu_get_release(sid, rid) or {
                    "release_id": rid,
                    "title": tup[1], "raw_title": tup[2], "description": tup[3],
                    "volume": tup[4], "chapter": tup[5], "subchapter": tup[6],
                    "group": tup[7], "url": tup[8], "release_ts": _seconds_from_any(tup[9]),
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
                            models.mu_mark_posted(gid, we.thread_id, sid, rid)
                        for ee in self.state[str(gid)]["entries"]:
                            if int(ee["thread_id"]) == we.thread_id:
                                prev = ee.get("last_release_ts") or -1
                                ee["last_release_ts"] = int(max(prev, max_posted_ts))
                                break
                        _save_state(self.state)
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
        _save_state(self.state)


async def setup(bot: commands.Bot):
    await bot.add_cog(MUWatcher(bot))
