from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Set, Tuple

import aiohttp
import discord

from ..models import mangaupdates as mu_models, settings
from ..strings import S
from .storage import resolve_data_file

API_BASE = "https://api.mangaupdates.com/v1"
DATA_FILE = resolve_data_file("mu_watch.json")
POLL_SECONDS = 4 * 60 * 60  # 4 hours

# --- Behavior toggles ---------------------------------------------------------
# 1) If True, when a thread is first linked/seen, we index ALL releases, mark them
#    posted, and set last_release_ts to newest. No posts are sent on the first cycle.
FIRST_RUN_SEED_ALL = True

# 2) Only post English-tagged releases, based on heuristic sniffing.
FILTER_ENGLISH_ONLY = False

# 3) Weak match threshold for search/auto-infer; show top-5 candidates if below.
WEAK_MATCH_THRESHOLD = 0.80

# Max bytes we'll attempt to upload for a cover image (Discord base limit is ~8 MiB)
MAX_COVER_BYTES = 7_500_000

# ---- SQLite integer bounds (signed 64-bit)
MAX_SQLITE_INT = (1 << 63) - 1

FORUM_TAG_PRIORITY = [
    "manga",
    "manhwa",
    "manhua",
    "webtoon",
    "fantasy",
    "slice of life",
    "drama",
    "sci-fi",
    "mystery",
    "horror",
    "tragedy",
    "comedy",
    "isekai",
    "harem",
    "ecchi",
    "psychological",
    "violence/gore",
    "historical",
    "nsfw",
]

__all__ = [
    "API_BASE",
    "DATA_FILE",
    "POLL_SECONDS",
    "FIRST_RUN_SEED_ALL",
    "FILTER_ENGLISH_ONLY",
    "WEAK_MATCH_THRESHOLD",
    "FORUM_TAG_PRIORITY",
    "WatchEntry",
    "MUClient",
    "now_utc",
    "strip_text",
    "best_match_score",
    "series_id_title_from_result",
    "stringify_aliases",
    "forum_post_name",
    "extract_max_chapter",
    "extract_max_volume",
    "parse_timestamp",
    "release_timestamp",
    "is_english_release",
    "seconds_from_any",
    "normalize_release_record",
    "map_mu_to_forum_tags",
    "scrape_mu_tags_and_type",
    "fetch_cover_image",
    "load_state",
    "save_state",
    "resolve_mu_forum",
]


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def strip_text(value: Optional[str]) -> str:
    return (value or "").strip()


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", value or "").strip().lower()


def best_match_score(query: str, title: str, aliases: List[str]) -> float:
    q = _normalize_text(query)
    candidates = [_normalize_text(title)] + [_normalize_text(a) for a in aliases]
    best = 0.0
    for cand in candidates:
        if q == cand:
            return 1.0
        if cand.startswith(q) or q.startswith(cand):
            best = max(best, 0.9)
        if q in cand or cand in q:
            best = max(best, 0.8)
    return best


def series_id_title_from_result(result: dict) -> Tuple[Optional[str], str]:
    record = result.get("record") or {}
    sid = (
        result.get("series_id")
        or result.get("id")
        or record.get("series_id")
        or record.get("id")
    )
    sid = str(sid) if sid is not None else None
    title = result.get("title") or record.get("title") or "Unknown"
    return sid, title


def stringify_aliases(raw) -> List[str]:
    out: List[str] = []
    for item in raw or []:
        if isinstance(item, str):
            text = item.strip()
        elif isinstance(item, dict):
            text = (
                item.get("name")
                or item.get("title")
                or item.get("value")
                or item.get("text")
                or ""
            ).strip()
        else:
            text = ""
        if text:
            out.append(text)

    seen = set()
    unique: List[str] = []
    for text in out:
        key = text.casefold()
        if key not in seen:
            seen.add(key)
            unique.append(text)
    return unique


def forum_post_name(name: str) -> str:
    cleaned = re.sub(r"\s+", " ", (name or "").strip())
    if not cleaned:
        cleaned = "Untitled"
    return cleaned[:100]


def _find_all_numbers(text: str, patterns: List[str]) -> List[float]:
    nums: List[float] = []
    for pattern in patterns:
        for match in re.finditer(pattern, text, flags=re.I):
            for group in match.groups():
                if not group:
                    continue
                try:
                    nums.append(float(group))
                except Exception:
                    pass
    return nums


_CH_PATTERNS = [
    r"(?:\bch(?:apter)?|\bc)\.?\s*(\d+(?:\.\d+)?)",
    r"\b(\d+(?:\.\d+)?)\s*[---]\s*(\d+(?:\.\d+)?)",
]
_VOL_PATTERNS = [
    r"(?:\bvol(?:ume)?|\bv)\.?\s*(\d+(?:\.\d+)?)",
]


def extract_max_chapter(text: str) -> Tuple[str, str]:
    nums = _find_all_numbers(text, _CH_PATTERNS)
    if not nums:
        return "", ""
    mx = max(nums)
    formatted = f"{mx}".rstrip("0").rstrip(".")
    if "." in formatted:
        chapter, sub = formatted.split(".", 1)
        return chapter, sub
    return formatted, ""


def extract_max_volume(text: str) -> str:
    nums = _find_all_numbers(text, _VOL_PATTERNS)
    if not nums:
        return ""
    mx = max(nums)
    return f"{mx}".rstrip("0").rstrip(".")


def parse_timestamp(value: Optional[str]) -> Optional[int]:
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


def release_timestamp(rel: dict) -> int:
    val = rel.get("release_ts")
    if isinstance(val, (int, float)):
        return int(val)
    for key in ("release_date", "date", "pubDate", "pubdate"):
        if key in rel and rel[key]:
            ts = parse_timestamp(rel[key])
            if ts is not None:
                return ts
    return -1


def is_english_release(rel: dict) -> bool:
    text = " ".join(
        [
            str(rel.get("title", "")),
            str(rel.get("raw_title", "")),
            str(rel.get("description", "")),
            str(rel.get("lang_hint", "")),
        ]
    ).lower()
    return any(token in text for token in ("eng", "english", "[en]", "(en)"))


def seconds_from_any(value: int | float | None) -> int:
    if value is None:
        return -1
    try:
        ts = int(value)
    except Exception:
        return -1
    if ts >= 100_000_000_000:
        ts //= 1000
    return max(ts, -1)


def _stable_63bit_id(key: str) -> int:
    digest = hashlib.sha1(key.encode("utf-8")).digest()
    val = int.from_bytes(digest[:8], "big") & MAX_SQLITE_INT
    if val == 0:
        val = 1
    return val


def normalize_release_record(series_id: str, release: dict) -> dict:
    out = dict(release)
    ts = release_timestamp(out)
    ts = seconds_from_any(ts)
    out["release_ts"] = ts

    rid_raw = out.get("release_id", out.get("id"))
    rid: Optional[int] = None
    try:
        if isinstance(rid_raw, (int, float)) or (
            isinstance(rid_raw, str) and rid_raw.isdigit()
        ):
            rid = int(rid_raw)
    except Exception:
        rid = None

    if rid is None or rid <= 0 or rid > MAX_SQLITE_INT:
        key = "|".join(
            [
                str(series_id),
                str(out.get("id") or ""),
                str(out.get("release_id") or ""),
                str(out.get("title") or ""),
                str(out.get("url") or ""),
                str(ts),
            ]
        )
        rid = _stable_63bit_id(key)

    out["release_id"] = rid
    return out


_MU_CANON_TAGS = [
    "josei",
    "lolicon",
    "seinen",
    "shotacon",
    "shoujo",
    "shoujo ai",
    "shounen",
    "shounen ai",
    "yaoi",
    "yuri",
    "action",
    "adult",
    "adventure",
    "comedy",
    "doujinshi",
    "drama",
    "ecchi",
    "fantasy",
    "gender bender",
    "harem",
    "hentai",
    "historical",
    "horror",
    "martial arts",
    "mature",
    "mecha",
    "mystery",
    "psychological",
    "romance",
    "school life",
    "sci-fi",
    "slice of life",
    "smut",
    "sports",
    "supernatural",
    "tragedy",
    "isekai",
]


def map_mu_to_forum_tags(mu_tags: Set[str]) -> Set[str]:
    lowered = {tag.lower() for tag in mu_tags}
    mapped: Set[str] = set()

    if "hentai" in lowered:
        mapped.add("nsfw")
    if {"slice of life", "school life"} & lowered:
        mapped.add("slice of life")
    if "psychological" in lowered:
        mapped.add("psychological")
    if {"action", "martial arts", "mecha"} & lowered:
        mapped.add("violence/gore")
    if "historical" in lowered:
        mapped.add("historical")

    direct_map = {
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
    for mu_tag, forum_tag in direct_map.items():
        if mu_tag in lowered:
            mapped.add(forum_tag)

    return mapped


async def scrape_mu_tags_and_type(
    session: aiohttp.ClientSession,
    sid: str,
    series_json: dict | None,
) -> Tuple[Optional[str], Set[str]]:
    mu_tags: Set[str] = set()
    type_tag: Optional[str] = None

    series_data = series_json or {}
    for key in ("genres", "genre", "categories", "tags", "themes"):
        raw = series_data.get(key)
        if isinstance(raw, list):
            for value in raw:
                if isinstance(value, str):
                    mu_tags.add(value.strip().lower())
                elif isinstance(value, dict):
                    name = (
                        value.get("name")
                        or value.get("title")
                        or value.get("value")
                        or ""
                    ).strip()
                    if name:
                        mu_tags.add(name.lower())

    for key in ("type", "format", "media_type"):
        val = str(series_data.get(key) or "").strip().lower()
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
                async with session.get(
                    url, timeout=aiohttp.ClientTimeout(total=12)
                ) as resp:
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
            for tag in _MU_CANON_TAGS:
                if tag in text:
                    mu_tags.add(tag)

    return type_tag, mu_tags


async def fetch_cover_image(
    session: aiohttp.ClientSession, series_json: dict
) -> Optional[discord.File]:
    urls: List[str] = []
    for key in ("cover", "image", "image_url", "thumbnail", "thumbnail_url"):
        val = series_json.get(key)
        if isinstance(val, str) and val.startswith(("http://", "https://")):
            urls.append(val)
        elif isinstance(val, dict):
            for inner_val in val.values():
                if isinstance(inner_val, str) and inner_val.startswith(
                    ("http://", "https://")
                ):
                    urls.append(inner_val)

    seen = set()
    urls = [url for url in urls if not (url in seen or seen.add(url))]

    for url in urls:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=12)) as resp:
                if resp.status != 200:
                    continue
                ctype = resp.headers.get("Content-Type", "").lower()
                if not any(token in ctype for token in ("image/", "jpeg", "png", "webp")):
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


@dataclass
class WatchEntry:
    series_id: str
    series_title: str
    aliases: List[str]
    forum_channel_id: int
    thread_id: int
    last_release_id: Optional[int] = None
    last_release_ts: Optional[int] = None


def load_state() -> Dict[str, dict]:
    if DATA_FILE.exists():
        try:
            with DATA_FILE.open("r", encoding="utf-8") as handle:
                return json.load(handle)
        except Exception:
            return {}
    return {}


def save_state(state: Dict[str, dict]) -> None:
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with DATA_FILE.open("w", encoding="utf-8") as handle:
        json.dump(state, handle, indent=2)


def resolve_mu_forum(guild: discord.Guild) -> Optional[discord.ForumChannel]:
    channel_id = settings.get_mu_forum_channel(guild.id)
    if channel_id:
        channel = guild.get_channel(channel_id)
        if isinstance(channel, discord.ForumChannel):
            return channel
    lname = "ongoing-reading-room"
    for channel in guild.channels:
        if (
            isinstance(channel, discord.ForumChannel)
            and channel.name.lower().strip() == lname
        ):
            return channel
    return None


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
            url,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=20),
            headers={**self._headers, **self._corsish},
        ) as resp:
            if resp.status != 200:
                txt = (await resp.text())[:200]
                raise RuntimeError(
                    S("mu.error.search_http", code=resp.status)
                    + (f" ({txt})" if txt else "")
                )
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
                raise RuntimeError(
                    S("mu.error.series_http", sid=series_id, code=resp.status)
                    + (f" ({txt})" if txt else "")
                )
            return await resp.json()

    async def get_series_releases(
        self, series_id: str, page: int = 1, per_page: int = 50
    ) -> dict:
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
                        raw_results = (
                            data.get("results", []) if isinstance(data, dict) else []
                        )
                        for release in raw_results:
                            results.append(normalize_release_record(series_id, release))
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

    async def get_series_releases_via_rss(
        self, series_id: str | int, *, limit: int = 50
    ) -> dict:
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
                raise RuntimeError(
                    S("mu.error.releases_http", sid=str(series_id), code=resp.status)
                    + (f" ({txt})" if txt else "")
                )

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
            channel = next(
                (child for child in root if child.tag.lower().endswith("channel")), None
            )
        elif root.tag.lower().endswith("channel"):
            channel = root
        else:
            for child in root.iter():
                if child.tag.lower().endswith("channel"):
                    channel = child
                    break

        if channel is None:
            return {"results": []}

        items = [item for item in channel if item.tag.lower().endswith("item")]
        releases = []

        def _text(element, tag):
            node = next(
                (child for child in element if child.tag.lower().endswith(tag)), None
            )
            return (node.text or "").strip() if node is not None and node.text else ""

        for item in items[:limit]:
            title = _text(item, "title")
            link = _text(item, "link")
            desc = _text(item, "description")
            pub = _text(item, "pubdate") or _text(item, "pubDate")

            ts = None
            try:
                dt = eut.parsedate_to_datetime(pub) if pub else None
                if dt is not None:
                    ts = int(dt.timestamp())
            except Exception:
                ts = None

            raw = f"{title} {desc}".strip()
            chapter, subchapter = extract_max_chapter(raw)
            volume = extract_max_volume(raw)
            m_group = re.search(r"\[(.*?)\]", title) or re.search(r"\[(.*?)\]", desc)
            group = m_group.group(1).strip() if m_group else ""

            rid = None
            if link:
                m_id_in_link = re.search(r"(\d{6,})", link)
                if m_id_in_link:
                    try:
                        rid = int(m_id_in_link.group(1))
                    except Exception:
                        rid = None

            release = {
                "id": rid,
                "release_id": rid,
                "chapter": chapter or "",
                "volume": volume or "",
                "subchapter": subchapter or "",
                "group": group or "",
                "url": link or "",
                "release_date": (
                    datetime.utcfromtimestamp(ts).isoformat() + "Z"
                )
                if ts
                else "",
                "release_ts": ts if ts is not None else -1,
                "title": title or "",
                "raw_title": title or "",
                "description": desc or "",
                "lang_hint": raw.lower(),
            }

            releases.append(normalize_release_record(str(series_id), release))

        return {"results": releases}
