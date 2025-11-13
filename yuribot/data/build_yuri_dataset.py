"""Utility for building a richer Yuri-focused akinator dataset from public APIs."""
from __future__ import annotations

import argparse
import dataclasses
import html
import json
import logging
import re
import textwrap
import time
import urllib.error
import urllib.request
from typing import Callable, Iterable, List, Optional, Sequence


LOG = logging.getLogger(__name__)

ANILIST_API = "https://graphql.anilist.co"
ANILIST_TAGS = ["Yuri", "Girls Love"]


class DatasetError(RuntimeError):
    """Raised when the dataset builder cannot talk to a remote API."""


@dataclasses.dataclass(slots=True)
class MediaTag:
    name: str
    rank: Optional[int]
    is_adult: bool


@dataclasses.dataclass(slots=True)
class MediaInfo:
    title: str
    format: Optional[str]
    source: Optional[str]
    season_year: Optional[int]
    genres: Sequence[str]
    tags: Sequence[MediaTag]
    is_adult: bool

    def has_genre(self, candidates: Iterable[str]) -> bool:
        genre_set = {g.lower() for g in self.genres}
        return any(c.lower() in genre_set for c in candidates)

    def tag_score(self, *names: str) -> float:
        best = 0.0
        for tag in self.tags:
            for name in names:
                if tag.name.lower() == name.lower():
                    if tag.rank is None:
                        best = max(best, 0.6)
                    elif tag.rank >= 70:
                        best = max(best, 1.0)
                    elif tag.rank >= 50:
                        best = max(best, 0.8)
                    else:
                        best = max(best, 0.6)
        return best


@dataclasses.dataclass(slots=True)
class CharacterInfo:
    name: str
    role: str
    series: str
    description: str


AnswerValue = str


def boolish_to_answer(score: Optional[float]) -> AnswerValue:
    if score is None:
        return "unknown"
    if score >= 0.85:
        return "yes"
    if score >= 0.6:
        return "probably"
    if score >= 0.35:
        return "unknown"
    if score >= 0.15:
        return "probably_not"
    return "no"


FeatureEvaluator = Callable[[MediaInfo, CharacterInfo], AnswerValue]


@dataclasses.dataclass(slots=True)
class Feature:
    question: str
    evaluator: FeatureEvaluator


def _student_answer(_media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    description = character.description.lower()
    score = 0.0
    if any(token in description for token in ("student", "schoolgirl", "high school")):
        score = 1.0
    elif "club" in description and "school" in description:
        score = 0.7
    return boolish_to_answer(score)


def _slice_of_life_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 0.0
    if media.has_genre(["Slice of Life"]):
        score = 1.0
    elif media.tag_score("Iyashikei", "School", "Cute Girls Doing Cute Things") >= 0.6:
        score = 0.8
    return boolish_to_answer(score)


def _fantasy_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 0.0
    if media.has_genre(["Fantasy", "Supernatural"]):
        score = 1.0
    elif media.tag_score("Magic", "Mythology", "Ghost", "Witch") >= 0.6:
        score = 0.8
    return boolish_to_answer(score)


def _action_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 0.0
    if media.has_genre(["Action", "Adventure", "Military"]):
        score = 0.85
    elif media.tag_score("Swordplay", "Gunfights", "Martial Arts", "Battle") >= 0.6:
        score = 0.7
    return boolish_to_answer(score)


def _scifi_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 0.0
    if media.has_genre(["Sci-Fi", "Mecha"]):
        score = 0.9
    elif media.tag_score("Space", "Cyberpunk", "Artificial Intelligence") >= 0.6:
        score = 0.8
    return boolish_to_answer(score)


def _music_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 0.0
    if media.has_genre(["Music"]):
        score = 0.95
    elif media.tag_score("Idol", "Band", "Performance") >= 0.6:
        score = 0.75
    return boolish_to_answer(score)


def _mature_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    score = 1.0 if media.is_adult else 0.0
    if score == 0.0 and media.tag_score("Nudity", "Sexual Content") >= 0.6:
        score = 0.7
    return boolish_to_answer(score)


def _protagonist_answer(_media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    if character.role.upper() == "MAIN":
        return "yes"
    if character.role.upper() == "SUPPORTING":
        return "probably_not"
    return "unknown"


def _source_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    source = (media.source or "").upper()
    if source in {"MANGA", "LIGHT_NOVEL", "NOVEL"}:
        return "yes"
    if source in {"ORIGINAL", "VIDEO_GAME"}:
        return "no"
    return "unknown"


def _contemporary_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    fantasy_score = _fantasy_answer(media, character)
    scifi_score = _scifi_answer(media, character)

    def value(answer: AnswerValue) -> float:
        table = {
            "yes": 1.0,
            "probably": 0.7,
            "unknown": 0.5,
            "probably_not": 0.3,
            "no": 0.0,
        }
        return table.get(answer, 0.5)

    combined = (value(fantasy_score) + value(scifi_score)) / 2
    if combined <= 0.2 and not media.has_genre(["Historical"]):
        return "yes"
    if combined <= 0.4:
        return "probably"
    if combined >= 0.8:
        return "no"
    return "unknown"


def _recent_answer(media: MediaInfo, character: CharacterInfo) -> AnswerValue:
    _ = character
    if media.season_year is None:
        return "unknown"
    if media.season_year >= 2018:
        return "yes"
    if media.season_year >= 2010:
        return "probably"
    if media.season_year <= 2000:
        return "no"
    return "probably_not"


FEATURES: List[Feature] = [
    Feature(
        question="Is the character still a student when their story begins?",
        evaluator=_student_answer,
    ),
    Feature(
        question="Does the story lean into slice-of-life or school club vibes?",
        evaluator=_slice_of_life_answer,
    ),
    Feature(
        question="Does the series feature fantasy or supernatural elements?",
        evaluator=_fantasy_answer,
    ),
    Feature(
        question="Is combat or organized action a major part of the plot?",
        evaluator=_action_answer,
    ),
    Feature(
        question="Is the setting futuristic or science-fiction focused?",
        evaluator=_scifi_answer,
    ),
    Feature(
        question="Is the cast tied to idols or music performances?",
        evaluator=_music_answer,
    ),
    Feature(
        question="Is the series targeted at mature audiences?",
        evaluator=_mature_answer,
    ),
    Feature(
        question="Is this character one of the main protagonists?",
        evaluator=_protagonist_answer,
    ),
    Feature(
        question="Did the story originate as a manga or light novel?",
        evaluator=_source_answer,
    ),
    Feature(
        question="Is the story set in a contemporary, mostly real-world setting?",
        evaluator=_contemporary_answer,
    ),
    Feature(
        question="Did the series debut in the modern streaming era (2010+)?",
        evaluator=_recent_answer,
    ),
]


def strip_html(description: Optional[str]) -> str:
    if not description:
        return ""
    text = html.unescape(description)
    text = re.sub(r"<[^>]+>", " ", text)
    text = text.replace("\n", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def anilist_request(query: str, variables: dict) -> dict:
    payload = json.dumps({"query": query, "variables": variables}).encode("utf-8")
    request = urllib.request.Request(
        ANILIST_API,
        data=payload,
        headers={"Content-Type": "application/json", "Accept": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=20) as response:
            raw = response.read()
    except urllib.error.URLError as exc:  # pragma: no cover - network heavy
        raise DatasetError(f"AniList request failed: {exc}") from exc
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:  # pragma: no cover - invalid payload
        raise DatasetError("AniList returned invalid JSON") from exc
    if "errors" in data:
        raise DatasetError(f"AniList responded with errors: {data['errors']}")
    return data["data"]


ANILIST_QUERY = textwrap.dedent(
    """
    query ($page: Int, $perPage: Int, $tags: [String]) {
      Page(page: $page, perPage: $perPage) {
        media(tag_in: $tags, type: ANIME, sort: POPULARITY_DESC) {
          id
          title { romaji english native }
          format
          source
          seasonYear
          genres
          tags { name rank isAdult }
          isAdult
          characters(perPage: 10) {
            edges {
              role
              node {
                id
                name { full }
                description
              }
            }
          }
        }
      }
    }
    """
)


def fetch_media(pages: int, per_page: int) -> List[dict]:
    media: List[dict] = []
    for page in range(1, pages + 1):
        LOG.info("Fetching AniList page %s/%s", page, pages)
        data = anilist_request(ANILIST_QUERY, {"page": page, "perPage": per_page, "tags": ANILIST_TAGS})
        items = data.get("Page", {}).get("media", [])
        media.extend(items)
        time.sleep(0.3)  # be nice to the API
    return media


def convert_media(entry: dict) -> MediaInfo:
    tags = [
        MediaTag(name=tag.get("name", ""), rank=tag.get("rank"), is_adult=bool(tag.get("isAdult")))
        for tag in entry.get("tags", [])
    ]
    title = entry.get("title", {}).get("romaji") or entry.get("title", {}).get("english") or "Unknown"
    return MediaInfo(
        title=title,
        format=entry.get("format"),
        source=entry.get("source"),
        season_year=entry.get("seasonYear"),
        genres=entry.get("genres", []),
        tags=tags,
        is_adult=bool(entry.get("isAdult")),
    )


def convert_character(edge: dict, media: MediaInfo) -> Optional[CharacterInfo]:
    node = edge.get("node")
    if not node:
        return None
    name = node.get("name", {}).get("full")
    if not name:
        return None
    description = strip_html(node.get("description"))
    role = edge.get("role", "")
    return CharacterInfo(name=name, role=role or "UNKNOWN", series=media.title, description=description)


def build_characters(media_entries: List[dict], limit: int) -> List[dict]:
    characters: List[dict] = []
    for entry in media_entries:
        media = convert_media(entry)
        edges = entry.get("characters", {}).get("edges", [])
        for edge in edges:
            character = convert_character(edge, media)
            if not character:
                continue
            answers = [feature.evaluator(media, character) for feature in FEATURES]
            characters.append(
                {
                    "name": character.name,
                    "series": character.series,
                    "blurb": character.description[:200],
                    "answers": answers,
                }
            )
            if len(characters) >= limit:
                return characters
    return characters


def build_dataset(pages: int, per_page: int, limit: int) -> dict:
    media_entries = fetch_media(pages, per_page)
    characters = build_characters(media_entries, limit)
    questions = [feature.question for feature in FEATURES]
    return {
        "yuri_remote": {
            "title": "Yuri Galaxy",
            "questions": questions,
            "characters": characters,
        }
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pages", type=int, default=2, help="Number of AniList result pages to scan")
    parser.add_argument("--per-page", type=int, default=25, help="AniList page size")
    parser.add_argument("--limit", type=int, default=60, help="Maximum number of character entries to keep")
    parser.add_argument("--output", type=str, default="yuri_remote_dataset.json", help="Where to write the dataset JSON")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    dataset = build_dataset(args.pages, args.per_page, args.limit)
    with open(args.output, "w", encoding="utf-8") as handle:
        json.dump(dataset, handle, ensure_ascii=False, indent=2)
    LOG.info("Wrote %s characters to %s", len(dataset["yuri_remote"]["characters"]), args.output)


if __name__ == "__main__":  # pragma: no cover - CLI entrypoint
    main()

