"""Helpers for loading remote akinator datasets on demand."""
from __future__ import annotations

import json
import logging
import os
import urllib.request
from typing import Any, Dict, Tuple

from .akinator_sets import AKINATOR_SETS, DEFAULT_SET, YURI_SET, CharacterEntry, GameSet

LOG = logging.getLogger(__name__)

_REMOTE_CACHE: Dict[str, GameSet] | None = None
_REMOTE_SOURCE: str | None = None

REMOTE_DATA_ENV = "AKINATOR_DATA_URL"
DEFAULT_SET_ENV = "AKINATOR_DEFAULT_SET"
YURI_SET_ENV = "AKINATOR_YURI_SET"


def load_available_sets() -> Tuple[Dict[str, GameSet], str, str]:
    """Return the merged dataset mapping and chosen default keys."""

    sets: Dict[str, GameSet] = dict(AKINATOR_SETS)

    remote_url = os.getenv(REMOTE_DATA_ENV)
    if remote_url:
        sets.update(_load_remote_sets(remote_url))

    default_key = os.getenv(DEFAULT_SET_ENV, DEFAULT_SET)
    if default_key not in sets:
        LOG.warning("Configured default set '%s' missing; falling back to '%s'", default_key, DEFAULT_SET)
        default_key = DEFAULT_SET

    yuri_key = os.getenv(YURI_SET_ENV, YURI_SET)
    if yuri_key not in sets:
        LOG.warning("Configured yuri set '%s' missing; falling back to '%s'", yuri_key, default_key)
        yuri_key = default_key if default_key in sets else DEFAULT_SET

    return sets, default_key, yuri_key


def _load_remote_sets(url: str) -> Dict[str, GameSet]:
    global _REMOTE_CACHE, _REMOTE_SOURCE

    if _REMOTE_CACHE is not None and url == _REMOTE_SOURCE:
        return _REMOTE_CACHE

    try:
        LOG.info("Fetching akinator dataset from %s", url)
        with urllib.request.urlopen(url, timeout=10) as response:
            payload = response.read()
    except Exception as exc:  # pragma: no cover - best effort network fetch
        LOG.warning("Failed to download remote akinator data: %s", exc)
        _REMOTE_CACHE = {}
        _REMOTE_SOURCE = url
        return {}

    try:
        data = json.loads(payload)
    except json.JSONDecodeError as exc:
        LOG.warning("Remote akinator dataset is not valid JSON: %s", exc)
        _REMOTE_CACHE = {}
        _REMOTE_SOURCE = url
        return {}

    sets: Dict[str, GameSet] = {}
    if isinstance(data, dict):
        for key, value in data.items():
            normalized = _validate_game_set(key, value)
            if normalized:
                sets[key] = normalized
    else:
        LOG.warning("Remote akinator dataset did not contain an object map")

    _REMOTE_CACHE = sets
    _REMOTE_SOURCE = url
    return sets


def _validate_game_set(key: str, data: Any) -> GameSet | None:
    if not isinstance(data, dict):
        LOG.warning("Skipping dataset '%s' - value must be an object", key)
        return None

    title = data.get("title")
    questions = data.get("questions")
    characters = data.get("characters")

    if not isinstance(title, str) or not isinstance(questions, list) or not isinstance(characters, list):
        LOG.warning("Dataset '%s' missing required fields", key)
        return None

    if not questions:
        LOG.warning("Dataset '%s' does not contain any questions", key)
        return None

    normalized_questions = [q for q in questions if isinstance(q, str)]
    if len(normalized_questions) != len(questions):
        LOG.warning("Dataset '%s' contains invalid questions", key)
        return None

    normalized_characters: list[CharacterEntry] = []
    for entry in characters:
        normalized = _validate_character(entry, len(normalized_questions))
        if normalized:
            normalized_characters.append(normalized)

    if not normalized_characters:
        LOG.warning("Dataset '%s' does not contain any valid characters", key)
        return None

    return {
        "title": title,
        "questions": normalized_questions,
        "characters": normalized_characters,
    }


def _validate_character(entry: Any, expected_answers: int) -> CharacterEntry | None:
    if not isinstance(entry, dict):
        return None

    name = entry.get("name")
    series = entry.get("series")
    blurb = entry.get("blurb", "")
    answers = entry.get("answers")

    if not isinstance(name, str) or not isinstance(series, str) or not isinstance(answers, list):
        return None

    if len(answers) != expected_answers:
        return None

    normalized_answers = [str(answer) if isinstance(answer, str) else "unknown" for answer in answers]

    return {
        "name": name,
        "series": series,
        "blurb": blurb if isinstance(blurb, str) else "",
        "answers": normalized_answers,
    }

