from __future__ import annotations

import logging
import os
import tempfile
from pathlib import Path


log = logging.getLogger(__name__)


def _candidate_dirs() -> list[Path]:
    """
    Ordered list of directories to try for persistent bot data.
    Preference order:
      1. Explicit YURIBOT_DATA_DIR environment variable (if set)
      2. ./data relative to the working directory
      3. XDG_DATA_HOME/yuribot (if XDG_DATA_HOME is set)
      4. A temp directory (system temp dir)/yuribot
    """
    candidates: list[Path] = []

    env_dir = os.getenv("YURIBOT_DATA_DIR")
    if env_dir:
        candidates.append(Path(env_dir))

    candidates.append(Path.cwd() / "data")

    xdg_data = os.getenv("XDG_DATA_HOME")
    if xdg_data:
        candidates.append(Path(xdg_data) / "yuribot")

    tmp_root = os.getenv("XDG_RUNTIME_DIR") or tempfile.gettempdir()
    candidates.append(Path(tmp_root) / "yuribot")

    # Remove duplicates while preserving order
    seen: set[Path] = set()
    unique: list[Path] = []
    for path in candidates:
        try:
            key = path.resolve()
        except OSError:
            key = path
        if key not in seen:
            seen.add(key)
            unique.append(path)
    return unique


def _first_writable_dir(candidates: list[Path]) -> Path | None:
    marker_name = ".yuribot_write_test"
    for base in candidates:
        try:
            base.mkdir(parents=True, exist_ok=True)
        except OSError:
            # Can't even create the directory, try next candidate
            continue
        marker = base / marker_name
        try:
            marker.write_text("", encoding="utf-8")
            marker.unlink(missing_ok=True)
            return base
        except OSError:
            # Not writable, move on
            continue
    return None


def _ensure_file_writable(path: Path) -> bool:
    """
    Try to open the path for append to confirm we can write. Returns True on success.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("ab"):
            pass
        return True
    except OSError:
        return False


def resolve_data_dir(*parts: str, force_temp: bool = False) -> Path:
    """
    Return a writable directory for Yuribot data. Optional *parts allow
    callers to request a named subdirectory.
    """
    if force_temp:
        writable_dir = Path(tempfile.gettempdir()) / "yuribot"
        try:
            writable_dir.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            log.warning("Failed to ensure temp data directory %s: %s", writable_dir, exc)
    else:
        candidates = _candidate_dirs()
        writable_dir = _first_writable_dir(candidates)
        if writable_dir is None:
            log.warning("No persistent data directory writable; falling back to temp directory")
            return resolve_data_dir(*parts, force_temp=True)

    target = writable_dir.joinpath(*parts) if parts else writable_dir
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as exc:
        log.warning("Failed to ensure data directory %s: %s", target, exc)
    return target


def resolve_data_file(filename: str) -> Path:
    """
    Determine a writable location for a persistent data file.

    - Prefers directories earlier in _candidate_dirs().
    - If the file already exists in any candidate directory, that version will
      be copied across to the first writable directory so the bot can continue
      without losing state.
    - Falls back to a temporary directory if nothing else is writable.
    """
    rel = Path(filename)
    if rel.is_absolute():
        # Absolute paths are left untouched.
        return rel

    candidates = _candidate_dirs()

    # Look for an existing file we can seed from
    source_path: Path | None = None
    for base in candidates:
        cand = base / rel
        if cand.exists():
            source_path = cand
            break

    target = resolve_data_dir() / rel

    if source_path and source_path != target and not target.exists():
        try:
            target.write_bytes(source_path.read_bytes())
            log.info("Copied data file from %s to writable location %s", source_path, target)
        except OSError as exc:
            log.warning("Failed to copy data file from %s to %s: %s", source_path, target, exc)

    if _ensure_file_writable(target):
        return target

    # Fall back to temp directory
    temp_target = resolve_data_dir(force_temp=True) / rel
    if source_path and source_path.exists() and source_path != temp_target and not temp_target.exists():
        try:
            temp_target.write_bytes(source_path.read_bytes())
            log.info("Copied data file to temp location %s", temp_target)
        except OSError as exc:
            log.warning("Failed to copy data file to temp location %s: %s", temp_target, exc)

    if not _ensure_file_writable(temp_target):
        log.error("Unable to secure writable location for data file %s; using %s but writes may fail", rel, temp_target)

    return temp_target
