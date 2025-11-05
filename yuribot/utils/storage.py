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

    writable_dir = _first_writable_dir(candidates)
    if writable_dir is None:
        writable_dir = Path(tempfile.mkdtemp(prefix="yuribot-"))
        log.warning("No persistent data directory writable; using temporary dir %s", writable_dir)

    target = writable_dir / rel
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
    except OSError:
        # Best-effort; writes may still fail later but we tried.
        pass

    if source_path and source_path != target and not target.exists():
        try:
            target.write_bytes(source_path.read_bytes())
            log.info("Copied data file from %s to writable location %s", source_path, target)
        except OSError as exc:
            log.warning("Failed to copy data file from %s to %s: %s", source_path, target, exc)

    return target
