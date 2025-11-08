from __future__ import annotations
from pathlib import Path
import os
from dateutil import tz

DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)


TZ_NAME = os.getenv("TZ", "UTC")
TZ = tz.gettz(TZ_NAME)
LOCAL_TZ = TZ  # backward-compat alias

BOT_DB_PATH = os.getenv("BOT_DB_PATH", os.path.join(DATA_DIR, "bot.sqlite3"))
DB_PATH = BOT_DB_PATH  # alias for backward-compat
