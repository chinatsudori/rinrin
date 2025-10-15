from __future__ import annotations
import os
from dateutil import tz

DATA_DIR = os.getenv("DATA_DIR", "./data")
os.makedirs(DATA_DIR, exist_ok=True)

DB_PATH = os.path.join(DATA_DIR, "yuribot.sqlite3")

TZ_NAME = os.getenv("TZ", "UTC")
LOCAL_TZ = tz.gettz(TZ_NAME)
