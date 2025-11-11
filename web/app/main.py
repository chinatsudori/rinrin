from fastapi import FastAPI, Request, Form, HTTPException, Depends
from fastapi.responses import (
    HTMLResponse,
    JSONResponse,
    RedirectResponse,
    PlainTextResponse,
)
from fastapi.staticfiles import StaticFiles
from jinja2 import Environment, FileSystemLoader, select_autoescape
from pathlib import Path
from starlette.middleware.sessions import SessionMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from . import auth
import os
import sqlite3
from datetime import datetime, timezone
import time
import re

BOT_DB_PATH = os.getenv("BOT_DB_PATH", "/app/data/bot.sqlite3")
LOG_PATH = os.getenv("LOG_PATH", "/app/data/bot.log")
STATIC_DIR = Path(__file__).parent / "static"

CSP = (
    "default-src 'self'; "
    "frame-ancestors https://discord.com https://*.discord.com; "
    "script-src 'self' https://discord.com; "
    "connect-src 'self' https://discord.com https://*.discord.com; "
    "img-src 'self' data: https:; "
    "style-src 'self' 'unsafe-inline'; "
    "base-uri 'self'; form-action 'self'"
)


class SecurityHeaders(BaseHTTPMiddleware):
    async def dispatch(self, request, call_next):
        resp = await call_next(request)
        resp.headers["X-Frame-Options"] = ""
        resp.headers["Content-Security-Policy"] = CSP
        return resp


app = FastAPI(title="Yuri Bot Dashboard", version="0.2.0")
app.add_middleware(
    SessionMiddleware,
    secret_key=os.getenv("SESSION_SECRET", "dev-change-me"),
    max_age=60 * 60 * 8,
    same_site="none",  # allow third-party iframe
    https_only=True,  # cookie requires HTTPS
)
app.mount(
    "/static",
    StaticFiles(directory=str(Path(__file__).parent / "static")),
    name="static",
)
app.include_router(auth.router)

templates_path = Path(__file__).parent / "templates"
env = Environment(
    loader=FileSystemLoader(str(templates_path)), autoescape=select_autoescape()
)


def db_conn():
    if not os.path.exists(BOT_DB_PATH):
        raise FileNotFoundError(f"DB not found at {BOT_DB_PATH}")
    con = sqlite3.connect(BOT_DB_PATH)
    con.row_factory = sqlite3.Row
    return con


def db_status():
    info = {
        "path": BOT_DB_PATH,
        "exists": False,
        "tables": [],
        "size_bytes": None,
        "mtime": None,
    }
    try:
        if os.path.exists(BOT_DB_PATH):
            info["exists"] = True
            info["size_bytes"] = os.path.getsize(BOT_DB_PATH)
            info["mtime"] = datetime.fromtimestamp(
                os.path.getmtime(BOT_DB_PATH)
            ).isoformat()
            con = db_conn()
            cur = con.cursor()
            cur.execute(
                "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
            )
            info["tables"] = [r[0] for r in cur.fetchall()]
            con.close()
    except Exception as e:
        info["error"] = str(e)
    return info


@app.get("/health")
def health():
    return {"ok": True, "service": "web", "time": datetime.utcnow().isoformat() + "Z"}


@app.get("/", response_class=HTMLResponse)
def home(request: Request):
    context = {
        "request": request,
        "db": db_status(),
        "env": {
            "BOT_DB_PATH": BOT_DB_PATH,
            "LOG_PATH": LOG_PATH,
            "TZ": os.getenv("TZ", "America/Los_Angeles"),
        },
    }
    template = env.get_template("index.html")
    return template.render(**context)


# ---------- Console log viewer ----------
@app.get(
    "/admin/logs",
    response_class=HTMLResponse,
    dependencies=[Depends(auth.require_auth())],
)
def logs_page(request: Request, n: int = 400):
    lines = []
    exists = os.path.exists(LOG_PATH)
    if exists:
        try:
            with open(LOG_PATH, "r", errors="ignore") as f:
                lines = f.readlines()[-n:]
        except Exception as e:
            lines = [f"[error reading log] {e}\\n"]
    template = env.get_template("logs.html")
    return template.render(
        request=request, log_path=LOG_PATH, exists=exists, lines=lines, n=n
    )


# ---------- DB viewer/editor ----------
@app.get(
    "/admin/db",
    response_class=HTMLResponse,
    dependencies=[Depends(auth.require_auth())],
)
def db_tables(request: Request):
    status = db_status()
    tables = status.get("tables", [])
    template = env.get_template("db.html")
    return template.render(request=request, tables=tables, status=status)


@app.get("/admin/db/table/{name}", response_class=HTMLResponse)
def db_table_view(request: Request, name: str, limit: int = 100, offset: int = 0):
    con = db_conn()
    cur = con.cursor()
    try:
        cur.execute(f"PRAGMA table_info({name})")
        cols = [r["name"] for r in cur.fetchall()]
        cur.execute(f"SELECT * FROM {name} LIMIT ? OFFSET ?", (limit, offset))
        rows = [dict(r) for r in cur.fetchall()]
        # Try to detect PK
        cur.execute(f"PRAGMA table_info({name})")
        pragma = cur.fetchall()
        pk_cols = [r["name"] for r in pragma if r["pk"]]
    finally:
        con.close()
    template = env.get_template("db_table.html")
    return template.render(
        request=request,
        name=name,
        cols=cols,
        rows=rows,
        limit=limit,
        offset=offset,
        pk_cols=pk_cols,
    )


@app.post("/admin/db/table/{name}/update")
def db_table_update(
    name: str, id: str = Form(...), column: str = Form(...), value: str = Form(...)
):
    con = db_conn()
    cur = con.cursor()
    # Try PK detection, fallback to ROWID
    cur.execute(f"PRAGMA table_info({name})")
    pragma = cur.fetchall()
    pk_cols = [r["name"] for r in pragma if r["pk"]]
    try:
        if pk_cols:
            pk = pk_cols[0]
            cur.execute(f"UPDATE {name} SET {column}=? WHERE {pk}=?", (value, id))
        else:
            cur.execute(f"UPDATE {name} SET {column}=? WHERE ROWID=?", (value, id))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url=f"/admin/db/table/{name}", status_code=303)


@app.post("/admin/db/table/{name}/insert")
def db_table_insert(request: Request, name: str):
    # Collect dynamic form fields
    form = request._form  # not populated yet
    # FastAPI workaround: use Request.form()
    import anyio

    async def handle():
        data = await request.form()
        cols = []
        vals = []
        for k, v in data.items():
            if k.startswith("col:"):
                cols.append(k.split(":", 1)[1])
                vals.append(v)
        placeholders = ",".join(["?"] * len(cols))
        q = f"INSERT INTO {name} ({','.join(cols)}) VALUES ({placeholders})"
        con = db_conn()
        cur = con.cursor()
        try:
            cur.execute(q, vals)
            con.commit()
        finally:
            con.close()
        return RedirectResponse(url=f"/admin/db/table/{name}", status_code=303)

    return anyio.from_thread.run(handle)


@app.post("/admin/db/table/{name}/delete")
def db_table_delete(name: str, id: str = Form(...)):
    con = db_conn()
    cur = con.cursor()
    cur.execute(f"PRAGMA table_info({name})")
    pragma = cur.fetchall()
    pk_cols = [r["name"] for r in pragma if r["pk"]]
    try:
        if pk_cols:
            pk = pk_cols[0]
            cur.execute(f"DELETE FROM {name} WHERE {pk}=?", (id,))
        else:
            cur.execute(f"DELETE FROM {name} WHERE ROWID=?", (id,))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url=f"/admin/db/table/{name}", status_code=303)


# ---------- Birthday viewer/editor ----------
@app.get("/admin/birthdays", response_class=HTMLResponse)
def birthdays_page(request: Request):
    con = db_conn()
    cur = con.cursor()
    err = None
    rows = []
    try:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='birthdays'"
        )
        if cur.fetchone():
            cur.execute("PRAGMA table_info(birthdays)")
            cols = [r["name"] for r in cur.fetchall()]
            # try common columns
            q = "SELECT * FROM birthdays ORDER BY day"
            cur.execute(q)
            rows = [dict(r) for r in cur.fetchall()]
        else:
            cols = []
            err = "Table 'birthdays' not found. Expected columns: id, user, day (YYYY-MM-DD), closeness (INT)."
    finally:
        con.close()
    template = env.get_template("birthdays.html")
    return template.render(request=request, rows=rows, err=err)


@app.post("/admin/birthdays/upsert")
def birthdays_upsert(
    id: str = Form(None),
    user: str = Form(...),
    day: str = Form(...),
    closeness: int = Form(0),
):
    con = db_conn()
    cur = con.cursor()
    try:
        if id and id.strip():
            cur.execute(
                "UPDATE birthdays SET user=?, day=?, closeness=? WHERE id=?",
                (user, day, closeness, id),
            )
        else:
            cur.execute(
                "INSERT INTO birthdays(user, day, closeness) VALUES (?,?,?)",
                (user, day, closeness),
            )
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/birthdays", status_code=303)


@app.post("/admin/birthdays/delete")
def birthdays_delete(id: int = Form(...)):
    con = db_conn()
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM birthdays WHERE id=?", (id,))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/birthdays", status_code=303)


# ---------- Booly editor ----------
@app.get(
    "/admin/booly",
    response_class=HTMLResponse,
    dependencies=[Depends(auth.require_auth())],
)
def booly_page(request: Request):
    con = db_conn()
    cur = con.cursor()
    err = None
    rows = []
    try:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='booly'"
        )
        if cur.fetchone():
            cur.execute("SELECT key, value FROM booly ORDER BY key")
            rows = [dict(r) for r in cur.fetchall()]
        else:
            err = "Table 'booly' not found. Expected schema: key TEXT PRIMARY KEY, value INTEGER (0/1)."
    finally:
        con.close()
    template = env.get_template("booly.html")
    return template.render(request=request, rows=rows, err=err)


@app.post("/admin/booly/upsert")
def booly_upsert(key: str = Form(...), value: int = Form(0)):
    con = db_conn()
    cur = con.cursor()
    try:
        cur.execute(
            "INSERT INTO booly(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, value),
        )
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/booly", status_code=303)


@app.post("/admin/booly/delete")
def booly_delete(key: str = Form(...)):
    con = db_conn()
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM booly WHERE key=?", (key,))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/booly", status_code=303)


# ---------- /mu status viewer ----------
@app.get(
    "/admin/mu",
    response_class=HTMLResponse,
    dependencies=[Depends(auth.require_auth())],
)
def mu_page(request: Request):
    con = db_conn()
    cur = con.cursor()
    err = None
    rows = []
    try:
        cur.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='mu_status'"
        )
        if cur.fetchone():
            cur.execute("SELECT * FROM mu_status ORDER BY updated_at DESC")
            rows = [dict(r) for r in cur.fetchall()]
        else:
            err = "Table 'mu_status' not found. Expected columns: id, user, status, updated_at."
    finally:
        con.close()
    template = env.get_template("mu.html")
    return template.render(request=request, rows=rows, err=err)


@app.post("/admin/mu/upsert")
def mu_upsert(id: str = Form(None), user: str = Form(...), status: str = Form(...)):
    now = datetime.now(timezone.utc).isoformat()
    con = db_conn()
    cur = con.cursor()
    try:
        if id and id.strip():
            cur.execute(
                "UPDATE mu_status SET user=?, status=?, updated_at=? WHERE id=?",
                (user, status, now, id),
            )
        else:
            cur.execute(
                "INSERT INTO mu_status(user, status, updated_at) VALUES (?,?,?)",
                (user, status, now),
            )
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/mu", status_code=303)


@app.post("/admin/mu/delete")
def mu_delete(id: int = Form(...)):
    con = db_conn()
    cur = con.cursor()
    try:
        cur.execute("DELETE FROM mu_status WHERE id=?", (id,))
        con.commit()
    finally:
        con.close()
    return RedirectResponse(url="/admin/mu", status_code=303)


# ----------- Activity entry -----------------
@app.get("/activity", response_class=HTMLResponse)
def activity_entry(request: Request):
    template = env.get_template("activity.html")
    return template.render(
        client_id=os.getenv("DISCORD_CLIENT_ID", ""),
        redirect_uri=os.getenv(
            "DISCORD_REDIRECT_URI", "https://yuri.icebrand.dev/auth/callback"
        ),
        next_after_login="/admin",  # or another post-login page
    )


# ---------- Timestamp tool ----------
@app.get("/admin/timestamp", response_class=HTMLResponse)
def timestamp_page(request: Request, ts: str = ""):
    result = None
    error = None
    if ts:
        try:
            if re.match(r"^\d+(\.\d+)?$", ts):
                # epoch seconds
                dt = datetime.fromtimestamp(float(ts), tz=timezone.utc)
                result = {
                    "input": ts,
                    "iso": dt.isoformat(),
                    "local": dt.astimezone().isoformat(),
                }
            else:
                # parse ISO -> epoch
                # very basic parse
                dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                epoch = dt.timestamp()
                result = {"input": ts, "epoch_seconds": epoch}
        except Exception as e:
            error = str(e)
    template = env.get_template("timestamp.html")
    return template.render(request=request, ts=ts, result=result, error=error)


@app.post("/admin/timestamp/now")
def timestamp_now():
    now = datetime.now(timezone.utc)
    return JSONResponse(
        {
            "epoch_seconds": now.timestamp(),
            "iso": now.isoformat(),
            "local": now.astimezone().isoformat(),
        }
    )
