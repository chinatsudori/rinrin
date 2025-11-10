
from typing import Optional, Dict, Any, List, Set
import os, time
from fastapi import APIRouter, Request, Depends, HTTPException
from fastapi.responses import RedirectResponse
from starlette.status import HTTP_302_FOUND
from authlib.integrations.starlette_client import OAuth
import httpx

DISCORD_CLIENT_ID = os.getenv("DISCORD_CLIENT_ID", "")
DISCORD_CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "")
DISCORD_REDIRECT_URI = os.getenv("DISCORD_REDIRECT_URI", "http://localhost:5780/auth/callback")
DISCORD_GUILD_ID = os.getenv("DISCORD_GUILD_ID", "")
DISCORD_BOT_TOKEN = os.getenv("DISCORD_BOT_TOKEN", "")
ALLOWED_ROLE_IDS = set([x.strip() for x in os.getenv("ALLOWED_ROLE_IDS", "").split(",") if x.strip()])
ALLOWED_ROLE_NAMES = set([x.strip() for x in os.getenv("ALLOWED_ROLE_NAMES", "").split(",") if x.strip()])

router = APIRouter(prefix="/auth", tags=["auth"])
oauth = OAuth()
oauth.register(
    name="discord",
    client_id=DISCORD_CLIENT_ID,
    client_secret=DISCORD_CLIENT_SECRET,
    server_metadata_url="https://discord.com/.well-known/openid-configuration",
    client_kwargs={"scope": "identify guilds.members.read"},
)
DISCORD_API = "https://discord.com/api/v10"

async def get_guild_roles(client: httpx.AsyncClient, guild_id: str):
    if not hasattr(get_guild_roles, "_cache"):
        get_guild_roles._cache = {}
    cache = get_guild_roles._cache
    key = f"roles:{guild_id}"
    if key in cache and time.time() - cache[key]["ts"] < 300:
        return cache[key]["data"]
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    r = await client.get(f"{DISCORD_API}/guilds/{guild_id}/roles", headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()
    mapping = {str(role["id"]): role["name"] for role in data}
    cache[key] = {"ts": time.time(), "data": mapping}
    return mapping

async def get_member_role_ids(client: httpx.AsyncClient, guild_id: str, user_id: str):
    headers = {"Authorization": f"Bot {DISCORD_BOT_TOKEN}"}
    r = await client.get(f"{DISCORD_API}/guilds/{guild_id}/members/{user_id}", headers=headers, timeout=15)
    if r.status_code == 404:
        return []
    r.raise_for_status()
    data = r.json()
    return [str(x) for x in data.get("roles", [])]

async def fetch_userinfo_via_token(request: Request):
    token = request.session.get("token")
    if not token:
        raise HTTPException(401, "Not authenticated")
    async with httpx.AsyncClient() as client:
        r = await client.get(f"{DISCORD_API}/users/@me", headers={"Authorization": f"Bearer {token['access_token']}"}, timeout=15)
        r.raise_for_status()
        return r.json()

@router.get("/login")
async def login(request: Request):
    return await oauth.discord.authorize_redirect(request, DISCORD_REDIRECT_URI)

@router.get("/callback")
async def auth_callback(request: Request):
    token = await oauth.discord.authorize_access_token(request)
    request.session["token"] = token
    user = await fetch_userinfo_via_token(request)
    request.session["user"] = {
        "id": user["id"],
        "username": user.get("username"),
        "global_name": user.get("global_name"),
        "discriminator": user.get("discriminator"),
        "avatar": user.get("avatar"),
    }
    # roles via bot
    if DISCORD_GUILD_ID and DISCORD_BOT_TOKEN:
        async with httpx.AsyncClient() as client:
            try:
                role_ids = await get_member_role_ids(client, DISCORD_GUILD_ID, user["id"])
                roles_map = await get_guild_roles(client, DISCORD_GUILD_ID)
                request.session["roles"] = {"ids": role_ids, "names": [roles_map.get(r, r) for r in role_ids]}
            except Exception:
                request.session["roles"] = {"ids": [], "names": []}
    else:
        request.session["roles"] = {"ids": [], "names": []}
    return RedirectResponse(url="/", status_code=HTTP_302_FOUND)

@router.get("/logout")
async def logout(request: Request):
    request.session.clear()
    return RedirectResponse(url="/", status_code=HTTP_302_FOUND)

async def current_user(request: Request):
    return request.session.get("user")

def require_auth():
    async def dep(request: Request):
        if not request.session.get("user"):
            raise HTTPException(401, "Login required")
    return dep

def require_roles(required=None, required_ids=None):
    role_names_env = ALLOWED_ROLE_NAMES if required is None else set(required)
    role_ids_env = ALLOWED_ROLE_IDS if required_ids is None else set(str(x) for x in (required_ids or []))
    async def dep(request: Request):
        user = request.session.get("user")
        if not user:
            raise HTTPException(401, "Login required")
        roles = request.session.get("roles") or {}
        user_role_ids = set(roles.get("ids", []))
        user_role_names = set(roles.get("names", []))
        if not role_names_env and not role_ids_env:
            return
        if not (user_role_ids & role_ids_env or user_role_names & role_names_env):
            raise HTTPException(403, "Insufficient role")
    return dep
