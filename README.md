# yuribot — simplified design

**What it does**

blah blah blah stuff stuff

**Prereqs**

- Python 3.11+
- `pip install -r requirements.txt`
- Enable the following in the **Discord Developer Portal** for your bot:
  - **GUILDS intent** (on)
  - **MESSAGE CONTENT intent** (on) ← required to read forum starter post content for the link
- Bot Permissions:
  - View Channels, Send Messages, Manage Messages
  - Create Public Threads, Send Messages in Threads
  - **Manage Events** (to create scheduled events)
  - Use Application Commands

**Run**

- Set env: `DISCORD_TOKEN=...`, optional `TZ=America/Los_Angeles`, `DATA_DIR=/app/data` if Docker.
- `python -m yuribot`

**Music / Lavalink**

- The new music cog uses [Lavalink](https://github.com/freyacodes/Lavalink) through Wavelink.
- Provide a node via `LAVALINK_URL` (for example `http://localhost:2333`) and `LAVALINK_PASSWORD`.
- Optional env: `LAVALINK_NAME`, `LAVALINK_RESUME_KEY`, `MUSIC_MAX_BITRATE` (defaults to 384000 bps for 384 kbps voice channels).
- To allow Spotify links, set `SPOTIFY_CLIENT_ID` and `SPOTIFY_CLIENT_SECRET` (plus optional `SPOTIFY_MAX_TRACKS` to limit how many tracks a playlist import will enqueue; defaults to 100).
- Commands: `/play`, `/pause`, `/skip`, `/queue`, `/controller`, and `/playlist <list|save|load|delete>`.
- Playlists are saved per-guild in `yuribot/data/music_playlists.json`.

