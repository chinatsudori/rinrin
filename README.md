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

**Akinator datasets**

- The clone ships with a tiny curated data set meant purely for demos.
- Set `AKINATOR_DATA_URL` to point at a remote JSON document if you want to pull a larger or regularly updated dataset. The remote document must follow the same structure used in `yuribot/data/akinator_sets.py`.
- Override `AKINATOR_DEFAULT_SET` and `AKINATOR_YURI_SET` if the remote document exposes additional named sets that should become the defaults.
- Need more Yuri-flavoured data? Run `python yuribot/data/build_yuri_dataset.py --pages 3 --per-page 25 --limit 80 --output my_yuri_dataset.json` to call the AniList GraphQL API and emit a fresh dataset keyed as `"yuri_remote"`. Host the resulting JSON (for example with `python -m http.server`) and point `AKINATOR_DATA_URL` at it.
- The script requires outbound internet access. When working offline you can still use `yuribot/data/yuri_remote_dataset.sample.json`, which contains a snapshot generated with the same question set so you can test the remote loader locally.

