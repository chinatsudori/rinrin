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

