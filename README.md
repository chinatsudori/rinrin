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

### Configuration quick reference

| Variable     | Purpose                          | Example                      |
|--------------|----------------------------------|------------------------------|
| `DISCORD_TOKEN` | Bot token (required)             | `xxxxx.yyyyy.zzzzz`          |
| `TZ`         | Timezone for logs/scheduling     | `America/Los_Angeles`        |
| `LOG_LEVEL`  | Logging level                    | `INFO` (default), `DEBUG`    |
| `DATA_DIR`   | Data directory for runtime files | `/app/data`                  |
| `BOT_DB_PATH`| SQLite DB path (single source)   | `/app/data/bot.sqlite3`      |

> `BOT_DB_PATH` is the single source of truth. `DB_PATH` is aliased for backward-compat and will be removed later.

### Guild settings and channel routing

This bot no longer relies on hard-coded channel IDs. Use slash commands to configure per-guild settings:

- `/set_channel key:<name> channel:#channel` — e.g., `key=log_channel` or `key=modlog_channel`
- `/get_setting key:<name>`
- `/list_settings`

At runtime, code that previously did `guild.get_channel(123456789012345678)` now resolves through a shim that prefers the stored setting and falls back to the legacy ID if still present.
