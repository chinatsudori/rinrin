# yuribot — simplified design

**What it does**

- Announcements → an **announcements** text channel.
- Submissions → a **planning** forum. The thread title is the submission title. The bot scrapes the **first URL** from the starter message as the link. During an **open collection window**, any new forum post auto-registers as a submission.
- Polls → a **polls** text channel. Council passes numbers (from `/club list_current_submissions`) into `/club create_poll numbers:"1,3,4"`. No shortlists.
- Discussions → assume **Fridays**. After picking the winning series, `/club plan_discussions total_chapters:X chapters_per_section:Y [hour:HH]` divides chapters accordingly and creates **Discord Scheduled Events** on successive Fridays at that hour (local TZ).

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

