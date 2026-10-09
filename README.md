[![AI Generated](https://img.shields.io/badge/AI_Generated-Gemini-blue.svg)](https://gemini.google.com)
## Acknowledgments

* **Code Generation:** The core logic and boilerplate for this project were generated using Gemini. All AI-generated code was subsequently reviewed and tested.
# raidori-telegram-monitor
Asynchronous Python monitor for Raidori fanclubs. Tracks new articles via GraphQL and dispatches real-time Telegram notifications.

## Features
* **GraphQL Native:** Bypasses heavy HTML DOM scraping by querying Raidori's internal API directly.
* **Asynchronous:** Utilizes `aiohttp` and `asyncio` for non-blocking I/O operations.
* **Silent Bootstrapping:** Intelligently caches existing articles on the first run without triggering notification spam.
* **Low Footprint:** Stripped of heavy parsing libraries (like BeautifulSoup) in favor of targeted regex and native JSON processing.

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/shmilyhua/raidori-telegram-monitor.git
   cd raidori-telegram-monitor
   ```

2. This project uses `uv` for dependency management. Install the dependencies:
   ```bash
   uv venv --python 3.13
   uv sync
   ```
   *(Alternatively, you can install the dependencies listed in `pyproject.toml` via pip).*

## Configuration

1. Create a `config.json` file in the root directory. This file is ignored by git to protect your credentials.
2. Structure your `config.json` as follows:

```json
{
  "CHECK_INTERVAL": 300,
  "TARGET_USERS": [
    "@username"
  ],
  "COOKIES": "__session_v2=YOUR_RAIDORI_COOKIE_STRING_HERE",
  "TELEGRAM_TOKEN": "your_bot_token",
  "TELEGRAM_CHAT_ID": "your_chat_id",
  "SEED_FILE": "seed_ids.json"
}
```

## Usage

Start the monitor by running:

```bash
uv run raidori_monitor.py
```

The script will create a directory named raidori_archive containing separate folders for each creator, populated with the downloaded assets and the article.html viewer.
