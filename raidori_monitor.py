import asyncio
import html
import json
import logging
import os
import re
from collections import OrderedDict
from datetime import datetime, timedelta, timezone

import aiohttp

# Japan Standard Time (JST = UTC+9)
JST = timezone(timedelta(hours=9))
CONFIG_FILE = "config.json"

def load_json(filepath, default):
    if not os.path.exists(filepath):
        return default
    with open(filepath, 'r', encoding='utf-8') as f:
        try:
            return json.load(f)
        except json.JSONDecodeError:
            return default

def write_string_atomic(filepath, data_str):
    tmp_path = f"{filepath}.tmp"
    with open(tmp_path, 'w', encoding='utf-8') as f:
        f.write(data_str)
    os.replace(tmp_path, filepath)

class RaidoriTelegramMonitor:
    
    GRAPHQL_ARTICLE_QUERY = """
    query GetFanclubArticles($userIds: [ID!]!, $first: Uint!) {
      usersByIds(ids: $userIds) {
        publicFanclubArticles(first: $first) {
          edges {
            node {
              id
              title
              firstPublishedAt
              publicScope {
                __typename
                ... on FanclubArticlePublicScopeMinimumAmount {
                  type
                  amount
                }
              }
            }
          }
        }
      }
    }
    """

    def __init__(self, config):
        self.config = config
        self.interval = config.get("CHECK_INTERVAL", 60)
        self.target_users = config.get("TARGET_USERS", [])
        
        self.state_file = config.get("SEED_FILE", "seed_ids.json")
        self.max_history = config.get("MAX_HISTORY", 1000)
        
        self.seed_ids = OrderedDict.fromkeys(load_json(self.state_file, []))
        self.state_io_lock = asyncio.Lock()
        
        # Flags the script to silently log IDs without notifying on the very first run
        self.is_first_run = len(self.seed_ids) == 0
        
        self.telegram_token = config.get("TELEGRAM_TOKEN")
        self.telegram_chat_id = config.get("TELEGRAM_CHAT_ID")
        
        self.cookies = config.get("COOKIES", "").strip()
        if self.cookies.lower().startswith("cookie:"):
            self.cookies = self.cookies[7:].strip()
            
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
            "Referer": "https://raidori.com/",
            "Origin": "https://raidori.com",
            "Cookie": self.cookies,
        }
        
        self.session = None
        self.notification_queue = asyncio.Queue()
        self.creators = {} # Resolved creator cache
        
        logging.basicConfig(
            level=logging.INFO,
            format="[%(asctime)s] %(message)s",
            datefmt="%H:%M:%S",
            force=True
        )

    def _mark_seen(self, a_id):
        self.seed_ids[a_id] = None
        while len(self.seed_ids) > self.max_history:
            self.seed_ids.popitem(last=False)

    async def save_state(self):
        async with self.state_io_lock:
            keys_to_save = list(self.seed_ids.keys())
            json_str = json.dumps(keys_to_save, indent=2)
            await asyncio.to_thread(write_string_atomic, self.state_file, json_str)

    def _resolve_index(self, data, val):
        if isinstance(val, int) and 0 <= val < len(data):
            return data[val]
        return val

    async def telegram_worker(self):
        while True:
            payload = await self.notification_queue.get()
            api_endpoint = "sendMessage" if 'text' in payload else "sendPhoto"
            api_url = f"https://api.telegram.org/bot{self.telegram_token}/{api_endpoint}"
            retries = 0
            max_retries = 3
            
            # Use text fallback if no photo is available
            if not payload.get('photo'):
                payload.pop('photo', None)
                api_endpoint = "sendMessage"
                api_url = f"https://api.telegram.org/bot{self.telegram_token}/{api_endpoint}"
                payload['text'] = payload.pop('caption', '')
                
            try:
                while retries < max_retries:
                    try:
                        async with self.session.post(api_url, json=payload, timeout=60) as resp:
                            if resp.status == 429:
                                retry_after = int(resp.headers.get("Retry-After", 5))
                                logging.warning(f"Telegram 429 Rate Limit. Sleeping {retry_after}s.")
                                await asyncio.sleep(retry_after)
                                retries += 1
                                continue
                            if resp.status >= 500:
                                await asyncio.sleep(5)
                                retries += 1
                                continue
                            resp.raise_for_status()
                            break
                    except asyncio.TimeoutError:
                        await asyncio.sleep(5)
                        retries += 1
                    except aiohttp.ClientError as e:
                        logging.error(f"Telegram ClientError: {e}")
                        break
                if retries >= max_retries:
                    logging.error("Telegram payload failed. Re-queueing with penalty.")
                    await asyncio.sleep(30)
                    await self.notification_queue.put(payload)
            except Exception as e:
                logging.error(f"Telegram Worker Error: {e}")
            finally:
                self.notification_queue.task_done()
                await asyncio.sleep(1.5)

    async def resolve_creator(self, identifier: str):
        if identifier in self.creators:
            return self.creators[identifier]
            
        clean = identifier.lstrip("@")
        graphql_endpoint = "https://api.raidori.com/query"
        query = """
        query ResolveCreator($screenNames: [String!]!) {
          usersByTwitterScreenNames(screenNames: $screenNames) {
            id
            profile { name twitterScreenName iconURL }
          }
        }
        """
        
        try:
            async with self.session.post(
                graphql_endpoint, 
                json={"query": query, "variables": {"screenNames": [clean]}},
                headers=self.headers
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    users = data.get("data", {}).get("usersByTwitterScreenNames", [])
                    if users and users[0].get("id"):
                        u = users[0]
                        prof = u.get("profile") or {}
                        creator_info = {
                            "user_id": str(u["id"]),
                            "display_name": prof.get("name") or clean,
                            "handle": prof.get("twitterScreenName") or clean,
                            "avatar_url": prof.get("iconURL")
                        }
                        self.creators[identifier] = creator_info
                        return creator_info
        except Exception as e:
            logging.error(f"GraphQL Resolve failed for {clean}: {e}")

        # Fallback to HTML data extraction if GraphQL resolves to nothing
        logging.info(f"Fallback resolution via HTML for {clean}...")
        url = f"https://raidori.com/@{clean}" if not clean.isdigit() else f"https://raidori.com/fanclub/user/{clean}/article"
        try:
            async with self.session.get(url, headers=self.headers) as resp:
                resp.raise_for_status()
                text = await resp.text()
                
                # Optimized: Regex extraction replaces BeautifulSoup dependency
                nuxt_match = re.search(r'<script[^>]*id="__NUXT_DATA__"[^>]*>([^<]+)</script>', text)
                
                if nuxt_match:
                    nuxt_data = json.loads(nuxt_match.group(1))
                    for item in nuxt_data:
                        if isinstance(item, dict) and "userId" in item and "userName" in item:
                            creator_info = {
                                "user_id": str(self._resolve_index(nuxt_data, item["userId"])),
                                "display_name": str(self._resolve_index(nuxt_data, item["userName"])),
                                "handle": str(self._resolve_index(nuxt_data, item.get("handleId", clean))),
                                "avatar_url": str(self._resolve_index(nuxt_data, item.get("iconUrl", ""))) or None,
                            }
                            self.creators[identifier] = creator_info
                            return creator_info

                id_match = re.search(r"UserProfileSection:clientInteraction:(\d+)", text)
                if not id_match:
                    id_match = re.search(r"/fanclub/user/(\d+)/", text)
                    
                if id_match:
                    title_match = re.search(r'<title>([^<]+)</title>', text)
                    raw_title = title_match.group(1).split("さん")[0].strip() if title_match else clean
                    creator_info = {
                        "user_id": id_match.group(1),
                        "display_name": raw_title,
                        "handle": clean,
                        "avatar_url": None
                    }
                    self.creators[identifier] = creator_info
                    return creator_info
        except Exception as e:
            logging.error(f"HTML Resolve failed for {clean}: {e}")
            
        return None

    async def fetch_article_thumbnail(self, creator, article_id):
        url = f"https://raidori.com/fanclub/user/{creator['user_id']}/article/{article_id}"
        try:
            async with self.session.get(url, headers=self.headers) as resp:
                if resp.status != 200:
                    return None
                text = await resp.text()
                # Optimized: Regex extraction replaces BeautifulSoup dependency
                match = re.search(r'<meta[^>]*property=["\']og:image["\'][^>]*content=["\']([^"\']+)["\']', text)
                if match:
                    return re.sub(r"/cdn-cgi/image/[^/]+/", "/", match.group(1))
        except Exception as e:
            logging.error(f"Thumbnail fetch failed for {article_id}: {e}")
        return None

    async def process_creator_articles(self, username):
        creator = await self.resolve_creator(username)
        if not creator:
            logging.warning(f"Could not resolve user profile: {username}")
            return
            
        try:
            payload = {
                "operationName": "GetFanclubArticles",
                "query": self.GRAPHQL_ARTICLE_QUERY,
                "variables": {"userIds": [creator["user_id"]], "first": 3}
            }
            async with self.session.post("https://api.raidori.com/query", json=payload, headers=self.headers) as resp:
                resp.raise_for_status()
                data = await resp.json()
                
                users = data.get("data", {}).get("usersByIds", [])
                if not users or not users[0]:
                    return
                    
                edges = users[0].get("publicFanclubArticles", {}).get("edges", [])
                state_modified = False
                
                # Reverse iteration routes the oldest retrieved item into the queue first
                for edge in reversed(edges):
                    node = edge.get("node", {})
                    art_id = str(node.get("id"))
                    
                    if not art_id or art_id in self.seed_ids:
                        continue
                        
                    # Silently seed the database on the very first run without fetching thumbnails or notifying
                    if self.is_first_run:
                        self._mark_seen(art_id)
                        state_modified = True
                        continue
                        
                    title = node.get("title", "Unknown Title")
                    published_at = node.get("firstPublishedAt", 0)
                    
                    price = 0
                    scope = node.get("publicScope", {})
                    if isinstance(scope, dict) and scope.get("type") == "MinimumAmount":
                        price = scope.get("amount", 0)
                        
                    thumbnail_url = await self.fetch_article_thumbnail(creator, art_id)
                    thumbnail_url = thumbnail_url or creator.get("avatar_url", "")
                    
                    dt = datetime.fromtimestamp(published_at, tz=JST) if published_at > 0 else datetime.now(JST)
                    time_str = f" | Time: {dt.strftime('%Y-%m-%d %H:%M')}"
                    
                    price_str = f"{price:,} JPY" if price > 0 else "FREE"
                    article_url = f"https://raidori.com/fanclub/user/{creator['user_id']}/article/{art_id}"
                    
                    caption_parts = [
                        f"<b>[RAIDORI ARTICLE]{time_str}</b>",
                        f"{html.escape(title)} — {price_str}",
                        html.escape(creator['display_name']),
                        article_url
                    ]
                    
                    notif_payload = {
                        'chat_id': self.telegram_chat_id,
                        'photo': thumbnail_url,
                        'caption': '\n'.join(caption_parts),
                        'parse_mode': 'HTML'
                    }
                    
                    logging.info(f"Queuing Telegram notification: {title}")
                    await self.notification_queue.put(notif_payload)
                    
                    self._mark_seen(art_id)
                    state_modified = True
                    
                if state_modified:
                    await self.save_state()
                    
        except Exception as e:
            logging.error(f"Error fetching article batch for {username}: {e}")

    async def monitoring_worker(self):
        while True:
            for target in self.target_users:
                await self.process_creator_articles(target)
                await asyncio.sleep(1)
                
            if self.is_first_run:
                self.is_first_run = False
                logging.info(f"Initial ID seeding complete. Seeded {len(self.seed_ids)} items.")
                
            await asyncio.sleep(self.interval)

    async def run(self):
        logging.info(f"Raidori Telegram Monitor established. Interval cycle: {self.interval}s")
        async with aiohttp.ClientSession() as session:
            self.session = session
            tasks = [
                asyncio.create_task(self.telegram_worker()),
                asyncio.create_task(self.monitoring_worker())
            ]
            await asyncio.gather(*tasks)

async def main():
    config = load_json(CONFIG_FILE, {})
    if not config:
        logging.error("Configuration file missing. Generating template config.json...")
        template = {
            "CHECK_INTERVAL": 300,
            "TARGET_USERS": ["username1", "12345"],
            "COOKIES": "",
            "TELEGRAM_TOKEN": "YOUR_TELEGRAM_BOT_TOKEN",
            "TELEGRAM_CHAT_ID": "-100YOUR_CHAT_ID",
            "SEED_FILE": "seed_ids.json"
        }
        write_string_atomic(CONFIG_FILE, json.dumps(template, indent=2))
        return
        
    monitor = RaidoriTelegramMonitor(config)
    await monitor.run()

if __name__ == '__main__':
    asyncio.run(main())