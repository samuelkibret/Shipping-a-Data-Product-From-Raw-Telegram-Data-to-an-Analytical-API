import os
import json
from datetime import datetime, timedelta
from telethon.sync import TelegramClient
from telethon.tl.functions.messages import GetHistoryRequest
from telethon.tl.types import MessageMediaPhoto
from dotenv import load_dotenv
import logging
from tqdm import tqdm

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('scraper.log')
    ]
)
logger = logging.getLogger(__name__)

# --- Load Environment Variables ---
load_dotenv()
API_ID = os.getenv('TELEGRAM_API_ID')
API_HASH = os.getenv('TELEGRAM_API_HASH')

SESSION_NAME = 'telegram_scraper_session'

# --- Target Channels ---
CHANNELS = [
    'lobelia4cosmetics',
    'tikvahpharma',
]

# --- Data Paths ---
BASE_DATA_LAKE_PATH = 'data/raw/telegram_messages'
IMAGE_DOWNLOAD_DIR = os.path.join(BASE_DATA_LAKE_PATH, 'images')

# --- JSON Encoder ---
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# --- Sanitize Message for JSON ---
def sanitize_dict(obj, path='root'):
    if isinstance(obj, dict):
        clean = {}
        for k, v in obj.items():
            if isinstance(v, bytes):
                logger.debug(f"Removed bytes at {path}.{k}")
                continue
            clean[k] = sanitize_dict(v, path=f"{path}.{k}")
        return clean
    elif isinstance(obj, list):
        return [sanitize_dict(v, path=f"{path}[]") for v in obj]
    return obj

# --- Get Telegram Client ---
async def get_telegram_client():
    if not API_ID or not API_HASH:
        raise ValueError("Missing TELEGRAM_API_ID or TELEGRAM_API_HASH")
    client = TelegramClient(SESSION_NAME, int(API_ID), API_HASH)
    await client.start()
    logger.info("Connected to Telegram")
    return client

# --- Scrape Messages from Channel ---
async def scrape_channel(client, channel_username, limit=500, days_back=30):
    logger.info(f"Scraping @{channel_username}")
    collected = []
    offset_id = 0
    start_date = datetime.now() - timedelta(days=days_back)
    stop_scraping = False
    os.makedirs(IMAGE_DOWNLOAD_DIR, exist_ok=True)

    entity = await client.get_entity(channel_username)
    pbar = None

    while True:
        history = await client(GetHistoryRequest(
            peer=entity,
            offset_id=offset_id,
            offset_date=None,
            add_offset=0,
            limit=limit,
            max_id=0,
            min_id=0,
            hash=0
        ))

        messages = history.messages
        if not messages:
            break

        if pbar is None:
            pbar = tqdm(total=len(messages), unit="msg", desc=f"@{channel_username}")

        for message in messages:
            if message.date.replace(tzinfo=None) < start_date:
                stop_scraping = True
                break

            raw_dict = message.to_dict()
            msg_dict = sanitize_dict(raw_dict)
            msg_dict['channel_username'] = channel_username

            if message.media and isinstance(message.media, MessageMediaPhoto):
                try:
                    image_filename = f"{channel_username}_{message.id}_{message.media.photo.id}.jpg"
                    image_path = os.path.join(IMAGE_DOWNLOAD_DIR, image_filename)

                    if not os.path.exists(image_path):
                        downloaded_path = await client.download_media(message, file=image_path)
                        if downloaded_path:
                            msg_dict['image_download_path'] = downloaded_path
                    else:
                        msg_dict['image_download_path'] = image_path

                except Exception as e:
                    logger.warning(f"Image download error for msg {message.id}: {e}")

            collected.append(msg_dict)
            pbar.update(1)

        offset_id = messages[-1].id
        if stop_scraping or len(messages) < limit:
            break

    if pbar:
        pbar.close()

    logger.info(f"Scraped {len(collected)} messages from @{channel_username}")
    return collected

# --- Save to Channel Folder (One JSON per Channel) ---
def save_channel_data(data, channel_username):
    if not data:
        logger.warning(f"No data to save for @{channel_username}")
        return

    today_str = datetime.now().strftime('%Y-%m-%d')
    output_dir = os.path.join(BASE_DATA_LAKE_PATH, today_str, channel_username)
    os.makedirs(output_dir, exist_ok=True)

    json_path = os.path.join(output_dir, f"{channel_username}.json")

    try:
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False, cls=DateTimeEncoder)
        logger.info(f"Saved {len(data)} messages to {json_path}")
    except Exception as e:
        logger.error(f"Error saving JSON for @{channel_username}: {e}")

# --- Main ---
async def main():
    client = None
    try:
        client = await get_telegram_client()

        for channel in tqdm(CHANNELS, desc="Scraping Channels"):
            messages = await scrape_channel(client, channel, limit=500, days_back=30)
            save_channel_data(messages, channel)

    except Exception as e:
        logger.critical(f"Fatal error: {e}")
    finally:
        if client:
            await client.disconnect()
            logger.info("Disconnected from Telegram")

# --- Run ---
if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
