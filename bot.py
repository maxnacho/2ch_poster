import os
import requests
import asyncio
import json
import random
import re
from io import BytesIO
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape
from PIL import Image
import asyncpg

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL", "https://2ch.hk/cc/res/229275.json")
SUPABASE_URL = os.getenv("SUPABASE_URL")  # Строка подключения к Supabase
SENT_POSTS_TABLE = "sent_posts"  # Название таблицы в Supabase

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID or not SUPABASE_URL:
    raise ValueError("Отсутствуют необходимые переменные окружения!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
CHECK_INTERVAL = 15  # Интервал проверки в секундах
MAX_CAPTION_LENGTH = 1024
MAX_MESSAGE_LENGTH = 4096

# Функция для получения списка отправленных постов из Supabase
async def get_sent_posts(pool):
    async with pool.acquire() as conn:
        rows = await conn.fetch(f"SELECT post_id FROM {SENT_POSTS_TABLE}")
        return {row['post_id'] for row in rows}

# Функция для добавления отправленного поста в Supabase
async def add_sent_post(pool, post_id):
    async with pool.acquire() as conn:
        await conn.execute(f"INSERT INTO {SENT_POSTS_TABLE} (post_id) VALUES ($1) ON CONFLICT DO NOTHING", post_id)

def clean_html(text):
    text = unescape(text)
    text = re.sub(r"<a .*?>(.*?)</a>", r"\1", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<.*?>", "", text)
    return text.strip()

def split_text(text, max_length=MAX_MESSAGE_LENGTH):
    return [text[i:i+max_length] for i in range(0, len(text), max_length)]

def validate_and_resize_image(image_data):
    try:
        img = Image.open(BytesIO(image_data))
        if img.mode in ("P", "RGBA", "LA"):
            img = img.convert("RGB")

        width, height = img.size
        if width < 320 or height < 320 or width > 10000 or height > 10000:
            img = img.resize((1280, 720))

        output = BytesIO()
        img.save(output, format="JPEG")
        output.seek(0)
        return output
    except Exception as e:
        print(f"Ошибка обработки изображения: {e}")
        return None

def get_new_posts(sent_posts):
    try:
        response = requests.get(THREAD_URL, timeout=10)
        data = response.json()
        posts = data["threads"][0]["posts"]
        return [p for p in posts if str(p["num"]) not in sent_posts and int(p["num"]) >= MIN_POST_ID]
    except Exception as e:
        print(f"Ошибка при парсинге: {e}")
        return []

async def post_to_telegram():
    pool = await asyncpg.create_pool(SUPABASE_URL)
    sent_posts = await get_sent_posts(pool)
    
    while True:
        new_posts = get_new_posts(sent_posts)
        if not new_posts:
            print("Новых постов нет. Ждем...")
        else:
            for post in new_posts:
                post_id = str(post["num"])
                if post_id in sent_posts:
                    continue
                
                text = clean_html(post.get("comment", ""))
                files = post.get("files", []) or []
                media_group = []
                
                header = f"#{post_id}"
                text = f"{header}\n\n{text}" if text else header
                
                file_links = []
                for file in files:
                    file_url = f"https://2ch.hk{file['path']}"
                    if file["path"].endswith((".webm", ".mp4")):
                        file_links.append(f"🎥 Видео: {file_url}")
                
                if file_links:
                    text += "\n\n" + "\n".join(file_links)
                
                messages = split_text(text)
                
                try:
                    for file in files:
                        file_url = f"https://2ch.hk{file['path']}"
                        response = requests.get(file_url, timeout=10)
                        if response.status_code == 200:
                            if file["path"].endswith((".jpg", ".jpeg", ".png", ".gif")):
                                image_data = validate_and_resize_image(response.content)
                                if image_data:
                                    media_group.append(InputMediaPhoto(media=image_data))
                            elif file["path"].endswith((".webm", ".mp4")):
                                video_data = BytesIO(response.content)
                                media_group.append(InputMediaVideo(media=video_data))
                    
                    if media_group:
                        for i in range(0, len(media_group), 10):
                            chunk = media_group[i:i+10]
                            if i == 0 and messages:
                                chunk[0] = InputMediaPhoto(media=chunk[0].media, caption=messages[0], parse_mode="HTML")
                            await bot.send_media_group(chat_id=TELEGRAM_CHANNEL_ID, media=chunk)
                            await asyncio.sleep(1)
                    
                    for i, message in enumerate(messages):
                        if i == 0 and media_group:
                            continue
                        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode="HTML")
                        await asyncio.sleep(1)
                    
                    await add_sent_post(pool, post_id)
                    sent_posts.add(post_id)
                except (RetryAfter, TimedOut, BadRequest) as e:
                    print(f"Ошибка Telegram: {e}")
                    await asyncio.sleep(5)
                    continue
        
        await asyncio.sleep(CHECK_INTERVAL + random.uniform(1, 5))

if __name__ == "__main__":
    asyncio.run(post_to_telegram())
