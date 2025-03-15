import os
import requests
import asyncio
import json
import random
import re
from io import BytesIO
from telegram import Bot
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL", "https://2ch.hk/cc/res/229275.json")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
    raise ValueError("Отсутствуют TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID в переменных окружения!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

CHECK_INTERVAL = 60  # Интервал проверки в секундах
SENT_POSTS_FILE = "sent_posts.json"

# Загружаем отправленные посты
try:
    with open(SENT_POSTS_FILE, "r") as f:
        sent_posts = set(json.load(f))
except (FileNotFoundError, json.JSONDecodeError):
    sent_posts = set()

def save_sent_posts():
    with open(SENT_POSTS_FILE, "w") as f:
        json.dump(list(sent_posts), f)

# Функция очистки HTML-тегов
def clean_html(text):
    text = unescape(text)
    text = re.sub(r"<a .*?>(.*?)</a>", r"\1", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<.*?>", "", text)
    return text.strip()

# Получение новых постов
def get_new_posts():
    try:
        response = requests.get(THREAD_URL, timeout=10)
        data = response.json()
        posts = data["threads"][0]["posts"]
        return [p for p in posts if str(p["num"]) not in sent_posts]
    except Exception as e:
        print(f"Ошибка при парсинге: {e}")
        return []

# Функция для публикации в Telegram
async def post_to_telegram():
    global sent_posts
    while True:
        new_posts = get_new_posts()
        if not new_posts:
            print("Новых постов нет. Ждем...")
        else:
            for post in new_posts:
                post_id = str(post["num"])
                text = clean_html(post.get("comment", ""))
                message = f"#{post_id}\n\n{text}"

                try:
                    await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode="HTML")
                    await asyncio.sleep(1.5)
                    
                    sent_posts.add(post_id)
                    save_sent_posts()
                except (RetryAfter, TimedOut, BadRequest) as e:
                    print(f"Ошибка Telegram: {e}")
                    await asyncio.sleep(5)
                    continue
        
        await asyncio.sleep(CHECK_INTERVAL + random.uniform(1, 5))

if __name__ == "__main__":
    asyncio.run(post_to_telegram())