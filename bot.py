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

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL", "https://2ch.hk/cc/res/229275.json")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
    raise ValueError("Отсутствуют TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID в переменных окружения!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)

CHECK_INTERVAL = 60  # Интервал проверки в секундах
SENT_POSTS_FILE = "sent_posts.json"
MAX_CAPTION_LENGTH = 1024
MAX_MESSAGE_LENGTH = 4096

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

# Функция разбиения длинных сообщений
def split_text(text, max_length=MAX_MESSAGE_LENGTH):
    return [text[i:i+max_length] for i in range(0, len(text), max_length)]

# Обработка изображений
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
                files = post.get("files", []) or []
                media_group = []

                # Добавляем заголовок
                header = f"#{post_id}"
                parent_id = post.get("parent")
                if parent_id and parent_id != "229275":
                    header += f"\nОтвет на #{parent_id}"
                
                text = f"{header}\n\n{text}"
                messages = split_text(text)

                try:
                    # Обрабатываем медиафайлы
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
                            await asyncio.sleep(1.5)

                    for i, message in enumerate(messages):
                        if i == 0 and media_group:
                            continue  # Первая часть уже отправлена с медиа
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