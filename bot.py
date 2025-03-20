import os
import requests
import asyncio
import json
import random
import re
import logging
from io import BytesIO
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape
from PIL import Image
from aiohttp import web

# Настройка логирования
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Переменные окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID or not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("Отсутствуют необходимые переменные окружения!")

# Настройка заголовков для REST API
headers = {
    "apikey": SUPABASE_KEY,
    "Content-Type": "application/json",
}
base_url = f"{SUPABASE_URL}/rest/v1"

# Функция для проверки, отправлен ли пост
def is_post_sent(post_id):
    url = f"{base_url}/sent_posts?select=post_id&post_id=eq.{post_id}"
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        data = response.json()
        return len(data) > 0
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка проверки поста {post_id}: {e}")
        return False  # Если ошибка, считаем, что пост не отправлен

# Функция для добавления отправленного поста в базу данных
def add_sent_post(post_id):
    url = f"{base_url}/sent_posts"
    data = {"post_id": post_id}
    try:
        response = requests.post(url, headers=headers, json=data)
        if response.status_code == 201:
            logger.info(f"Post {post_id} successfully added to database")
            return True
        elif response.status_code == 400:
            error_data = response.json()
            if "message" in error_data and "unique constraint" in error_data["message"].lower():
                logger.info(f"Post {post_id} already exists in database")
                return False
            else:
                logger.error(f"Ошибка добавления поста {post_id}: {response.text}")
                return False
        else:
            logger.error(f"Ошибка добавления поста {post_id}: {response.status_code} {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса при добавлении поста {post_id}: {e}")
        return False

# Очистка HTML
def clean_html(text):
    text = unescape(text)
    text = re.sub(r"<a .*?>(.*?)</a>", r"\1", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<.*?>", "", text)
    return text.strip()

# Разбиение текста на части
def split_text(text, max_length=4096):
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

# Валидация и ресайз изображения
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
        logger.error(f"Ошибка обработки изображения: {e}")
        return None

# Получение всех постов из потока
def get_all_posts():
    try:
        response = requests.get(THREAD_URL, timeout=10)
        data = response.json()
        return data["threads"][0]["posts"]
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {e}")
        return []

# Отправка поста в Telegram с повторными попытками
async def send_post_to_telegram(bot, chat_id, post, max_retries=3):
    post_id = str(post["num"])
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

    for attempt in range(max_retries):
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
                    await bot.send_media_group(chat_id=chat_id, media=chunk)
                    await asyncio.sleep(3)  # Задержка между отправками медиа

            for i, message in enumerate(messages):
                if i == 0 and media_group:
                    continue
                await bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
                await asyncio.sleep(3)  # Задержка между отправками сообщений

            logger.info(f"Post {post_id} sent successfully")
            return True

        except RetryAfter as e:
            logger.error(f"Ошибка Telegram при отправке поста {post_id}: {e}")
            await asyncio.sleep(e.retry_after + 5)  # Ждём указанное время + 5 секунд
            if attempt == max_retries - 1:
                logger.warning(f"Failed to send post {post_id} after {max_retries} attempts")
                return False
            continue

        except TimedOut as e:
            logger.error(f"Ошибка Telegram при отправке поста {post_id}: {e}")
            await asyncio.sleep(10)  # Задержка при таймауте
            if attempt == max_retries - 1:
                logger.warning(f"Failed to send post {post_id} after {max_retries} attempts")
                return False
            continue

        except BadRequest as e:
            logger.error(f"Ошибка Telegram при отправке поста {post_id}: {e}")
            return False

    return False

# Основной цикл бота
async def bot_task():
    logger.info("Bot task started")
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    while True:
        logger.info("Checking for new posts")
        all_posts = get_all_posts()
        logger.info(f"Found {len(all_posts)} posts")
        for post in all_posts:
            post_id = str(post["num"])
            if is_post_sent(post_id):
                logger.info(f"Post {post_id} already sent, skipping")
                continue

            # Сначала пытаемся записать в базу
            if add_sent_post(post_id):
                logger.info(f"Sending post {post_id} to Telegram")
                success = await send_post_to_telegram(bot, TELEGRAM_CHANNEL_ID, post)
                if not success:
                    logger.warning(f"Failed to send post {post_id} to Telegram")
            else:
                logger.warning(f"Failed to add post {post_id} to database, skipping Telegram send")

        await asyncio.sleep(60 + random.uniform(1, 5))

# HTTP-сервер для проверки состояния
async def health(request):
    return web.Response(text="OK")

# Основная функция
async def main():
    # Запуск HTTP-сервера
    app = web.Application()
    app.add_routes([web.get('/health', health)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 10000))  # Изменено на 10000, так как Render обнаружил этот порт
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    # Запуск задачи бота
    bot_task_instance = asyncio.create_task(bot_task())

    # Поддержание работы event loop
    await asyncio.Future()  # Бесконечное ожидание

    # Очистка (не будет достигнута, но хорошо иметь)
    await runner.cleanup()
    bot_task_instance.cancel()

if __name__ == "__main__":
    asyncio.run(main())