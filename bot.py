import logging
import logging.handlers
import threading
import queue
import os
import requests
import asyncio
import random
import re
from io import BytesIO
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape
from PIL import Image

# Очередь для логов
log_queue = queue.Queue()

# Обработчик для записи логов в очередь
queue_handler = logging.handlers.QueueHandler(log_queue)

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,  # Уровень логирования
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),  # Для вывода в терминал
        logging.FileHandler("bot_logs.log")  # Для записи в файл
    ]
)

logger = logging.getLogger()
logger.addHandler(queue_handler)

# Обработчик для вывода логов
def log_listener():
    while True:
        try:
            record = log_queue.get()
            if record is None:
                break  # Завершаем слушатель при получении None
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception as e:
            print(f"Ошибка при обработке лога: {e}")

# Запуск слушателя логов в фоновом потоке
listener_thread = threading.Thread(target=log_listener)
listener_thread.start()

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL", "https://2ch.hk/cc/res/229275.json")
LAST_POST_ID = int(os.getenv("LAST_POST_ID", 1247714))  # Загружаем последний отправленный пост

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
    raise ValueError("Отсутствуют TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID в переменных окружения!")

bot = Bot(token=TELEGRAM_BOT_TOKEN)
CHECK_INTERVAL = 60  # Интервал проверки в секундах
MAX_CAPTION_LENGTH = 1024
MAX_MESSAGE_LENGTH = 4096

# Логирование
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.error(f"Ошибка обработки изображения: {e}")
        return None

# Функция получения новых постов
def get_new_posts():
    try:
        response = requests.get(THREAD_URL, timeout=10)
        data = response.json()
        posts = data["threads"][0]["posts"]
        return [p for p in posts if int(p["num"]) > LAST_POST_ID]
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {e}")
        return []

# Функция обновления переменной окружения
def update_last_post_id(post_id):
    global LAST_POST_ID
    LAST_POST_ID = post_id
    os.environ["LAST_POST_ID"] = str(post_id)
    logger.info(f"Обновлён LAST_POST_ID: {post_id}")

# Функция для публикации в Telegram
async def post_to_telegram():
    global LAST_POST_ID
    while True:
        new_posts = get_new_posts()
        if not new_posts:
            logger.info("Новых постов нет. Ждем...")
        else:
            for post in new_posts:
                post_id = int(post["num"])
                text = clean_html(post.get("comment", ""))
                files = post.get("files", []) or []
                media_group = []

                header = f"#{post_id}"
                text = f"{header}\n\n{text}" if text else header

                # Ссылки на файлы, кроме изображений и видео
                file_links = [
                    f"{file['name']}: https://2ch.hk{file['path']}"
                    for file in files
                    if not file["path"].endswith((".jpg", ".jpeg", ".png", ".gif", ".webm", ".mp4"))
                ]
                
                if file_links:
                    text += "\n\n" + "\n".join(file_links)

                messages = split_text(text)

                try:
                    # Обрабатываем медиафайлы
                    video_sent = False
                    for file in files:
                        file_url = f"https://2ch.hk{file['path']}"
                        if file["path"].endswith((".jpg", ".jpeg", ".png", ".gif")):
                            response = requests.get(file_url, timeout=10)
                            if response.status_code == 200:
                                image_data = validate_and_resize_image(response.content)
                                if image_data:
                                    media_group.append(InputMediaPhoto(media=image_data))
                        elif file["path"].endswith((".webm", ".mp4")):
                            response = requests.get(file_url, timeout=10)
                            if response.status_code == 200:
                                video_data = BytesIO(response.content)
                                media_group.append(InputMediaVideo(media=video_data))
                                video_sent = True
                    
                    # Если видео не отправлено, добавляем ссылку
                    if not video_sent:
                        for file in files:
                            if file["path"].endswith((".webm", ".mp4")):
                                text += f"\n\n🎥 Видео: https://2ch.hk{file['path']}"
                        messages = split_text(text)  # Обновляем текстовые сообщения

                    # Отправка медиафайлов
                    if media_group:
                        for i in range(0, len(media_group), 10):
                            chunk = media_group[i:i+10]
                            if i == 0 and messages:
                                chunk[0] = InputMediaPhoto(media=chunk[0].media, caption=messages[0], parse_mode="HTML")
                            await bot.send_media_group(chat_id=TELEGRAM_CHANNEL_ID, media=chunk)
                            await asyncio.sleep(1.5)

                    # Отправка текста
                    for i, message in enumerate(messages):
                        if i == 0 and media_group:
                            continue  # Первая часть уже отправлена с медиа
                        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode="HTML")
                        await asyncio.sleep(1.5)

                    # Обновляем ID последнего поста
                    update_last_post_id(post_id)
                except (RetryAfter, TimedOut, BadRequest) as e:
                    logger.error(f"Ошибка Telegram: {e}")
                    await asyncio.sleep(5)
                    continue
        
        await asyncio.sleep(CHECK_INTERVAL + random.uniform(1, 5))

# Запуск бота
def start_bot():
    logger.info("Запуск бота...")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(post_to_telegram())

if __name__ == "__main__":
    try:
        start_bot()
    except KeyboardInterrupt:
        logger.info("Бот завершил свою работу.")
        listener_thread.join()  # Завершаем поток слушателя логов