import logging
import logging.handlers
import threading
import queue
import os
import requests
import asyncio
import random
import re
import json
from io import BytesIO
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape
from PIL import Image

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()]
)
logging.getLogger("telegram.Bot").setLevel(logging.WARNING)

logger = logging.getLogger(__name__)
log_queue = queue.Queue()
queue_handler = logging.handlers.QueueHandler(log_queue)
logger.addHandler(queue_handler)

def log_listener():
    while True:
        try:
            record = log_queue.get()
            if record is None:
                break
            logger = logging.getLogger(record.name)
            logger.handle(record)
        except Exception as e:
            print(f"Ошибка при обработке лога: {e}")

listener_thread = threading.Thread(target=log_listener, daemon=True)
listener_thread.start()

# Файл для хранения последнего ID
LAST_POST_FILE = "last_post_id.json"

# Настройки из переменных окружения
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL", "https://2ch.hk/cc/res/229275.json")

if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHANNEL_ID:
    raise ValueError("Отсутствуют TELEGRAM_BOT_TOKEN или TELEGRAM_CHANNEL_ID!")

# Загрузка последнего ID из файла
def load_last_post_id():
    try:
        with open(LAST_POST_FILE, "r") as f:
            data = json.load(f)
            return int(data.get("last_post_id", 0))
    except (FileNotFoundError, json.JSONDecodeError, ValueError):
        return 0

# Сохранение последнего ID в файл
def save_last_post_id(post_id):
    with open(LAST_POST_FILE, "w") as f:
        json.dump({"last_post_id": post_id}, f)
    logger.info(f"Обновлён LAST_POST_ID: {post_id}")

LAST_POST_ID = load_last_post_id()

# Инициализация бота один раз
bot = Bot(token=TELEGRAM_BOT_TOKEN)
logger.info("Бот инициализирован")

CHECK_INTERVAL = 60
MAX_CAPTION_LENGTH = 1024
MAX_MESSAGE_LENGTH = 4096

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
        logger.error(f"Ошибка обработки изображения: {e}")
        return None

async def get_new_posts():
    global LAST_POST_ID
    try:
        response = requests.get(THREAD_URL, timeout=10)
        response.raise_for_status()
        data = response.json()
        posts = data["threads"][0]["posts"]
        new_posts = [p for p in posts if int(p["num"]) > LAST_POST_ID]
        return new_posts
    except Exception as e:
        logger.error(f"Ошибка при парсинге: {e}")
        return []

async def post_to_telegram():
    global LAST_POST_ID
    while True:
        try:
            new_posts = await get_new_posts()
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

                    file_links = [
                        f"{file['name']}: https://2ch.hk{file['path']}"
                        for file in files
                        if not file["path"].endswith((".jpg", ".jpeg", ".png", ".gif", ".webm", ".mp4"))
                    ]
                    if file_links:
                        text += "\n\n" + "\n".join(file_links)

                    messages = split_text(text)
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

                    if not video_sent:
                        for file in files:
                            if file["path"].endswith((".webm", ".mp4")):
                                text += f"\n\n🎥 Видео: https://2ch.hk{file['path']}"
                        messages = split_text(text)

                    if media_group:
                        for i in range(0, len(media_group), 10):
                            chunk = media_group[i:i+10]
                            if i == 0 and messages:
                                chunk[0] = InputMediaPhoto(media=chunk[0].media, caption=messages[0][:MAX_CAPTION_LENGTH], parse_mode="HTML")
                            await bot.send_media_group(chat_id=TELEGRAM_CHANNEL_ID, media=chunk)
                            await asyncio.sleep(1.5)

                    for i, message in enumerate(messages):
                        if i == 0 and media_group:
                            continue
                        await bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=message, parse_mode="HTML")
                        await asyncio.sleep(1.5)

                    LAST_POST_ID = post_id
                    save_last_post_id(post_id)
        except Exception as e:
            logger.error(f"Ошибка в цикле обработки: {e}")
            await asyncio.sleep(5)
        
        await asyncio.sleep(CHECK_INTERVAL + random.uniform(1, 5))

async def main():
    logger.info("Запуск бота...")
    await post_to_telegram()

if __name__ == "__main__":
    # Защита от многократного запуска
    pid_file = "bot.pid"
    if os.path.exists(pid_file):
        with open(pid_file, "r") as f:
            pid = int(f.read().strip())
        try:
            os.kill(pid, 0)
            logger.error("Бот уже запущен с PID %d. Завершаем.", pid)
            exit(1)
        except OSError:
            pass  # Процесс не существует, можно запускать

    with open(pid_file, "w") as f:
        f.write(str(os.getpid()))

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем")
    finally:
        os.remove(pid_file)
        log_queue.put(None)
        listener_thread.join()