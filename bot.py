import os
import time
import requests
import logging
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from PIL import Image
from io import BytesIO

# Настройки
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL")
SENT_POSTS_FILE = "sent_posts.txt"
MAX_MESSAGE_LENGTH = 4096

bot = Bot(token=TELEGRAM_BOT_TOKEN)
logging.basicConfig(level=logging.INFO)

def load_sent_posts():
    if not os.path.exists(SENT_POSTS_FILE):
        return set()
    with open(SENT_POSTS_FILE, "r") as f:
        return set(f.read().splitlines())

def save_sent_post(post_id):
    with open(SENT_POSTS_FILE, "a") as f:
        f.write(post_id + "\n")

def fetch_posts():
    response = requests.get(THREAD_URL)
    response.raise_for_status()
    return response.json().get("posts", [])

def split_text(text, max_length=MAX_MESSAGE_LENGTH):
    """Разбивает длинный текст на части, не превышающие max_length."""
    parts = []
    while len(text) > max_length:
        split_index = text.rfind(" ", 0, max_length)
        if split_index == -1:
            split_index = max_length
        parts.append(text[:split_index])
        text = text[split_index:].strip()
    parts.append(text)
    return parts

def download_and_resize_image(url, max_size=(1280, 1280)):
    response = requests.get(url)
    response.raise_for_status()
    img = Image.open(BytesIO(response.content))
    img.thumbnail(max_size)
    img_byte_array = BytesIO()
    img.save(img_byte_array, format=img.format)
    return img_byte_array.getvalue()

def send_post(post):
    post_id = str(post["id"])
    text = f"#{post_id}\n{post['content']}"
    media = []
    
    # Обработка изображений и видео
    for attachment in post.get("attachments", []):
        url = attachment.get("url")
        if attachment.get("type") == "image":
            media.append(InputMediaPhoto(download_and_resize_image(url)))
        elif attachment.get("type") == "video":
            media.append(InputMediaVideo(url))
    
    try:
        # Отправка текста
        text_parts = split_text(text)
        sent_message = None
        for part in text_parts:
            sent_message = bot.send_message(chat_id=TELEGRAM_CHANNEL_ID, text=part, reply_to_message_id=sent_message.message_id if sent_message else None)
            time.sleep(1)  # Небольшая задержка между сообщениями
        
        # Отправка медиа
        if media:
            bot.send_media_group(chat_id=TELEGRAM_CHANNEL_ID, media=media, reply_to_message_id=sent_message.message_id if sent_message else None)
        
        save_sent_post(post_id)
    except (RetryAfter, TimedOut) as e:
        logging.warning(f"Ошибка: {e}. Повторная попытка через 10 секунд...")
        time.sleep(10)
        send_post(post)  # Повторная попытка
    except BadRequest as e:
        logging.error(f"Ошибка при отправке сообщения: {e}")

def main():
    sent_posts = load_sent_posts()
    posts = fetch_posts()
    for post in posts:
        if str(post["id"]) not in sent_posts:
            send_post(post)
            time.sleep(2)  # Задержка между отправками

if __name__ == "__main__":
    main()
