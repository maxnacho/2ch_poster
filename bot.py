import os
import requests
import asyncio
import re
import logging
from io import BytesIO
from datetime import datetime
from telegram import Bot, InputMediaPhoto, InputMediaVideo
from telegram.error import RetryAfter, TimedOut, BadRequest
from html import unescape
from PIL import Image
from aiohttp import web

# =========================
# Логирование
# =========================
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# =========================
# Переменные окружения
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHANNEL_ID = os.getenv("TELEGRAM_CHANNEL_ID")
THREAD_URL = os.getenv("THREAD_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

required_env = {
    "TELEGRAM_BOT_TOKEN": TELEGRAM_BOT_TOKEN,
    "TELEGRAM_CHANNEL_ID": TELEGRAM_CHANNEL_ID,
    "THREAD_URL": THREAD_URL,
    "SUPABASE_URL": SUPABASE_URL,
    "SUPABASE_KEY": SUPABASE_KEY,
}
missing = [k for k, v in required_env.items() if not v]
if missing:
    raise ValueError(f"Отсутствуют необходимые переменные окружения: {', '.join(missing)}")

logger.info(f"THREAD_URL raw: {THREAD_URL}")

# =========================
# HTTP-сессия (общая)
# =========================
SESSION = requests.Session()
SESSION.headers.update({
    "User-Agent": "Mozilla/5.0 (compatible; 2ch_poster/1.0; +render.com)",
    "Accept": "application/json, */*;q=0.1",
    "X-Requested-With": "XMLHttpRequest",
})

# =========================
# Supabase REST
# =========================
headers = {
    "apikey": SUPABASE_KEY,
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}
base_url = f"{SUPABASE_URL}/rest/v1"

# =========================
# Глобальная блокировка
# =========================
bot_task_lock = asyncio.Lock()

# =========================
# Вспомогательные функции
# =========================
def clean_html(text):
    text = unescape(text)
    text = re.sub(r"<a .*?>(.*?)</a>", r"\1", text)
    text = re.sub(r"<br\s*/?>", "\n", text)
    text = re.sub(r"<.*?>", "", text)
    return text.strip()

def split_text(text, max_length=4096):
    return [text[i:i + max_length] for i in range(0, len(text), max_length)]

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

# ------- Supabase helpers
async def are_posts_sent(post_ids, max_retries=5):
    if not post_ids:
        return set()
    post_ids_str = ",".join(map(str, post_ids))
    url = f"{base_url}/sent_posts?select=post_id&post_id=in.({post_ids_str})"
    for attempt in range(max_retries):
        try:
            response = SESSION.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            data = response.json()
            logger.info(f"are_posts_sent: Found {len(data)} existing posts for {len(post_ids)} IDs")
            return {item["post_id"] for item in data}
        except requests.exceptions.RequestException as e:
            logger.error(f"Ошибка проверки постов (попытка {attempt + 1}/{max_retries}): {e}")
            if attempt == max_retries - 1:
                logger.error("Не удалось проверить посты после всех попыток")
                return set()
            await asyncio.sleep(2 ** attempt)
    return set()

async def add_sent_posts(post_ids):
    if not post_ids:
        return True
    url = f"{base_url}/sent_posts"
    current_time = datetime.utcnow().isoformat()
    data = [{"post_id": post_id, "created_at": current_time} for post_id in post_ids]
    try:
        response = SESSION.post(url, headers=headers, json=data, timeout=15)
        if response.status_code == 201:
            logger.info(f"Posts {post_ids} successfully added to database at {current_time}")
            return True
        elif response.status_code == 409:
            logger.warning(f"Duplicate posts detected in {post_ids}, but ignored due to merge-duplicates")
            return True
        else:
            logger.error(f"Ошибка добавления постов {post_ids}: {response.status_code} {response.text}")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"Ошибка запроса при добавлении постов {post_ids}: {e}")
        return False

# =========================
# Парсинг THREAD_URL и API endpoints
# =========================
THREAD_RE = re.compile(
    r"""(?xi)
    ^(?:https?://)?      # optional scheme
    (?:(?:2ch\.hk|2ch\.life|2ch\.pm|2-ch\.ru))/   # host
    (?P<board>[a-z0-9_]+)/                       # board
    (?:res/)?                                     # optional 'res/'
    (?P<thread>\d+)                               # thread id
    (?:\.(?:html|json))?                          # optional extension
    (?:\?.*)?$                                    # optional query
    """
)

def parse_thread(thread_url: str):
    m = THREAD_RE.search(thread_url.strip())
    if not m:
        raise ValueError("THREAD_URL не распознан. Пример: https://2ch.hk/b/res/123456.html")
    board = m.group("board")
    thread = m.group("thread")
    return board, thread

BOARD, THREAD_ID = parse_thread(THREAD_URL)
logger.info(f"Parsed THREAD: board={BOARD}, thread={THREAD_ID}")

API_ENDPOINTS = [
    f"https://2ch.hk/makaba/mobile.fcgi?task=get_thread&board={BOARD}&thread={THREAD_ID}&post=0",
    f"https://2ch.life/makaba/mobile.fcgi?task=get_thread&board={BOARD}&thread={THREAD_ID}&post=0",
]

# =========================
# Получение постов (Makaba API + фолбэк)
# =========================
def get_all_posts():
    last_err = None
    for api_url in API_ENDPOINTS:
        try:
            resp = SESSION.get(api_url, timeout=25)
            if resp.status_code != 200:
                logger.error(f"[{api_url}] HTTP {resp.status_code}; first 200 bytes: {resp.text[:200]!r}")
                last_err = f"HTTP {resp.status_code}"
                continue

            ctype = resp.headers.get("Content-Type", "")
            if "application/json" not in ctype and "text/json" not in ctype:
                # Makaba иногда отдает text/plain с JSON — пробуем json() всё равно
                if "html" in ctype:
                    logger.error(f"[{api_url}] Ожидался JSON, пришёл HTML; first 200 bytes: {resp.text[:200]!r}")
                    last_err = "HTML instead of JSON"
                    continue

            try:
                data = resp.json()
            except ValueError as e:
                logger.error(f"[{api_url}] JSONDecodeError: {e}; first 200 bytes: {resp.text[:200]!r}")
                last_err = "bad JSON"
                continue

            # Структура Makaba: список постов или словарь {"Error": "..."}
            if isinstance(data, dict) and data.get("Error"):
                logger.error(f"[{api_url}] Makaba Error: {data.get('Error')}")
                last_err = f"Makaba error: {data.get('Error')}"
                continue

            if isinstance(data, list):
                # список постов
                return data
            elif isinstance(data, dict) and "threads" in data:
                # На всякий: поддержка схемы threads[0].posts
                threads = data.get("threads") or []
                if threads:
                    return threads[0].get("posts", [])
                return []

            logger.error(f"[{api_url}] Неожиданная структура ответа: type={type(data)}")
            last_err = "unexpected structure"
        except requests.exceptions.RequestException as e:
            logger.error(f"[{api_url}] Ошибка сети: {e}")
            last_err = str(e)

    # Если все варианты не удались
    logger.error(f"Не удалось получить JSON от всех эндпоинтов. Последняя ошибка: {last_err}")
    return []

# =========================
# Отправка в Telegram
# =========================
async def send_post_to_telegram(bot, chat_id, post, max_retries=3):
    # Makaba возвращает поля "num", "comment", "files" (могут быть [])
    post_id = str(post.get("num"))
    text = clean_html(post.get("comment", "") or "")
    files = post.get("files", []) or []
    media_group = []

    header = f"#{post_id}"
    text = f"{header}\n\n{text}" if text else header

    file_links = []
    for file in files:
        path = file.get("path") or file.get("fullname") or ""
        if not path:
            continue
        file_url = f"https://2ch.hk{path}"
        if path.endswith((".webm", ".mp4")):
            file_links.append(file_url)

    if file_links:
        text += "\n\n" + "\n".join(file_links)

    messages = split_text(text)

    for attempt in range(max_retries):
        try:
            # Скачивание медиа
            for file in files:
                path = file.get("path") or file.get("fullname") or ""
                if not path:
                    continue
                file_url = f"https://2ch.hk{path}"
                try:
                    response = SESSION.get(file_url, timeout=30)
                    if response.status_code != 200:
                        logger.warning(f"Не удалось скачать файл {file_url}: HTTP {response.status_code}")
                        continue
                except requests.exceptions.RequestException as e:
                    logger.warning(f"Ошибка скачивания {file_url}: {e}")
                    continue

                if path.endswith((".jpg", ".jpeg", ".png", ".gif")):
                    image_data = validate_and_resize_image(response.content)
                    if image_data:
                        media_group.append(InputMediaPhoto(media=image_data))
                elif path.endswith((".webm", ".mp4")):
                    video_data = BytesIO(response.content)
                    media_group.append(InputMediaVideo(media=video_data))

            # Медиа-группы по 10
            if media_group:
                for i in range(0, len(media_group), 10):
                    chunk = media_group[i:i+10]
                    if i == 0 and messages:
                        try:
                            chunk[0].caption = messages[0]
                            chunk[0].parse_mode = "HTML"
                        except Exception as e:
                            logger.warning(f"Не удалось назначить подпись первому медиа: {e}")
                    await bot.send_media_group(chat_id=chat_id, media=chunk)
                    await asyncio.sleep(3)

            # Остальные текстовые
            for i, message in enumerate(messages):
                if i == 0 and media_group:
                    continue
                await bot.send_message(chat_id=chat_id, text=message, parse_mode="HTML")
                await asyncio.sleep(3)

            logger.info(f"Post {post_id} sent successfully")
            return True

        except RetryAfter as e:
            logger.error(f"RetryAfter при отправке поста {post_id}: {e}")
            await asyncio.sleep(e.retry_after + 5)
            if attempt == max_retries - 1:
                logger.warning(f"Failed to send post {post_id} after {max_retries} attempts")
                return False
            continue

        except TimedOut as e:
            logger.error(f"TimedOut при отправке поста {post_id}: {e}")
            await asyncio.sleep(10)
            if attempt == max_retries - 1:
                logger.warning(f"Failed to send post {post_id} after {max_retries} attempts")
                return False
            continue

        except BadRequest as e:
            logger.error(f"BadRequest при отправке поста {post_id}: {e}")
            return False

    return False

# =========================
# Основной цикл
# =========================
async def bot_task():
    logger.info("Bot task started")
    bot = Bot(token=TELEGRAM_BOT_TOKEN)

    while True:
        async with bot_task_lock:
            logger.info("Checking for new posts")
            all_posts = get_all_posts()
            logger.info(f"Found {len(all_posts)} posts")

            post_ids = [str(p.get("num")) for p in all_posts if p.get("num")]
            sent_post_ids = await are_posts_sent(post_ids)

            new_posts = [p for p in all_posts if str(p.get("num")) not in sent_post_ids]
            new_post_ids = [str(p.get("num")) for p in new_posts]

            if not new_posts:
                logger.info("No new posts to send")
            else:
                logger.info(f"Found {len(new_posts)} new posts: {new_post_ids}")
                final_check = await are_posts_sent(new_post_ids)
                new_posts = [p for p in new_posts if str(p.get("num")) not in final_check]
                new_post_ids = [str(p.get("num")) for p in new_posts]
                logger.info(f"After final check, {len(new_posts)} posts remain: {new_post_ids}")

                if new_posts:
                    success = await add_sent_posts(new_post_ids)
                    if success:
                        for post in new_posts:
                            post_id = str(post.get("num"))
                            logger.info(f"Sending post {post_id} to Telegram")
                            ok = await send_post_to_telegram(bot, TELEGRAM_CHANNEL_ID, post)
                            if not ok:
                                logger.warning(f"Failed to send post {post_id} to Telegram")
                    else:
                        logger.warning("Failed to add new posts to database, skipping Telegram send")

        await asyncio.sleep(300)  # 5 минут

# =========================
# HTTP healthcheck
# =========================
async def health(request):
    logger.info("Health check requested")
    return web.Response(text="OK")

# =========================
# main
# =========================
async def main():
    app = web.Application()
    app.add_routes([web.get('/health', health)])
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.getenv('PORT', 10000))
    site = web.TCPSite(runner, '0.0.0.0', port)
    await site.start()

    bot_task_instance = asyncio.create_task(bot_task())
    await asyncio.Future()

    await runner.cleanup()
    bot_task_instance.cancel()

if __name__ == "__main__":
    asyncio.run(main())