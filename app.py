import os
import tempfile
import requests
import time
import logging
import schedule
from flask import Flask
from datetime import datetime
import threading

# ------------------- KONFIGURATSIYA (environment variables) -------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")          # Masalan: "@kanalnomi"
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")      # Ixtiyoriy, lekin rate limit uchun tavsiya etiladi
PORT = int(os.environ.get("PORT", 5000))

# GitHub Search parametrlari
SEARCH_QUERY = "stars:>10"          # 1000+ yulduzli repolar
SORT = "stars"
ORDER = "desc"
PER_PAGE = 10                          # Har safar 10 ta repo tekshiriladi
MAX_SIZE_BYTES = 50 * 1024 * 1024      # 50 MB
MAX_SIZE_KB = MAX_SIZE_BYTES // 1024   # 51200 KB

# Qaysi branch'larni sinab ko‘rish
BRANCHES = ["main", "master"]

# Oldin yuborilgan repolarni eslab qolish uchun fayl
SENT_REPOS_FILE = "sent_repos.txt"

# Logging sozlamalari
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ------------------- YORDAMCHI FUNKSIYALAR -------------------
def load_sent_repos():
    """Oldin yuborilgan repolarni fayldan o‘qish"""
    if not os.path.exists(SENT_REPOS_FILE):
        return set()
    with open(SENT_REPOS_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_sent_repo(repo_full_name):
    """Yangi yuborilgan reponi faylga qo‘shish"""
    with open(SENT_REPOS_FILE, "a") as f:
        f.write(repo_full_name + "\n")

# ------------------- GITHUB SEARCH -------------------
def search_popular_repos():
    """GitHub Search API orqali eng popular repolarni qidiradi"""
    url = "https://api.github.com/search/repositories"
    params = {
        "q": SEARCH_QUERY,
        "sort": SORT,
        "order": ORDER,
        "per_page": PER_PAGE
    }
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            logger.info(f"GitHub dan {len(items)} ta repo topildi.")
            return items
        else:
            logger.error(f"GitHub search xatosi: {resp.status_code} - {resp.text}")
            return []
    except Exception as e:
        logger.error(f"GitHub search exception: {e}")
        return []

# ------------------- REPO HAJMINI TEKSHIRISH -------------------
def check_repo_size(repo_full_name):
    """Reponing hajmini KB da qaytaradi (GitHub API orqali)"""
    url = f"https://api.github.com/repos/{repo_full_name}"
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    try:
        resp = requests.get(url, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            size_kb = data.get("size", 0)  # KB da
            return size_kb
        else:
            logger.error(f"Repo info xatosi {repo_full_name}: {resp.status_code}")
            return None
    except Exception as e:
        logger.error(f"Repo info so‘rovida xato {repo_full_name}: {e}")
        return None

# ------------------- ZIP YUKLAB OLISH -------------------
def download_repo_zip(repo_full_name):
    """Reponi ZIP sifatida yuklab oladi (birinchi mavjud branch dan)"""
    for branch in BRANCHES:
        zip_url = f"https://github.com/{repo_full_name}/archive/refs/heads/{branch}.zip"
        try:
            # HEAD so‘rov orqali hajmni tekshirish (ixtiyoriy, lekin ishonchlilik uchun)
            head_resp = requests.head(zip_url, allow_redirects=True, timeout=10)
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get('Content-Length')
                if content_length and int(content_length) > MAX_SIZE_BYTES:
                    logger.info(f"{repo_full_name} ({branch}) hajmi {content_length} bayt – 50MB dan katta (HEAD), tashlab ketildi.")
                    return "TOO_LARGE"
                
                # Yuklab olish
                zip_resp = requests.get(zip_url, stream=True, timeout=30)
                if zip_resp.status_code == 200:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                    for chunk in zip_resp.iter_content(chunk_size=8192):
                        temp_file.write(chunk)
                    temp_file.close()
                    logger.info(f"Yuklab olindi: {repo_full_name} ({branch})")
                    return temp_file.name
                else:
                    logger.debug(f"Yuklab olishda xato: {branch} status {zip_resp.status_code}")
            else:
                logger.debug(f"Branch {branch} topilmadi (status {head_resp.status_code})")
        except Exception as e:
            logger.debug(f"Branch {branch} tekshirishda xato: {e}")
            continue

    logger.error(f"Hech qanday branch ishlamadi: {repo_full_name}")
    return None  # Boshqa xatolik

# ------------------- TELEGRAMGA YUBORISH -------------------
def send_to_telegram(file_path, repo_info):
    """ZIP faylni Telegram kanaliga chiroyli caption bilan yuboradi"""
    caption = f"""
╔══════════════════════════╗
║     📦 **{repo_info['name']}**   ║
║     🔗 Git Provider       ║
╚══════════════════════════╝

👤 **Muallif:** {repo_info['owner']}
⭐ **Yulduzlar:** {repo_info['stars']}
🔧 **Til:** {repo_info.get('language', 'Noma’lum')}
📝 **Tavsif:** {repo_info['description'][:200]}...

👉 [GitHub da ko‘rish](https://github.com/{repo_info['full_name']})
🕒 Yuklandi: {datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {
                "chat_id": CHANNEL_ID,
                "caption": caption,
                "parse_mode": "Markdown"
            }
            resp = requests.post(url, files=files, data=data, timeout=60)
        if resp.status_code == 200:
            logger.info(f"Yuborildi: {repo_info['full_name']}")
            return True
        else:
            logger.error(f"Telegram xatosi: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        logger.error(f"Telegramga yuborishda xato: {e}")
        return False

# ------------------- ASOSIY JOB -------------------
def process_popular_repos():
    """Eng popular repolarni qidirib, hajmini tekshirib, kichiklarini yuklab yuboradi"""
    logger.info("🔍 Popular repolar qidirilmoqda...")
    sent_repos = load_sent_repos()
    repos = search_popular_repos()

    if not repos:
        logger.warning("Hech qanday repo topilmadi")
        return

    new_repos_count = 0
    for repo in repos:
        full_name = repo["full_name"]

        # 1. Oldin yuborilganmi?
        if full_name in sent_repos:
            logger.debug(f"{full_name} allaqachon ko‘rilgan, o‘tkazib yuborildi.")
            continue

        # 2. Repo hajmini tekshirish (GitHub API)
        size_kb = check_repo_size(full_name)
        if size_kb is None:
            continue  # API xatosi bo‘lsa, keyingi safar qayta uriniladi

        if size_kb > MAX_SIZE_KB:
            logger.info(f"{full_name} hajmi {size_kb/1024:.1f} MB – 50 MB dan katta, tashlab ketildi.")
            save_sent_repo(full_name)  # Katta repo – eslab qolamiz
            continue

        logger.info(f"✅ {full_name} hajmi {size_kb/1024:.1f} MB – yuklash mumkin")

        # 3. Repo ma'lumotlarini tayyorlash
        repo_info = {
            "full_name": full_name,
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "stars": repo["stargazers_count"],
            "description": repo["description"] or "No description",
            "language": repo.get("language")
        }

        # 4. ZIP yuklab olish
        result = download_repo_zip(full_name)
        if result == "TOO_LARGE":
            # HEAD orqali aniqlangan katta repo – eslab qolamiz
            save_sent_repo(full_name)
            continue
        elif result is None:
            # Boshqa xatolik – hozircha eslab qolmaymiz, keyin qayta tekshiriladi
            logger.warning(f"{full_name} yuklab olinmadi, keyingi safar qayta tekshiriladi.")
            continue
        else:
            zip_path = result
            success = send_to_telegram(zip_path, repo_info)
            try:
                os.remove(zip_path)
                logger.debug(f"Vaqtinchalik fayl o‘chirildi: {zip_path}")
            except Exception as e:
                logger.error(f"Faylni o‘chirishda xato: {e}")

            if success:
                save_sent_repo(full_name)
                new_repos_count += 1

            # Telegram rate limit uchun 2 soniya kutish
            time.sleep(2)

    logger.info(f"✅ {new_repos_count} ta yangi repo kanalga yuborildi.")

# ------------------- SCHEDULER (HAR 5 DAKIQADA) -------------------
def run_scheduler():
    schedule.every(5).minutes.do(process_popular_repos)
    while True:
        schedule.run_pending()
        time.sleep(1)

# ------------------- FLASK SERVER (RENDER UCHUN) -------------------
app = Flask(__name__)

@app.route('/')
def health():
    return "Bot ishlayapti!", 200

# ------------------- BOSHLASH -------------------
if __name__ == "__main__":
    logger.info("🤖 Bot ishga tushdi.")
    logger.info(f"Qidiruv so‘rovi: {SEARCH_QUERY}")

    # Scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()

    # Dastlab bir marta ishga tushirish
    process_popular_repos()

    # Flask server
    app.run(host="0.0.0.0", port=PORT)
