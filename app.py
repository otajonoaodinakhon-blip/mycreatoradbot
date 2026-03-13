import os
import tempfile
import requests
import time
import logging
import schedule
from flask import Flask
from datetime import datetime

# ------------------- KONFIGURATSIYA -------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")          # Masalan: "@kanalnomi"
PORT = int(os.environ.get("PORT", 5000))

# GitHub Search parametrlari
SEARCH_QUERY = "stars:>1000"                       # 1000+ yulduzli repolar
SORT = "stars"                                     # Yulduzlar soni bo‘yicha saralash
ORDER = "desc"                                      # Eng ko‘pdan kamga
PER_PAGE = 10                                       # Har safar 10 ta reponi tekshiramiz
MAX_SIZE = 50 * 1024 * 1024                         # 50 MB

# Qaysi branch'larni sinab ko‘rish
BRANCHES = ["main", "master"]

# Oldin yuborilgan repolarni eslab qolish uchun (takror yubormaslik)
SENT_REPOS_FILE = "sent_repos.txt"

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    headers = {
        "Accept": "application/vnd.github.v3+json"
    }
    
    try:
        resp = requests.get(url, params=params, headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("items", [])
        else:
            logger.error(f"GitHub search xatosi: {resp.status_code} - {resp.text}")
            return []
    except Exception as e:
        logger.error(f"GitHub search exception: {e}")
        return []

# ------------------- ZIP YUKLAB OLISH -------------------
def download_repo_zip(repo_full_name, default_branch="main"):
    """Reponi ZIP sifatida yuklab oladi, agar 50MB dan kichik bo‘lsa"""
    # Avval hajmini tekshiramiz (HEAD so‘rov)
    head_url = f"https://github.com/{repo_full_name}/archive/refs/heads/{default_branch}.zip"
    try:
        head_resp = requests.head(head_url, allow_redirects=True)
        if head_resp.status_code == 200:
            content_length = head_resp.headers.get('Content-Length')
            if content_length and int(content_length) > MAX_SIZE:
                logger.info(f"{repo_full_name} hajmi {content_length} bayt – 50MB dan katta, tashlab ketildi.")
                return None
        else:
            # Agar branch topilmasa, keyingi branchni sinaymiz
            return None
    except Exception as e:
        logger.debug(f"HEAD so‘rovda xato: {e}")
        return None

    # Yuklab olish
    for branch in BRANCHES:
        zip_url = f"https://github.com/{repo_full_name}/archive/refs/heads/{branch}.zip"
        try:
            resp = requests.get(zip_url, stream=True, timeout=30)
            if resp.status_code == 200:
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                for chunk in resp.iter_content(chunk_size=8192):
                    temp_file.write(chunk)
                temp_file.close()
                logger.info(f"Yuklab olindi: {repo_full_name} ({branch})")
                return temp_file.name
            else:
                logger.debug(f"Branch {branch} topilmadi (status {resp.status_code})")
        except Exception as e:
            logger.error(f"Yuklab olishda xato ({branch}): {e}")
            continue
    
    logger.error(f"Hech qanday branch ishlamadi: {repo_full_name}")
    return None

# ------------------- TELEGRAMGA YUBORISH -------------------
def send_to_telegram(file_path, repo_info):
    """ZIP faylni Telegram kanaliga description bilan yuboradi"""
    # Caption yaratish
    caption = f"📦 **{repo_info['name']}**\n"
    caption += f"👤 {repo_info['owner']}\n"
    caption += f"⭐ {repo_info['stars']} stars\n"
    if repo_info['description']:
        caption += f"📝 {repo_info['description']}\n"
    caption += f"🔗 [GitHub](https://github.com/{repo_info['full_name']})\n"
    caption += f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {
                "chat_id": CHANNEL_ID,
                "caption": caption,
                "parse_mode": "Markdown"
            }
            resp = requests.post(url, files=files, data=data)
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
    """Eng popular repolarni qidirib, yangilarini kanalga yuboradi"""
    logger.info("🔍 Popular repolar qidirilmoqda...")
    sent_repos = load_sent_repos()
    repos = search_popular_repos()
    
    if not repos:
        logger.warning("Hech qanday repo topilmadi")
        return
    
    new_repos_count = 0
    for repo in repos:
        full_name = repo["full_name"]
        
        # Oldin yuborilganmi?
        if full_name in sent_repos:
            logger.debug(f"{full_name} allaqachon yuborilgan, o‘tkazib yuborildi.")
            continue
        
        # Repo ma'lumotlarini tayyorlash
        repo_info = {
            "full_name": full_name,
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "stars": repo["stargazers_count"],
            "description": repo["description"] or "No description",
            "default_branch": repo.get("default_branch", "main")
        }
        
        logger.info(f"➡️  {full_name} tekshirilmoqda...")
        
        # ZIP yuklab olish
        zip_path = download_repo_zip(full_name, repo_info["default_branch"])
        if not zip_path:
            # Agar default branch ishlamasa, boshqa branchlarni sinaymiz
            zip_path = download_repo_zip(full_name, "main")
            if not zip_path:
                zip_path = download_repo_zip(full_name, "master")
        
        if zip_path:
            # Yuborish
            success = send_to_telegram(zip_path, repo_info)
            
            # Tozalash
            try:
                os.remove(zip_path)
            except:
                pass
            
            if success:
                save_sent_repo(full_name)
                new_repos_count += 1
            
            # Rate limit uchun kutish
            time.sleep(2)
    
    logger.info(f"✅ {new_repos_count} ta yangi repo kanalga yuborildi.")

# ------------------- SCHEDULER -------------------
def run_scheduler():
    schedule.every(5).minutes.do(process_popular_repos)
    while True:
        schedule.run_pending()
        time.sleep(1)

# ------------------- FLASK SERVER -------------------
app = Flask(__name__)

@app.route('/')
def health():
    return "Bot ishlayapti!", 200

# ------------------- BOSHLASH -------------------
if __name__ == "__main__":
    logger.info("🤖 Avtomatik GitHub popular repo bot ishga tushdi.")
    logger.info(f"Qidiruv so‘rovi: {SEARCH_QUERY}")
    
    # Scheduler thread
    import threading
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Dastlab bir marta ishga tushirish
    process_popular_repos()
    
    # Flask server
    app.run(host="0.0.0.0", port=PORT)
