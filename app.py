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
CHANNEL_ID = os.environ.get("CHANNEL_ID")
PORT = int(os.environ.get("PORT", 5000))

SEARCH_QUERY = "stars:>1000"
SORT = "stars"
ORDER = "desc"
PER_PAGE = 10
MAX_SIZE = 50 * 1024 * 1024  # 50 MB
BRANCHES = ["main", "master"]

SENT_REPOS_FILE = "sent_repos.txt"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ------------------- YORDAMCHI FUNKSIYALAR -------------------
def load_sent_repos():
    if not os.path.exists(SENT_REPOS_FILE):
        return set()
    with open(SENT_REPOS_FILE, "r") as f:
        return set(line.strip() for line in f if line.strip())

def save_sent_repo(repo_full_name):
    with open(SENT_REPOS_FILE, "a") as f:
        f.write(repo_full_name + "\n")

# ------------------- GITHUB SEARCH -------------------
def search_popular_repos():
    url = "https://api.github.com/search/repositories"
    params = {
        "q": SEARCH_QUERY,
        "sort": SORT,
        "order": ORDER,
        "per_page": PER_PAGE
    }
    headers = {"Accept": "application/vnd.github.v3+json"}
    try:
        resp = requests.get(url, params=params, headers=headers)
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

# ------------------- ZIP YUKLAB OLISH (TO‘G‘RILANGAN) -------------------
def download_repo_zip(repo_full_name):
    """Har bir branchni sinab ko‘radi, hajmni tekshiradi va yuklaydi."""
    for branch in BRANCHES:
        head_url = f"https://github.com/{repo_full_name}/archive/refs/heads/{branch}.zip"
        try:
            # HEAD so‘rov orqali hajmni tekshirish
            head_resp = requests.head(head_url, allow_redirects=True, timeout=10)
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get('Content-Length')
                if content_length and int(content_length) > MAX_SIZE:
                    logger.info(f"{repo_full_name} ({branch}) hajmi {content_length} bayt – 50MB dan katta, tashlab ketildi.")
                    # Katta repo – uni eslab qolish uchun maxsus qiymat qaytaramiz
                    return "TOO_LARGE"
                
                # Hajm maqbul – yuklab olish
                zip_resp = requests.get(head_url, stream=True, timeout=30)
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
    caption = f"📦 **{repo_info['name']}**\n"
    caption += f"👤 {repo_info['owner']}\n"
    caption += f"⭐ {repo_info['stars']} stars\n"
    if repo_info.get('language'):
        caption += f"🔧 {repo_info['language']}\n"
    if repo_info['description']:
        caption += f"📝 {repo_info['description']}\n"
    caption += f"🔗 [GitHub](https://github.com/{repo_info['full_name']})\n"
    caption += f"🕒 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    try:
        with open(file_path, "rb") as f:
            files = {"document": f}
            data = {"chat_id": CHANNEL_ID, "caption": caption, "parse_mode": "Markdown"}
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
    logger.info("🔍 Popular repolar qidirilmoqda...")
    sent_repos = load_sent_repos()
    repos = search_popular_repos()
    
    if not repos:
        logger.warning("Hech qanday repo topilmadi")
        return
    
    new_repos_count = 0
    for repo in repos:
        full_name = repo["full_name"]
        
        if full_name in sent_repos:
            logger.debug(f"{full_name} allaqachon ko‘rilgan, o‘tkazib yuborildi.")
            continue
        
        repo_info = {
            "full_name": full_name,
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "stars": repo["stargazers_count"],
            "description": repo["description"] or "No description",
            "language": repo.get("language")
        }
        
        logger.info(f"➡️  {full_name} tekshirilmoqda...")
        result = download_repo_zip(full_name)
        
        if result == "TOO_LARGE":
            # Katta repo – uni eslab qolamiz va keyingi safar tekshirmaymiz
            save_sent_repo(full_name)
            logger.info(f"{full_name} 50MB dan katta, keyingi safar tekshirilmaydi.")
            continue
        elif result is None:
            # Boshqa xatolik (branch topilmadi va h.k.) – hozircha eslab qolmaymiz, keyin qayta tekshiriladi
            logger.warning(f"{full_name} yuklab olinmadi, keyingi safar qayta tekshiriladi.")
            continue
        else:
            zip_path = result
            success = send_to_telegram(zip_path, repo_info)
            try:
                os.remove(zip_path)
            except:
                pass
            if success:
                save_sent_repo(full_name)
                new_repos_count += 1
            
            time.sleep(2)  # Telegram rate limit uchun
    
    logger.info(f"✅ {new_repos_count} ta yangi repo kanalga yuborildi.")

# ------------------- SCHEDULER VA FLASK -------------------
def run_scheduler():
    schedule.every(5).minutes.do(process_popular_repos)
    while True:
        schedule.run_pending()
        time.sleep(1)

app = Flask(__name__)

@app.route('/')
def health():
    return "Bot ishlayapti!", 200

if __name__ == "__main__":
    logger.info("🤖 Bot ishga tushdi.")
    import threading
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Dastlab bir marta ishga tushirish
    process_popular_repos()
    
    app.run(host="0.0.0.0", port=PORT)
