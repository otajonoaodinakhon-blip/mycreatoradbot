import os
import tempfile
import requests
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from flask import Flask
import threading
import schedule

# ------------------- KONFIGURATSIYA -------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")  # Neon connection string
PORT = int(os.environ.get("PORT", 5000))

# GitHub Search
SEARCH_QUERY = "stars:>10"
PER_PAGE = 10
MAX_SIZE_KB = 50 * 1024  # 50 MB = 51200 KB
BRANCHES = ["main", "master"]

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ------------------- POSTGRESQL FUNKSIYALARI -------------------
def get_db_connection():
    """PostgreSQL ga ulanish (DATABASE_URL orqali)"""
    conn = psycopg2.connect(DATABASE_URL, sslmode='require')
    return conn

def init_db():
    """Jadvallarni yaratish (birinchi marta ishga tushganda)"""
    conn = get_db_connection()
    cur = conn.cursor()
    
    # sent_repos jadvali
    cur.execute("""
        CREATE TABLE IF NOT EXISTS sent_repos (
            id SERIAL PRIMARY KEY,
            full_name VARCHAR(255) UNIQUE NOT NULL,
            name VARCHAR(255),
            owner VARCHAR(255),
            stars INT,
            description TEXT,
            language VARCHAR(100),
            size_kb INT,
            sent_at TIMESTAMP DEFAULT NOW()
        )
    """)
    
    # bot_state jadvali (pagination uchun)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bot_state (
            key VARCHAR(50) PRIMARY KEY,
            value INT NOT NULL
        )
    """)
    
    # current_page mavjudligini tekshirish
    cur.execute("SELECT value FROM bot_state WHERE key = 'current_page'")
    if cur.fetchone() is None:
        cur.execute("INSERT INTO bot_state (key, value) VALUES ('current_page', 1)")
    
    conn.commit()
    cur.close()
    conn.close()
    logger.info("✅ PostgreSQL jadvallari tayyor")

def get_current_page():
    """Qaysi sahifada qolganini olish"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT value FROM bot_state WHERE key = 'current_page'")
    page = cur.fetchone()[0]
    cur.close()
    conn.close()
    return page

def update_current_page(page):
    """Yangi sahifani saqlash"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE bot_state SET value = %s WHERE key = 'current_page'", (page,))
    conn.commit()
    cur.close()
    conn.close()

def is_repo_sent(full_name):
    """Repo avval yuborilganmi?"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT id FROM sent_repos WHERE full_name = %s", (full_name,))
    result = cur.fetchone() is not None
    cur.close()
    conn.close()
    return result

def save_repo(repo_info, size_kb):
    """Yuborilgan reponi bazaga qo'shish"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sent_repos (full_name, name, owner, stars, description, language, size_kb)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (full_name) DO NOTHING
    """, (
        repo_info['full_name'],
        repo_info['name'],
        repo_info['owner'],
        repo_info['stars'],
        repo_info['description'],
        repo_info['language'],
        size_kb
    ))
    conn.commit()
    cur.close()
    conn.close()

def save_large_repo(full_name, size_kb):
    """Katta reponi ham eslab qolish (qayta tekshirmaslik uchun)"""
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO sent_repos (full_name, name, size_kb)
        VALUES (%s, %s, %s)
        ON CONFLICT (full_name) DO NOTHING
    """, (full_name, full_name.split('/')[-1], size_kb))
    conn.commit()
    cur.close()
    conn.close()

# ------------------- GITHUB SEARCH -------------------
def search_repos_page(page):
    """GitHub Search API orqali ma'lum sahifadagi repolarni qidiradi"""
    url = "https://api.github.com/search/repositories"
    params = {
        "q": SEARCH_QUERY,
        "sort": "stars",
        "order": "desc",
        "per_page": PER_PAGE,
        "page": page
    }
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            items = data.get("items", [])
            logger.info(f"GitHub dan {len(items)} ta repo topildi (page {page})")
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
            size_kb = data.get("size", 0)
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
            head_resp = requests.head(zip_url, allow_redirects=True, timeout=10)
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get('Content-Length')
                if content_length and int(content_length) > (50 * 1024 * 1024):
                    logger.info(f"{repo_full_name} ({branch}) hajmi {content_length} bayt – 50MB dan katta (HEAD), tashlab ketildi.")
                    return "TOO_LARGE"
                
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
    return None

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

# ------------------- ASOSIY JOB (PAGINATION) -------------------
def process_next_repo():
    """Bitta yangi reponi topib, yuklab, yuboradi"""
    current_page = get_current_page()
    logger.info(f"🔍 Sahifa {current_page} tekshirilmoqda...")
    
    repos = search_repos_page(current_page)
    if not repos:
        logger.warning("Bu sahifada repo topilmadi, 1-sahifaga qaytish")
        update_current_page(1)
        return
    
    sent_count = 0
    for repo in repos:
        full_name = repo["full_name"]
        
        # Oldin yuborilganmi?
        if is_repo_sent(full_name):
            logger.debug(f"{full_name} allaqachon yuborilgan, o‘tkazib yuborildi.")
            continue
        
        # Repo hajmini tekshirish
        size_kb = check_repo_size(full_name)
        if size_kb is None:
            continue
        
        if size_kb > MAX_SIZE_KB:
            logger.info(f"{full_name} hajmi {size_kb/1024:.1f} MB – 50 MB dan katta, tashlab ketildi.")
            save_large_repo(full_name, size_kb)
            continue
        
        # Repo ma'lumotlari
        repo_info = {
            "full_name": full_name,
            "name": repo["name"],
            "owner": repo["owner"]["login"],
            "stars": repo["stargazers_count"],
            "description": repo["description"] or "No description",
            "language": repo.get("language")
        }
        
        # ZIP yuklab olish
        result = download_repo_zip(full_name)
        if result == "TOO_LARGE":
            save_large_repo(full_name, size_kb)
            continue
        elif result is None:
            logger.warning(f"{full_name} yuklab olinmadi")
            continue
        else:
            zip_path = result
            success = send_to_telegram(zip_path, repo_info)
            try:
                os.remove(zip_path)
            except:
                pass
            
            if success:
                save_repo(repo_info, size_kb)
                sent_count += 1
                logger.info(f"✅ Yuborildi: {full_name}")
                
                # Bitta repo yubordik, keyingi sahifaga o'tamiz
                next_page = current_page + 1
                update_current_page(next_page)
                logger.info(f"➡️ Keyingi sahifa: {next_page}")
                return  # Bitta repo yubordik, to'xtaymiz
    
    # Agar bu sahifada yuborilmagan repolar bo'lmasa, keyingi sahifaga o'tish
    if sent_count == 0:
        logger.info(f"Sahifa {current_page} da yangi repolar yo'q, keyingi sahifaga o'tish")
        update_current_page(current_page + 1)

# ------------------- SCHEDULER -------------------
def run_scheduler():
    schedule.every(5).minutes.do(process_next_repo)
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
    logger.info("🤖 Bot ishga tushdi (PostgreSQL bilan)")
    
    # Bazani tayyorlash
    init_db()
    
    # Scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Dastlab bir marta ishga tushirish
    process_next_repo()
    
    # Flask server
    app.run(host="0.0.0.0", port=PORT)
