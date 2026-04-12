import os
import tempfile
import requests
import time
import logging
import psycopg2
import psycopg2.extras
from datetime import datetime
from flask import Flask, jsonify
import threading
import schedule
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ==================== KONFIGURATSIYA ====================
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL_ID = os.environ.get("CHANNEL_ID")
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")
PORT = int(os.environ.get("PORT", 5000))

# ============ RATE LIMIT SOZLAMALARI ============
GITHUB_REQUEST_DELAY = 0.5
TELEGRAM_MESSAGE_DELAY = 1.0

# ============ BOT SOZLAMALARI ============
REPOS_PER_RUN = int(os.environ.get("REPOS_PER_RUN", 10))
SEARCH_QUERY = "stars:>100"  # 100+ yulduz (kamroq, lekin sifatli)
PER_PAGE = 100
MAX_SIZE_MB = 45
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
BRANCHES = ["main", "master"]

# GitHub API maksimal 1000 natija qaytaradi
MAX_GITHUB_PAGE = 10  # 1000 / 100 = 10 sahifa

# ============ LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==================== RATE LIMITER ====================

class RateLimiter:
    def __init__(self, delay=1.0):
        self.delay = delay
        self.last_request = None
    
    def wait(self):
        if self.last_request:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self.last_request = time.time()

github_limiter = RateLimiter(delay=GITHUB_REQUEST_DELAY)
telegram_limiter = RateLimiter(delay=TELEGRAM_MESSAGE_DELAY)

# ==================== POSTGRESQL FUNKSIYALARI ====================

def get_db_connection():
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL topilmadi!")
        return None
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    """Ma'lumotlar bazasi jadvallarini yaratish"""
    try:
        conn = get_db_connection()
        if not conn:
            logger.error("❌ Ma'lumotlar bazasiga ulanish mumkin emas")
            return False
        
        cur = conn.cursor()
        
        # sent_repos jadvali - UNIQUE constraint bilan (takrorlanmaslik uchun)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sent_repos (
                id SERIAL PRIMARY KEY,
                full_name VARCHAR(255) UNIQUE NOT NULL,
                name VARCHAR(255),
                owner VARCHAR(255),
                stars INTEGER,
                description TEXT,
                language VARCHAR(100),
                size_mb FLOAT,
                sent_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # bot_state jadvali
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key VARCHAR(50) PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        
        # bot_state_index jadvali
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state_index (
                key VARCHAR(50) PRIMARY KEY,
                value INTEGER NOT NULL
            )
        """)
        
        # Boshlang'ich qiymatlar
        cur.execute("""
            INSERT INTO bot_state (key, value) 
            VALUES ('current_page', 1)
            ON CONFLICT (key) DO NOTHING
        """)
        
        cur.execute("""
            INSERT INTO bot_state_index (key, value) 
            VALUES ('current_index', 0)
            ON CONFLICT (key) DO NOTHING
        """)
        
        conn.commit()
        logger.info("✅ PostgreSQL jadvallari tayyor")
        
        cur.close()
        conn.close()
        return True
        
    except Exception as e:
        logger.error(f"❌ init_db xatosi: {e}")
        return False

def get_current_page():
    """Joriy sahifa raqamini olish"""
    try:
        conn = get_db_connection()
        if not conn:
            return 1
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_state WHERE key = 'current_page'")
        result = cur.fetchone()
        cur.close()
        conn.close()
        page = result[0] if result else 1
        
        if page > MAX_GITHUB_PAGE:
            logger.info(f"📌 Sahifa {page} GitHub limitidan oshdi, 1-sahifaga qaytish")
            update_current_page(1)
            return 1
        return page
    except Exception as e:
        logger.error(f"get_current_page xatosi: {e}")
        return 1

def update_current_page(page):
    """Joriy sahifani yangilash"""
    if page > MAX_GITHUB_PAGE:
        page = 1
        logger.info(f"🔄 Sahifa limitdan oshdi, 1 ga qaytarildi")
    
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bot_state (key, value) VALUES ('current_page', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (page,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"update_current_page xatosi: {e}")

def get_current_index():
    """Joriy indeksni olish"""
    try:
        conn = get_db_connection()
        if not conn:
            return 0
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_state_index WHERE key = 'current_index'")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0] if result else 0
    except Exception as e:
        logger.error(f"get_current_index xatosi: {e}")
        return 0

def update_current_index(index):
    """Joriy indeksni yangilash"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO bot_state_index (key, value) VALUES ('current_index', %s)
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
        """, (index,))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"update_current_index xatosi: {e}")

def is_repo_sent(full_name):
    """Repository oldin yuborilganmi tekshirish"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("SELECT id FROM sent_repos WHERE full_name = %s", (full_name,))
        result = cur.fetchone() is not None
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"is_repo_sent xatosi: {e}")
        return False

def save_repo(repo_info, size_mb):
    """Yuborilgan reponi saqlash"""
    try:
        conn = get_db_connection()
        if not conn:
            return False
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO sent_repos (full_name, name, owner, stars, description, language, size_mb)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (full_name) DO NOTHING
        """, (
            repo_info['full_name'],
            repo_info['name'],
            repo_info['owner'],
            repo_info['stars'],
            repo_info['description'],
            repo_info['language'],
            size_mb
        ))
        conn.commit()
        cur.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"save_repo xatosi: {e}")
        return False

def get_total_sent():
    """Jami yuborilgan repolar soni"""
    try:
        conn = get_db_connection()
        if not conn:
            return 0
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM sent_repos")
        result = cur.fetchone()[0]
        cur.close()
        conn.close()
        return result
    except Exception as e:
        logger.error(f"get_total_sent xatosi: {e}")
        return 0

# ==================== GITHUB API ====================

def github_request(url, params=None):
    """GitHub API ga so'rov yuborish (faqat search uchun)"""
    github_limiter.wait()
    
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        
        if resp.status_code == 403:
            remaining = resp.headers.get('X-RateLimit-Remaining')
            if remaining == '0':
                reset_time = int(resp.headers.get('X-RateLimit-Reset', 0))
                wait_time = reset_time - time.time()
                if wait_time > 0:
                    logger.warning(f"⚠️ GitHub rate limit! {wait_time:.0f} sekund kutish")
                    time.sleep(wait_time + 5)
                    return github_request(url, params)
        
        return resp
    except Exception as e:
        logger.error(f"github_request xatosi: {e}")
        return None

def search_repos_page(page):
    """GitHub Search API - faqat 1-10 sahifalar ishlaydi"""
    
    if page > MAX_GITHUB_PAGE:
        logger.info(f"🏁 Sahifa {page} GitHub limitidan oshdi ({MAX_GITHUB_PAGE} maksimal)")
        update_current_page(1)
        update_current_index(0)
        return []
    
    url = "https://api.github.com/search/repositories"
    params = {
        "q": SEARCH_QUERY,
        "sort": "stars",
        "order": "desc",
        "per_page": PER_PAGE,
        "page": page
    }
    
    resp = github_request(url, params)
    if resp and resp.status_code == 200:
        data = resp.json()
        items = data.get("items", [])
        total_count = data.get("total_count", 0)
        logger.info(f"✅ GitHub dan {len(items)} ta repo topildi (page {page}/{MAX_GITHUB_PAGE}, total: {total_count})")
        
        if not items and page > 1:
            logger.info(f"📌 Sahifa {page} bo'sh, 1-sahifaga qaytish")
            update_current_page(1)
            update_current_index(0)
            return []
        
        return items
    elif resp:
        logger.error(f"❌ GitHub search xatosi: {resp.status_code}")
        if resp.status_code == 422:
            logger.info("📌 422 xatosi - GitHub limitiga yetildi, 1-sahifaga qaytish")
            update_current_page(1)
            update_current_index(0)
    return []

# ==================== REPO YUKLASH (API LIMITSIZ) ====================

def get_repo_size_no_api(full_name):
    """
    API ishlatmasdan repository hajmini aniqlash
    Faqat HEAD so'rovi orqali Content-Length ni olish
    """
    for branch in BRANCHES:
        zip_url = f"https://github.com/{full_name}/archive/refs/heads/{branch}.zip"
        
        try:
            resp = requests.head(zip_url, timeout=10)
            if resp.status_code == 200:
                size_bytes = resp.headers.get('Content-Length')
                if size_bytes:
                    size_mb = int(size_bytes) / (1024 * 1024)
                    return size_mb
        except Exception as e:
            logger.debug(f"Branch {branch} HEAD xatosi: {e}")
            continue
    
    return None

def download_repo_zip(full_name):
    """
    Repository ni ZIP sifatida yuklab olish (API limitsiz)
    """
    for branch in BRANCHES:
        zip_url = f"https://github.com/{full_name}/archive/refs/heads/{branch}.zip"
        
        try:
            # HEAD so'rovi - hajmni tekshirish
            head_resp = requests.head(zip_url, allow_redirects=True, timeout=10)
            
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get('Content-Length')
                
                if content_length:
                    size_bytes = int(content_length)
                    size_mb = size_bytes / (1024 * 1024)
                    
                    if size_bytes > MAX_SIZE_BYTES:
                        logger.info(f"⚠️ {full_name} hajmi {size_mb:.1f} MB – 45MB dan katta")
                        return "TOO_LARGE", size_mb
                
                # ZIP ni yuklab olish
                zip_resp = requests.get(zip_url, stream=True, timeout=60)
                
                if zip_resp.status_code == 200:
                    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".zip")
                    downloaded = 0
                    
                    for chunk in zip_resp.iter_content(chunk_size=8192):
                        downloaded += len(chunk)
                        if downloaded > MAX_SIZE_BYTES:
                            temp_file.close()
                            os.unlink(temp_file.name)
                            return "TOO_LARGE", downloaded / (1024 * 1024)
                        temp_file.write(chunk)
                    
                    temp_file.close()
                    logger.info(f"✅ Yuklab olindi: {full_name}")
                    return temp_file.name, downloaded / (1024 * 1024)
                    
        except Exception as e:
            logger.debug(f"Branch {branch} yuklab olish xatosi: {e}")
            continue
    
    return None, 0

# ==================== TELEGRAM ====================

def send_to_telegram(file_path, repo_info, size_mb):
    """Telegramga fayl yuborish"""
    telegram_limiter.wait()
    
    caption = f"""
<b>📦 {repo_info['name']}</b>

👤 <b>Muallif:</b> <code>{repo_info['owner']}</code>
⭐ <b>Yulduzlar:</b> {repo_info['stars']}
🔧 <b>Til:</b> {repo_info.get('language', 'Nomaʼlum')}
📦 <b>Hajmi:</b> {size_mb:.1f} MB

📝 <b>Tavsif:</b>
{repo_info['description'][:200]}...

🔗 <a href="https://github.com/{repo_info['full_name']}">GitHub da ko‘rish</a>

🕒 {datetime.now().strftime('%Y-%m-%d %H:%M')}
"""
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    
    for attempt in range(3):
        try:
            with open(file_path, "rb") as f:
                files = {"document": f}
                data = {
                    "chat_id": CHANNEL_ID,
                    "caption": caption,
                    "parse_mode": "HTML"
                }
                resp = requests.post(url, files=files, data=data, timeout=120)
            
            if resp.status_code == 200:
                logger.info(f"✅ Yuborildi: {repo_info['full_name']}")
                return True
            elif resp.status_code == 429:
                retry_after = resp.json().get('parameters', {}).get('retry_after', 30)
                logger.warning(f"⚠️ Telegram rate limit! {retry_after} sekund kutish")
                time.sleep(retry_after)
            else:
                logger.error(f"❌ Telegram xatosi: {resp.status_code}")
                if attempt < 2:
                    time.sleep(5)
                
        except requests.exceptions.Timeout:
            logger.error(f"❌ Telegram timeout (urinish {attempt+1}/3)")
            if attempt < 2:
                time.sleep(10)
        except Exception as e:
            logger.error(f"❌ Telegram xatosi: {e}")
            if attempt < 2:
                time.sleep(5)
    
    return False

# ==================== ASOSIY JOB ====================

def process_repos_batch():
    """
    Har bir ishga tushganda REPOS_PER_RUN ta repo yuboradi
    Bitta reponi qayta-qayta yubormaslik uchun is_repo_sent() tekshiruvi
    """
    sent_count = 0
    checked_pages = 0
    MAX_PAGES_TO_CHECK = 5
    
    logger.info(f"🚀 Boshlanyapti! {REPOS_PER_RUN} ta repo yuboriladi")
    
    while sent_count < REPOS_PER_RUN and checked_pages < MAX_PAGES_TO_CHECK:
        current_page = get_current_page()
        current_index = get_current_index()
        
        logger.info(f"🔍 Sahifa {current_page}/{MAX_GITHUB_PAGE}, index {current_index}")
        
        repos = search_repos_page(current_page)
        if not repos:
            next_page = current_page + 1
            if next_page > MAX_GITHUB_PAGE:
                next_page = 1
                logger.info(f"🔄 Barcha sahifalar tekshirildi, 1-sahifaga qaytish")
            update_current_page(next_page)
            update_current_index(0)
            checked_pages += 1
            continue
        
        for i in range(current_index, len(repos)):
            repo = repos[i]
            full_name = repo["full_name"]
            
            # MUHIM: Oldin yuborilganmi tekshirish (takrorlanmaslik uchun)
            if is_repo_sent(full_name):
                logger.debug(f"⏭️ {full_name} oldin yuborilgan – o'tkazib yuborildi")
                update_current_index(i + 1)
                continue
            
            # API limitsiz hajmni tekshirish
            size_mb = get_repo_size_no_api(full_name)
            if size_mb is None:
                logger.debug(f"❌ {full_name} hajmini aniqlab bo'lmadi")
                update_current_index(i + 1)
                continue
            
            if size_mb > MAX_SIZE_MB:
                logger.info(f"⚠️ {full_name} hajmi {size_mb:.1f} MB – tashlab ketildi")
                update_current_index(i + 1)
                continue
            
            repo_info = {
                "full_name": full_name,
                "name": repo["name"],
                "owner": repo["owner"]["login"],
                "stars": repo["stargazers_count"],
                "description": repo["description"] or "Tavsif mavjud emas",
                "language": repo.get("language")
            }
            
            # ZIP yuklab olish
            result, actual_size = download_repo_zip(full_name)
            
            if result == "TOO_LARGE":
                update_current_index(i + 1)
                continue
            elif result is None:
                update_current_index(i + 1)
                continue
            
            # Telegramga yuborish
            success = send_to_telegram(result, repo_info, actual_size)
            
            # Vaqtinchalik faylni o'chirish
            try:
                os.unlink(result)
            except:
                pass
            
            if success:
                save_repo(repo_info, actual_size)
                sent_count += 1
                logger.info(f"✅ {sent_count}/{REPOS_PER_RUN} yuborildi: {full_name}")
                update_current_index(i + 1)
                
                if sent_count >= REPOS_PER_RUN:
                    break
            else:
                # Yuborilmadi, lekin indeksni oshirmaymiz (keyingi safar qayta urinadi)
                logger.warning(f"⚠️ {full_name} yuborilmadi, keyingi safar qayta uriniladi")
        
        if sent_count < REPOS_PER_RUN:
            next_page = current_page + 1
            if next_page > MAX_GITHUB_PAGE:
                next_page = 1
                logger.info(f"🔄 Barcha sahifalar tekshirildi, 1-sahifaga qaytish")
            update_current_page(next_page)
            update_current_index(0)
            checked_pages += 1
            logger.info(f"➡️ Keyingi sahifa: {next_page}")
    
    logger.info(f"✅ Yakunlandi! Yuborilgan: {sent_count}")

# ==================== SCHEDULER ====================

def run_scheduler():
    schedule.every(5).minutes.do(process_repos_batch)
    logger.info("⏰ Scheduler ishga tushdi: har 5 minutda")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# ==================== FLASK ====================

app = Flask(__name__)

@app.route('/')
def health():
    return jsonify({
        "status": "running",
        "total_sent": get_total_sent(),
        "current_page": get_current_page(),
        "max_page": MAX_GITHUB_PAGE
    })

@app.route('/stats')
def stats():
    return jsonify({
        "total_sent": get_total_sent(),
        "current_page": get_current_page(),
        "current_index": get_current_index(),
        "max_page": MAX_GITHUB_PAGE,
        "repos_per_run": REPOS_PER_RUN,
        "search_query": SEARCH_QUERY
    })

@app.route('/trigger', methods=['POST'])
def trigger():
    threading.Thread(target=process_repos_batch).start()
    return jsonify({"status": "started"})

@app.route('/reset', methods=['POST'])
def reset():
    update_current_page(1)
    update_current_index(0)
    return jsonify({"status": "reset", "current_page": 1})

# ==================== ASOSIY ====================

if __name__ == "__main__":
    logger.info("="*50)
    logger.info("🤖 GitHub Repo Monitor Bot v3.0")
    logger.info("="*50)
    logger.info(f"⚡ Har bir ishga tushganda: {REPOS_PER_RUN} ta repo")
    logger.info(f"🔍 Qidiruv: {SEARCH_QUERY}")
    logger.info(f"📄 GitHub maksimal sahifa: {MAX_GITHUB_PAGE}")
    logger.info(f"📦 Maksimal hajm: {MAX_SIZE_MB} MB")
    logger.info("="*50)
    
    if not BOT_TOKEN or not CHANNEL_ID:
        logger.error("❌ BOT_TOKEN yoki CHANNEL_ID topilmadi!")
        exit(1)
    
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL topilmadi!")
        exit(1)
    
    if GITHUB_TOKEN:
        logger.info("✅ GitHub token topildi")
    else:
        logger.warning("⚠️ GitHub token topilmadi! Limit: 60 so'rov/soat")
    
    # Ma'lumotlar bazasini ishga tushirish
    if not init_db():
        logger.error("❌ Ma'lumotlar bazasini ishga tushirib bo'lmadi!")
        exit(1)
    
    # Scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Flask server
    logger.info(f"🌐 Flask server: http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
