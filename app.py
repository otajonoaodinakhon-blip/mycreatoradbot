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
# GitHub limit: 5000 so'rov/soat (token bilan)
# Har bir repo tekshirish uchun 2 ta so'rov ketadi (search + size)
# Xavfsizlik uchun 1 sekund/so'rov
GITHUB_REQUEST_DELAY = 1.0  # 1 sekund

# Telegram limit: 30 xabar/sekund (nazariy), lekin fayl yuklash sekinroq
# Xavfsizlik uchun 1 xabar/sekund (kanalga yuborayotganda)
TELEGRAM_MESSAGE_DELAY = 1.0

# ============ BOT SOZLAMALARI ============
REPOS_PER_RUN = int(os.environ.get("REPOS_PER_RUN", 10))  # Har bir ishga tushganda yuboriladigan repo soni
SEARCH_QUERY = "stars:>10"  # 10+ yulduzli repolar
PER_PAGE = 100  # GitHub API maksimal 100
MAX_SIZE_MB = 45  # 45 MB (xavfsizlik chegarasi, Telegram 50 MB)
MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024
BRANCHES = ["main", "master"]

# ============ LOGGING ============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ==================== RATE LIMITER KLASSLARI ====================

class RateLimiter:
    """Umumiy rate limit boshqaruvchisi"""
    def __init__(self, delay=1.0):
        self.delay = delay
        self.last_request = None
    
    def wait(self):
        """So'rovdan oldin kerakli vaqtni kutish"""
        if self.last_request:
            elapsed = time.time() - self.last_request
            if elapsed < self.delay:
                time.sleep(self.delay - elapsed)
        self.last_request = time.time()

# Rate limiter obyektlari
github_limiter = RateLimiter(delay=GITHUB_REQUEST_DELAY)
telegram_limiter = RateLimiter(delay=TELEGRAM_MESSAGE_DELAY)

# ==================== POSTGRESQL FUNKSIYALARI ====================

def get_db_connection():
    """PostgreSQL ga ulanish"""
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable topilmadi!")
        return None
    return psycopg2.connect(DATABASE_URL, sslmode='require')

def init_db():
    """Jadvallarni yaratish"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        
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
                size_mb FLOAT,
                sent_at TIMESTAMP DEFAULT NOW()
            )
        """)
        logger.info("✅ sent_repos jadvali tayyor")
        
        # bot_state jadvali (sahifa)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state (
                key VARCHAR(50) PRIMARY KEY,
                value INT NOT NULL
            )
        """)
        
        # current_page
        cur.execute("SELECT value FROM bot_state WHERE key = 'current_page'")
        if cur.fetchone() is None:
            cur.execute("INSERT INTO bot_state (key, value) VALUES ('current_page', 1)")
            logger.info("✅ current_page = 1 qo'shildi")
        
        # current_index (sahifa ichidagi index)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS bot_state_index (
                key VARCHAR(50) PRIMARY KEY,
                value INT NOT NULL
            )
        """)
        cur.execute("SELECT value FROM bot_state_index WHERE key = 'current_index'")
        if cur.fetchone() is None:
            cur.execute("INSERT INTO bot_state_index (key, value) VALUES ('current_index', 0)")
            logger.info("✅ current_index = 0 qo'shildi")
        
        conn.commit()
        logger.info("✅ PostgreSQL jadvallari tayyor")
        
    except Exception as e:
        logger.error(f"❌ init_db xatosi: {e}")
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

def get_current_page():
    """Qaysi sahifada qolganini olish"""
    try:
        conn = get_db_connection()
        if not conn:
            return 1
        cur = conn.cursor()
        cur.execute("SELECT value FROM bot_state WHERE key = 'current_page'")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result[0] if result else 1
    except Exception as e:
        logger.error(f"get_current_page xatosi: {e}")
        return 1

def update_current_page(page):
    """Yangi sahifani saqlash"""
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
        logger.debug(f"current_page yangilandi: {page}")
    except Exception as e:
        logger.error(f"update_current_page xatosi: {e}")

def get_current_index():
    """Sahifa ichidagi qaysi repodan davom etish"""
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
    """Sahifa ichidagi indexni yangilash"""
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
        logger.debug(f"current_index yangilandi: {index}")
    except Exception as e:
        logger.error(f"update_current_index xatosi: {e}")

def is_repo_sent(full_name):
    """Repo avval yuborilganmi?"""
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
    """Yuborilgan reponi bazaga qo'shish"""
    try:
        conn = get_db_connection()
        if not conn:
            return
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
    except Exception as e:
        logger.error(f"save_repo xatosi: {e}")

def save_large_repo(full_name, size_mb):
    """Katta reponi eslab qolish"""
    try:
        conn = get_db_connection()
        if not conn:
            return
        cur = conn.cursor()
        repo_name = full_name.split('/')[-1]
        cur.execute("""
            INSERT INTO sent_repos (full_name, name, size_mb)
            VALUES (%s, %s, %s)
            ON CONFLICT (full_name) DO NOTHING
        """, (full_name, repo_name, size_mb))
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"save_large_repo xatosi: {e}")

def get_total_sent():
    """Jami yuborilgan repo soni"""
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

# ==================== GITHUB API FUNKSIYALARI ====================

def github_request(url, params=None):
    """GitHub API ga rate limit bilan so'rov yuborish"""
    github_limiter.wait()
    
    headers = {"Accept": "application/vnd.github.v3+json"}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"
    
    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        
        # Rate limitni tekshirish
        if resp.status_code == 403:
            remaining = resp.headers.get('X-RateLimit-Remaining')
            if remaining == '0':
                reset_time = int(resp.headers.get('X-RateLimit-Reset', 0))
                wait_time = reset_time - time.time()
                if wait_time > 0:
                    logger.warning(f"⚠️ GitHub rate limit! {wait_time:.0f} sekund kutish kerak")
                    time.sleep(wait_time + 5)
                    return github_request(url, params)  # Qayta urinish
        
        return resp
    except Exception as e:
        logger.error(f"github_request xatosi: {e}")
        return None

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
    
    resp = github_request(url, params)
    if resp and resp.status_code == 200:
        data = resp.json()
        items = data.get("items", [])
        logger.info(f"GitHub dan {len(items)} ta repo topildi (page {page})")
        return items
    elif resp:
        logger.error(f"GitHub search xatosi: {resp.status_code}")
    return []

def get_repo_size(repo_full_name):
    """Reponing hajmini MB da qaytaradi"""
    url = f"https://api.github.com/repos/{repo_full_name}"
    resp = github_request(url)
    
    if resp and resp.status_code == 200:
        data = resp.json()
        size_kb = data.get("size", 0)
        size_mb = size_kb / 1024
        logger.debug(f"{repo_full_name} hajmi: {size_mb:.1f} MB")
        return size_mb
    else:
        logger.error(f"Repo info xatosi {repo_full_name}: {resp.status_code if resp else 'No response'}")
        return None

# ==================== REPO YUKLASH FUNKSIYALARI ====================

def download_repo_zip(repo_full_name):
    """Reponi ZIP sifatida yuklab oladi"""
    for branch in BRANCHES:
        zip_url = f"https://github.com/{repo_full_name}/archive/refs/heads/{branch}.zip"
        
        try:
            # HEAD so'rov orqali hajmni tekshirish
            head_resp = requests.head(zip_url, allow_redirects=True, timeout=10)
            
            if head_resp.status_code == 200:
                content_length = head_resp.headers.get('Content-Length')
                
                if content_length:
                    size_bytes = int(content_length)
                    size_mb = size_bytes / (1024 * 1024)
                    
                    if size_bytes > MAX_SIZE_BYTES:
                        logger.info(f"⚠️ {repo_full_name} hajmi {size_mb:.1f} MB – {MAX_SIZE_MB} MB dan katta")
                        return "TOO_LARGE", size_mb
                
                # Yuklab olish
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
                    logger.info(f"✅ Yuklab olindi: {repo_full_name} ({branch})")
                    return temp_file.name, downloaded / (1024 * 1024)
                    
            else:
                logger.debug(f"Branch {branch} topilmadi (status {head_resp.status_code})")
                
        except Exception as e:
            logger.debug(f"Branch {branch} tekshirishda xato: {e}")
            continue
    
    logger.error(f"❌ Hech qanday branch ishlamadi: {repo_full_name}")
    return None, 0

# ==================== TELEGRAM YUBORISH FUNKSIYALARI ====================

def send_to_telegram(file_path, repo_info, size_mb):
    """ZIP faylni Telegram kanaliga rate limit bilan yuboradi"""
    
    # Rate limitni kutish
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
            error_data = resp.json()
            retry_after = error_data.get('parameters', {}).get('retry_after', 30)
            logger.warning(f"⚠️ Telegram rate limit! {retry_after} sekund kutish kerak")
            time.sleep(retry_after)
            return False
        else:
            logger.error(f"❌ Telegram xatosi: {resp.status_code}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Telegramga yuborishda xato: {e}")
        return False

# ==================== ASOSIY JOB ====================

def process_repos_batch():
    """Har bir ishga tushganda REPOS_PER_RUN ta repo yuboradi"""
    sent_count = 0
    skipped_count = 0
    large_count = 0
    
    logger.info(f"🚀 Boshlanyapti! {REPOS_PER_RUN} ta repo yuboriladi")
    
    while sent_count < REPOS_PER_RUN:
        current_page = get_current_page()
        current_index = get_current_index()
        
        logger.info(f"🔍 Sahifa {current_page}, index {current_index} tekshirilmoqda...")
        
        # Sahifadagi repolarni olish
        repos = search_repos_page(current_page)
        if not repos:
            logger.info(f"Sahifa {current_page} bo'sh, keyingi sahifaga o'tish")
            update_current_page(current_page + 1)
            update_current_index(0)
            continue
        
        # Qolgan repolarni tekshirish
        for i in range(current_index, len(repos)):
            repo = repos[i]
            full_name = repo["full_name"]
            
            # Oldin yuborilganmi?
            if is_repo_sent(full_name):
                logger.debug(f"{full_name} oldin yuborilgan")
                skipped_count += 1
                continue
            
            # Repo hajmini tekshirish
            size_mb = get_repo_size(full_name)
            if size_mb is None:
                continue
            
            if size_mb > MAX_SIZE_MB:
                logger.info(f"⚠️ {full_name} hajmi {size_mb:.1f} MB – {MAX_SIZE_MB} MB dan katta")
                save_large_repo(full_name, size_mb)
                large_count += 1
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
                save_large_repo(full_name, actual_size)
                large_count += 1
                continue
            elif result is None:
                logger.warning(f"{full_name} yuklab olinmadi")
                continue
            
            # Telegramga yuborish
            success = send_to_telegram(result, repo_info, actual_size)
            
            # Faylni o'chirish
            try:
                os.remove(result)
            except:
                pass
            
            if success:
                save_repo(repo_info, actual_size)
                sent_count += 1
                logger.info(f"✅ {sent_count}/{REPOS_PER_RUN} yuborildi: {full_name}")
                
                # Indexni yangilash
                update_current_index(i + 1)
                
                if sent_count >= REPOS_PER_RUN:
                    break
            else:
                # Yuborilmadi, keyingi safar qayta urinish
                update_current_index(i)
                logger.warning(f"⚠️ {full_name} yuborilmadi, keyingi safar qayta uriniladi")
                return
        
        # Agar sahifa tugagan bo'lsa, keyingi sahifaga o'tish
        if sent_count < REPOS_PER_RUN:
            logger.info(f"Sahifa {current_page} tugadi, keyingi sahifaga o'tish")
            update_current_page(current_page + 1)
            update_current_index(0)
    
    logger.info(f"✅ Yakunlandi! Yuborilgan: {sent_count}, O'tkazib yuborilgan: {skipped_count}, Katta repolar: {large_count}")

# ==================== SCHEDULER ====================

def run_scheduler():
    """Scheduler thread"""
    schedule.every(5).minutes.do(process_repos_batch)
    logger.info("⏰ Scheduler ishga tushdi: har 5 minutda ishlaydi")
    
    while True:
        schedule.run_pending()
        time.sleep(1)

# ==================== FLASK SERVER ====================

app = Flask(__name__)

@app.route('/')
def health():
    """Health check endpoint"""
    return jsonify({
        "status": "running",
        "version": "2.0.0",
        "timestamp": datetime.now().isoformat()
    }), 200

@app.route('/stats')
def stats():
    """Statistika endpoint"""
    return jsonify({
        "total_sent": get_total_sent(),
        "current_page": get_current_page(),
        "current_index": get_current_index(),
        "repos_per_run": REPOS_PER_RUN,
        "config": {
            "max_size_mb": MAX_SIZE_MB,
            "search_query": SEARCH_QUERY,
            "github_request_delay": GITHUB_REQUEST_DELAY,
            "telegram_delay": TELEGRAM_MESSAGE_DELAY
        }
    }), 200

@app.route('/trigger', methods=['POST'])
def trigger():
    """Manual trigger endpoint"""
    try:
        process_repos_batch()
        return jsonify({"status": "success", "message": "Bot ishga tushirildi"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ==================== ASOSIY ISHGA TUSHIRISH ====================

if __name__ == "__main__":
    logger.info("="*50)
    logger.info("🤖 GitHub Repo Monitor Bot v2.0")
    logger.info("="*50)
    logger.info(f"⚡ Har bir ishga tushganda: {REPOS_PER_RUN} ta repo")
    logger.info(f"⏱️  Har 5 minutda ishlaydi")
    logger.info(f"📦 Maksimal fayl hajmi: {MAX_SIZE_MB} MB")
    logger.info(f"🐙 GitHub rate limit: 5000 so'rov/soat (token bilan)")
    logger.info(f"📱 Telegram rate limit: 1 xabar/sekund")
    logger.info("="*50)
    
    # Environment variables tekshirish
    if not BOT_TOKEN:
        logger.error("❌ BOT_TOKEN topilmadi!")
        exit(1)
    if not CHANNEL_ID:
        logger.error("❌ CHANNEL_ID topilmadi!")
        exit(1)
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL topilmadi!")
        exit(1)
    
    if GITHUB_TOKEN:
        logger.info("✅ GitHub token topildi (5000 so'rov/soat)")
    else:
        logger.warning("⚠️ GitHub token topilmadi! Limit: 60 so'rov/soat")
    
    # Bazani tayyorlash
    init_db()
    
    # Scheduler thread
    scheduler_thread = threading.Thread(target=run_scheduler, daemon=True)
    scheduler_thread.start()
    
    # Dastlab bir marta ishga tushirish
    logger.info("🚀 Dastlabki ishga tushirish...")
    process_repos_batch()
    
    # Flask server
    logger.info(f"🌐 Flask server ishga tushmoqda: http://0.0.0.0:{PORT}")
    app.run(host="0.0.0.0", port=PORT)
