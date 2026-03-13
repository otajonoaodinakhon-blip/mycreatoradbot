import os
import requests
from flask import Flask, request
from telegram import Bot
from apscheduler.schedulers.background import BackgroundScheduler
import psycopg2

# ------------------------
# ENV variables
# ------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
CHANNEL = os.environ.get("CHANNEL")          # @kanalingiz
CHANNEL_ID = int(os.environ.get("CHANNEL_ID"))  # numeric channel id, xavfsizlik uchun
DEEPSEEK_KEY = os.environ.get("DEEPSEEK_KEY")
RENDER_URL = os.environ.get("RENDER_URL")    # https://yourapp.onrender.com
GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN")  # optional
DATABASE_URL = os.environ.get("DATABASE_URL")  # PostgreSQL

# ------------------------
# Telegram bot
# ------------------------
bot = Bot(token=BOT_TOKEN)

# ------------------------
# Flask app
# ------------------------
app = Flask(__name__)

# ------------------------
# PostgreSQL connection
# ------------------------
conn = psycopg2.connect(DATABASE_URL)
cursor = conn.cursor()

cursor.execute("""
CREATE TABLE IF NOT EXISTS repos(
    id SERIAL PRIMARY KEY,
    repo_url TEXT UNIQUE
);
""")
conn.commit()

# ------------------------
# DeepSeek AI caption
# ------------------------
def generate_caption(name, stars):
    prompt = f"""
Uzbek tilida qisqa Telegram caption yozing.
Link yozmang.

Loyiha: {name}
Yulduzlar: {stars}
"""
    url = "https://api.deepseek.com/v1/chat/completions"
    headers = {
        "Authorization": f"Bearer {DEEPSEEK_KEY}",
        "Content-Type": "application/json"
    }
    data = {"model": "deepseek-chat", "messages": [{"role": "user", "content": prompt}]}

    try:
        r = requests.post(url, json=data, headers=headers)
        return r.json()["choices"][0]["message"]["content"]
    except:
        return f"🔥 Yangi GitHub loyiha\n📦 {name}\n⭐ {stars} yulduz"

# ------------------------
# GitHub repo topish
# ------------------------
def get_repo():
    url = "https://api.github.com/search/repositories?q=stars:>0&sort=stars&order=desc&per_page=50"
    headers = {}
    if GITHUB_TOKEN:
        headers["Authorization"] = f"token {GITHUB_TOKEN}"

    data = requests.get(url, headers=headers).json()
    for repo in data.get("items", []):
        repo_url = repo["html_url"]
        name = repo["name"]
        stars = repo["stargazers_count"]
        owner = repo["owner"]["login"]

        cursor.execute("SELECT * FROM repos WHERE repo_url=%s", (repo_url,))
        if cursor.fetchone():
            continue
        return name, stars, owner, repo_url
    return None

# ------------------------
# ZIP yuklash va Telegramga yuborish
# ------------------------
def send_repo():
    repo = get_repo()
    if not repo:
        print("Yangi repo topilmadi")
        return

    name, stars, owner, repo_url = repo
    zip_url = f"https://github.com/{owner}/{name}/archive/refs/heads/main.zip"

    r = requests.get(zip_url)
    if len(r.content) > 50*1024*1024:
        print(f"{name} fayl kattaligi 50MB dan oshadi, o'tkazildi")
        return

    with open("repo.zip", "wb") as f:
        f.write(r.content)

    caption = generate_caption(name, stars)

    # Faqat CHANNEL_ID ga yuborish
    bot.send_document(
        chat_id=CHANNEL_ID,
        document=open("repo.zip", "rb"),
        caption=caption
    )

    cursor.execute("INSERT INTO repos(repo_url) VALUES(%s) ON CONFLICT DO NOTHING", (repo_url,))
    conn.commit()

    os.remove("repo.zip")
    print(f"{name} yuborildi!")

# ------------------------
# Scheduler (har 10 daqiqa)
# ------------------------
scheduler = BackgroundScheduler()
scheduler.add_job(send_repo, "interval", minutes=10)
scheduler.start()

# ------------------------
# Webhookni avtomatik o‘rnatish
# ------------------------
def set_webhook():
    webhook_url = f"{RENDER_URL}/{BOT_TOKEN}"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/setWebhook"
    r = requests.get(url, params={"url": webhook_url})
    print("Webhook set:", r.json())

set_webhook()  # Deploy paytida

# ------------------------
# Flask routes
# ------------------------
@app.route("/", methods=["GET"])
def home():
    return "Bot ishlayapti"

@app.route(f"/{BOT_TOKEN}", methods=["POST"])
def webhook():
    data = request.get_json()
    chat_id = data.get("message", {}).get("chat", {}).get("id")

    # Faqat o'z kanal ID dan kelgan xabarlar ishlatiladi
    if chat_id != CHANNEL_ID:
        return "Not allowed"

    # Agar kerak bo'lsa, xabarni ishlash logikasi shu yerda
    return "ok"

# ------------------------
# Flask run
# ------------------------
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)