import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = os.getenv("DB_PATH", "beauty_bot.db")

# Webhook-режим: если WEBHOOK_URL задан — Render/продакшн, иначе — локальный polling
WEBHOOK_URL = os.getenv("WEBHOOK_URL", "")        # например: https://beauty-bot.onrender.com
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "beauty_book_secret_2024")
PORT = int(os.getenv("PORT", 8000))

# Через сколько дней напоминать мастеру о клиенте
REMINDER_DAYS = 40

# Количество клиентов на одной странице списка
PAGE_SIZE = 10
