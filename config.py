import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
DB_PATH = "beauty_bot.db"

# Через сколько дней напоминать мастеру о клиенте
REMINDER_DAYS = 40

# Количество клиентов на одной странице списка
PAGE_SIZE = 10
