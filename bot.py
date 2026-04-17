import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN
from database import init_db
from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats

logging.basicConfig(level=logging.INFO)


async def main():
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Регистрируем все роутеры
    dp.include_router(start.router)
    dp.include_router(clients.router)
    dp.include_router(appointments.router)
    dp.include_router(settings.router)
    dp.include_router(stats.router)

    # Инициализируем базу данных
    await init_db()

    # Запускаем планировщик напоминаний (каждый день в 10:00)
    setup_scheduler(bot)

    print("✅ Beauty Book бот запущен!")
    print("🔔 Напоминания включены (каждый день в 10:00)")

    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
