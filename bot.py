import asyncio
import logging
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, WEBHOOK_URL, WEBHOOK_SECRET, PORT
from database import init_db
from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats

logging.basicConfig(level=logging.INFO)


def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(start.router)
    dp.include_router(clients.router)
    dp.include_router(appointments.router)
    dp.include_router(settings.router)
    dp.include_router(stats.router)
    return dp


# ─── Webhook-режим (Render / продакшн) ──────────────────────────────
def run_webhook():
    from aiohttp import web
    from aiogram.webhook.aiohttp_server import SimpleRequestHandler, setup_application

    bot = Bot(token=BOT_TOKEN)
    dp = build_dispatcher()

    async def on_startup(bot: Bot):
        await init_db()
        setup_scheduler(bot)
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
            drop_pending_updates=True,
        )
        print(f"✅ Beauty Book запущен в webhook-режиме")
        print(f"🌐 URL: {WEBHOOK_URL}/webhook")

    async def on_shutdown(bot: Bot):
        await bot.delete_webhook()

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    app = web.Application()
    SimpleRequestHandler(dispatcher=dp, bot=bot, secret_token=WEBHOOK_SECRET).register(
        app, path="/webhook"
    )
    setup_application(app, dp, bot=bot)
    web.run_app(app, host="0.0.0.0", port=PORT)


# ─── Polling-режим (локальная разработка) ────────────────────────────
async def run_polling():
    bot = Bot(token=BOT_TOKEN)
    dp = build_dispatcher()

    await init_db()
    setup_scheduler(bot)

    print("✅ Beauty Book запущен в polling-режиме (локально)")
    print("🔔 Напоминания включены (каждый день в 10:00)")

    await dp.start_polling(bot)


if __name__ == "__main__":
    if WEBHOOK_URL:
        run_webhook()
    else:
        asyncio.run(run_polling())
