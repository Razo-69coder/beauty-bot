from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from database import get_all_masters, get_inactive_clients, get_reminder_days

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


async def send_inactive_reminders(bot: Bot):
    """Каждый день в 10:00 отправляет мастеру список клиентов, которые давно не приходили"""
    masters = await get_all_masters()
    for master_id, telegram_id in masters:
        reminder_days = await get_reminder_days(telegram_id)
        clients = await get_inactive_clients(master_id, reminder_days)
        if not clients:
            continue

        text = f"🔔 *Напоминание!*\n\n"
        text += f"Эти клиенты не приходили больше {reminder_days} дней:\n\n"
        for cid, name, phone, last_visit, days_ago in clients[:5]:
            text += f"💅 *{name}* — {days_ago} дн. назад\n"
            text += f"   📱 {phone}\n\n"

        if len(clients) > 5:
            text += f"_...и ещё {len(clients) - 5} клиентов_\n\n"

        text += "Открой бот чтобы написать им 👇"

        try:
            await bot.send_message(telegram_id, text, parse_mode="Markdown")
        except Exception:
            pass


def setup_scheduler(bot: Bot):
    scheduler.add_job(
        send_inactive_reminders,
        trigger="cron",
        hour=10,
        minute=0,
        args=[bot]
    )
    scheduler.start()
