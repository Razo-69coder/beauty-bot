from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from datetime import datetime, timedelta, timezone

from database import (
    get_all_masters, get_inactive_clients, get_reminder_days,
    get_appointments_for_reminder_24h, get_appointments_for_reminder_2h,
    mark_reminder_sent,
)

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

# Московское время (UTC+3)
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK).replace(tzinfo=None)


async def send_inactive_reminders(bot: Bot):
    """Ежедневно в 10:00 — напоминает мастеру о давно не приходивших клиентах"""
    masters = await get_all_masters()
    for master_id, telegram_id in masters:
        reminder_days = await get_reminder_days(telegram_id)
        clients = await get_inactive_clients(master_id, reminder_days)
        if not clients:
            continue

        text = f"🔔 *Напоминание!*\n\nЭти клиенты не приходили больше {reminder_days} дней:\n\n"
        for _, name, phone, last_visit, days_ago in clients[:5]:
            text += f"💅 *{name}* — {days_ago} дн. назад\n"
            text += f"   📱 {phone}\n\n"
        if len(clients) > 5:
            text += f"_...и ещё {len(clients) - 5} клиентов_\n\n"
        text += "Открой бот чтобы написать им 👇"

        try:
            await bot.send_message(telegram_id, text, parse_mode="Markdown")
        except Exception:
            pass


async def send_client_reminders_24h(bot: Bot):
    """Ежедневно в 18:00 — напоминает клиентам о записи завтра"""
    tomorrow = (now_msk() + timedelta(days=1)).strftime("%Y-%m-%d")
    appointments = await get_appointments_for_reminder_24h(tomorrow)

    for appt_id, client_tg_id, client_name, master_tg_id, date, time, procedure in appointments:
        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        try:
            await bot.send_message(
                client_tg_id,
                f"🔔 *Напоминание о записи*\n\n"
                f"Завтра, *{date_fmt}* в *{time}*\n"
                f"📋 {procedure}\n\n"
                f"Ждём вас!",
                parse_mode="Markdown"
            )
            await mark_reminder_sent(appt_id, "24h")
        except Exception:
            pass


async def send_client_reminders_2h(bot: Bot):
    """Каждые 30 минут — напоминает клиентам о записи через ~2 часа"""
    now = now_msk()
    target = now + timedelta(hours=2)
    target_date = target.strftime("%Y-%m-%d")
    # Окно ±15 минут от целевого времени
    time_from = (target - timedelta(minutes=15)).strftime("%H:%M")
    time_to = (target + timedelta(minutes=15)).strftime("%H:%M")

    appointments = await get_appointments_for_reminder_2h(target_date, time_from, time_to)

    for appt_id, client_tg_id, client_name, master_tg_id, date, time, procedure in appointments:
        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        try:
            await bot.send_message(
                client_tg_id,
                f"⏰ *Через 2 часа ваша запись!*\n\n"
                f"📅 {date_fmt} в *{time}*\n"
                f"📋 {procedure}\n\n"
                f"Не забудьте!",
                parse_mode="Markdown"
            )
            await mark_reminder_sent(appt_id, "2h")
        except Exception:
            pass


def setup_scheduler(bot: Bot):
    # Напоминание мастеру о неактивных клиентах
    scheduler.add_job(send_inactive_reminders, "cron", hour=10, minute=0, args=[bot])

    # Напоминание клиентам за 24 часа (в 18:00)
    scheduler.add_job(send_client_reminders_24h, "cron", hour=18, minute=0, args=[bot])

    # Напоминание клиентам за 2 часа (каждые 30 минут)
    scheduler.add_job(send_client_reminders_2h, "interval", minutes=30, args=[bot])

    scheduler.start()
