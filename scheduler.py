from apscheduler.schedulers.asyncio import AsyncIOScheduler
from aiogram import Bot
from datetime import datetime, timedelta, timezone

from database import (
    get_all_masters, get_inactive_clients, get_reminder_days,
    get_appointments_for_reminder_24h, get_appointments_for_reminder_2h,
    mark_reminder_sent,
    get_appointments_for_correction_reminder, mark_correction_reminder_sent,
    get_appointments_for_review, mark_review_sent,
    get_appointments_pending_deposit_24h, get_appointments_pending_deposit_2h,
    get_appointments_for_review_request,
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


async def send_correction_reminders(bot: Bot):
    """Ежедневно в 12:00 — напоминает клиентам о коррекции через 3 недели после визита"""
    three_weeks_ago = (now_msk() - timedelta(days=21)).strftime("%Y-%m-%d")
    appointments = await get_appointments_for_correction_reminder(three_weeks_ago)

    for appt_id, client_tg_id, client_name, master_name, procedure in appointments:
        try:
            await bot.send_message(
                client_tg_id,
                f"💅 *Привет, {client_name.split()[0]}!*\n\n"
                f"Прошло 3 недели после визита — самое время на коррекцию!\n\n"
                f"Запишитесь к мастеру {master_name} заранее 🗓",
                parse_mode="Markdown"
            )
            await mark_correction_reminder_sent(appt_id)
        except Exception:
            pass


async def send_review_requests(bot: Bot):
    """Каждые 30 минут — просит клиента оценить визит.
    
    Приоритет: сначала проверяем review_requested_at (установлен после нажатия "Услуга оказана"),
    затем — старая логика (через 2 часа после времени записи).
    """
    now = now_msk()
    
    appointments = await get_appointments_for_review_request(now.isoformat())
    
    for appt in appointments:
        from keyboards import review_rating_keyboard
        try:
            await bot.send_message(
                appt['client_telegram_id'],
                f"💅 *{appt['client_name'].split()[0]}, как прошёл визит?*\n\n"
                f"Оцените процедуру «{appt['procedure']}»:",
                reply_markup=review_rating_keyboard(appt['id']),
                parse_mode="Markdown",
            )
            await mark_review_sent(appt['id'])
        except Exception:
            pass
    
    if not appointments:
        target = now - timedelta(hours=2)
        target_date = target.strftime("%Y-%m-%d")
        time_from = (target - timedelta(minutes=15)).strftime("%H:%M")
        time_to = (target + timedelta(minutes=15)).strftime("%H:%M")

        appointments = await get_appointments_for_review(target_date, time_from, time_to)

        for appt_id, client_tg_id, client_id, master_id, client_name, master_name, procedure in appointments:
            from keyboards import review_rating_keyboard
            try:
                await bot.send_message(
                    client_tg_id,
                    f"💅 *{client_name.split()[0]}, как прошёл визит?*\n\n"
                    f"Оцените процедуру «{procedure}» у мастера {master_name}:",
                    reply_markup=review_rating_keyboard(appt_id),
                    parse_mode="Markdown",
                )
                await mark_review_sent(appt_id)
            except Exception:
                pass


async def send_payment_reminders_24h(bot: Bot):
    """Ежедневно в 19:00 — напоминает клиентам о невнесённой предоплате за 24 часа до визита."""
    tomorrow = (now_msk() + timedelta(days=1)).strftime("%Y-%m-%d")
    appointments = await get_appointments_pending_deposit_24h(tomorrow)

    for appt_id, client_tg_id, client_name, master_tg_id, date, time, deposit_pct, payment_card, payment_phone, payment_banks in appointments:
        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        time_str = f" в *{time}*" if time else ""

        rekv_parts = []
        if payment_card:
            rekv_parts.append(f"*Карта:* {payment_card}")
        if payment_phone:
            rekv_parts.append(f"*Телефон:* {payment_phone}")
        if payment_banks:
            rekv_parts.append(f"*Банки:* {payment_banks}")
        rekv_block = ""
        if rekv_parts:
            rekv_block = "\n\nРеквизиты для оплаты:\n" + "\n".join(rekv_parts)

        try:
            await bot.send_message(
                client_tg_id,
                f"⚠️ *Напоминание об оплате*\n\n"
                f"Завтра, *{date_fmt}*{time_str} у вас запись.\n\n"
                f"Для подтверждения необходима предоплата *{deposit_pct}%*.{rekv_block}\n\n"
                f"Пожалуйста, внесите оплату — мастер ждёт подтверждения 💳",
                parse_mode="Markdown"
            )
        except Exception:
            pass


async def send_payment_reminders_2h(bot: Bot):
    """Каждые 30 минут — напоминает об оплате через 2 часа после записи."""
    appointments = await get_appointments_pending_deposit_2h()
    
    for appt_id, client_tg_id, client_name, master_tg_id, date, time, deposit_pct, payment_card, payment_phone, payment_banks in appointments:
        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        time_str = f" в *{time}*" if time else ""

        rekv_parts = []
        if payment_card:
            rekv_parts.append(f"*Карта:* {payment_card}")
        if payment_phone:
            rekv_parts.append(f"*Телефон:* {payment_phone}")
        if payment_banks:
            rekv_parts.append(f"*Банки:* {payment_banks}")
        rekv_block = ""
        if rekv_parts:
            rekv_block = "\n\nРеквизиты для оплаты:\n" + "\n".join(rekv_parts)

        try:
            await bot.send_message(
                client_tg_id,
                f"💳 *Напоминание об оплате*\n\n"
                f"Вы записаны на *{date_fmt}*{time_str}.\n\n"
                f"Для подтверждения записи внесите предоплату *{deposit_pct}%*.{rekv_block}\n\n"
                f"После оплаты мастер подтвердит вашу запись ✅",
                parse_mode="Markdown"
            )
        except Exception:
            pass


async def send_birthday_greetings(bot: Bot):
    """Ежедневно в 9:00 MSK — отправляет поздравления с днём рождения клиентам."""
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.telegram_id, c.name, m.name as master_name, m.birthday_discount_percent
            FROM clients c JOIN masters m ON c.master_id = m.id
            WHERE c.telegram_id IS NOT NULL
            AND c.birthday IS NOT NULL
            AND m.birthday_discount_enabled = TRUE
            AND TO_CHAR(CURRENT_DATE, 'MM-DD') = c.birthday
        """)

    for telegram_id, name, master_name, discount_percent in rows:
        try:
            await bot.send_message(
                telegram_id,
                f"🎂 *С днём рождения, {name.split()[0]}!*\n\n"
                f"Мастер {master_name} поздравляет вас с праздником! 🎉\n\n"
                f"🎁 Скидка *{discount_percent}%* на следующий визит ждёт вас!\n"
                f"Запишитесь и напомните мастеру о скидке 💅",
                parse_mode="Markdown"
            )
        except Exception:
            pass


async def send_loyalty_notifications(bot: Bot):
    """Ежедневно в 20:00 MSK — отправляет уведомления о лояльности клиентам."""
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.telegram_id, c.name, m.name as master_name,
                   COUNT(a.id) as visit_count,
                   COALESCE(m.loyalty_threshold, 10) as threshold,
                   COALESCE(m.loyalty_discount_percent, 10) as discount_percent
            FROM clients c
            JOIN masters m ON c.master_id = m.id
            LEFT JOIN appointments a ON a.client_id = c.id AND a.status = 'completed'
            WHERE c.telegram_id IS NOT NULL
            AND m.loyalty_discount_enabled = TRUE
            GROUP BY c.id, c.name, c.telegram_id, m.name, m.loyalty_threshold, m.loyalty_discount_percent
            HAVING COUNT(a.id) > 0 AND COUNT(a.id) % COALESCE(m.loyalty_threshold, 10) = 0
        """)

    for telegram_id, name, master_name, visit_count, threshold, discount_percent in rows:
        try:
            await bot.send_message(
                telegram_id,
                f"🏆 *{name.split()[0]}, вы у нас уже {visit_count} раз!*\n\n"
                f"Вы заработали скидку *{discount_percent}%* на следующий визит 🎉\n\n"
                f"Запишитесь и скажите мастеру {master_name} что вы постоянный клиент 💅",
                parse_mode="Markdown"
            )
        except Exception:
            pass


def setup_scheduler(bot: Bot):
    # Напоминание мастеру о неактивных клиентах
    scheduler.add_job(send_inactive_reminders, "cron", hour=10, minute=0, args=[bot])

    # Напоминание клиентам за 24 часа (в 18:00)
    scheduler.add_job(send_client_reminders_24h, "cron", hour=18, minute=0, args=[bot])

    # Напоминание клиентам за 2 часа (каждые 30 минут)
    scheduler.add_job(send_client_reminders_2h, "interval", minutes=30, args=[bot])

    # Напоминание о коррекции через 3 недели
    scheduler.add_job(send_correction_reminders, "cron", hour=12, minute=0, args=[bot])

    # Запрос отзыва через 2 часа после визита
    scheduler.add_job(send_review_requests, "interval", minutes=30, args=[bot])

    # Напоминание об оплате за 24 часа до визита (в 19:00)
    scheduler.add_job(send_payment_reminders_24h, "cron", hour=19, minute=0, args=[bot])
    
    # Напоминание об оплате через 2 часа после записи
    scheduler.add_job(send_payment_reminders_2h, "interval", minutes=30, args=[bot])

    # Task 3: Birthday greetings at 9:00 MSK
    scheduler.add_job(send_birthday_greetings, "cron", hour=9, minute=0, args=[bot])

    # Loyalty notifications at 20:00 MSK
    scheduler.add_job(send_loyalty_notifications, "cron", hour=20, minute=0, args=[bot])

    scheduler.start()
