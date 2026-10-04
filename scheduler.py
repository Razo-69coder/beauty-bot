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
    get_master_id_by_tg,
)
from database import get_reminder_template, get_reminder_template_with_enabled

scheduler = AsyncIOScheduler(timezone="Europe/Moscow")


async def _reminder_kb(master_id, appt_id):
    """Кнопки «Подтверждаю / Отменить / Перенести» под напоминанием клиентке."""
    from handlers.client_reminder import client_reminder_keyboard
    from database import get_pool
    slug = None
    if master_id:
        pool = await get_pool()
        async with pool.acquire() as conn:
            slug = await conn.fetchval("SELECT booking_link FROM masters WHERE id=$1", master_id)
    return client_reminder_keyboard(appt_id, slug or None)

# Московское время (UTC+3)
MSK = timezone(timedelta(hours=3))


def now_msk() -> datetime:
    return datetime.now(MSK).replace(tzinfo=None)


async def send_inactive_reminders(bot: Bot):
    """Ежедневно в 10:00 — напоминает мастеру о давно не приходивших клиентах"""
    print(f"[INACTIVE] Запуск в {now_msk().strftime('%Y-%m-%d %H:%M:%S')} MSK")
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
        except Exception as e:
            print(f"[INACTIVE] ❌ Ошибка отправки мастеру tg={telegram_id}: {e}")


async def send_client_reminders_24h(bot: Bot):
    """Ежедневно в 18:00 — напоминает клиентам о записи завтра"""
    tomorrow = (now_msk() + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"[REMINDER-24H] Запуск в {now_msk().strftime('%Y-%m-%d %H:%M:%S')} MSK, ищем записи на {tomorrow}")
    appointments = await get_appointments_for_reminder_24h(tomorrow)
    print(f"[REMINDER-24H] Найдено записей: {len(appointments)}")

    for appt_id, client_tg_id, client_name, master_tg_id, date, time, procedure, tz_offset in appointments:
        tz_offset = tz_offset or 3
        local_now = now_msk() + timedelta(hours=(tz_offset - 3))
        local_tomorrow = local_now + timedelta(days=1)
        appt_datetime = datetime.strptime(f"{date} {time}", "%Y-%m-%d %H:%M") if time else datetime.strptime(date, "%Y-%m-%d")
        if appt_datetime.date() != local_tomorrow.date():
            continue

        print(f"[REMINDER-24H] Обработка записи #{appt_id}: клиент {client_name} (tg={client_tg_id}), {date} {time}")
        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        from database import get_master_id_by_tg
        from database import get_reminder_template
        master_id = await get_master_id_by_tg(master_tg_id) if master_tg_id else None
        custom_template = await get_reminder_template(master_id, "24h") if master_id else None
        if custom_template:
            message_text = custom_template.format(
                date=date_fmt, time=time, procedure=procedure, name=client_name
            )
        else:
            message_text = (
                f"🔔 *Напоминание о записи*\n\n"
                f"Завтра, *{date_fmt}* в *{time}*\n"
                f"📋 {procedure}\n\n"
                f"Ждём вас!"
            )
        try:
            await bot.send_message(
                client_tg_id,
                message_text,
                parse_mode="Markdown",
                reply_markup=await _reminder_kb(master_id, appt_id),
            )
            await mark_reminder_sent(appt_id, "24h")
            print(f"[REMINDER-24H] ✅ Отправлено клиенту {client_name} (tg={client_tg_id}), запись #{appt_id}")
        except Exception as e:
            print(f"[REMINDER-24H] ❌ Ошибка отправки клиенту {client_name} (tg={client_tg_id}), запись #{appt_id}: {e}")


async def send_client_reminders_2h(bot: Bot):
    """Каждые 30 минут — напоминает клиентам о записи через ~2 часа (с учётом часового пояса мастера)"""
    now = now_msk()
    print(f"[REMINDER-2H] Запуск {now.strftime('%H:%M:%S')} MSK")

    # Берём все записи на сегодня и завтра, фильтруем по локальному времени каждого мастера
    today = now.strftime("%Y-%m-%d")
    tomorrow = (now + timedelta(days=1)).strftime("%Y-%m-%d")
    appointments = await get_appointments_for_reminder_2h(today, "00:00", "23:59") + await get_appointments_for_reminder_2h(tomorrow, "00:00", "23:59")
    if appointments:
        print(f"[REMINDER-2H] Найдено записей до фильтрации: {len(appointments)}")

    for appt_id, client_tg_id, client_name, master_tg_id, date, time, procedure, tz_offset in appointments:
        tz_offset = tz_offset or 3
        local_now = now + timedelta(hours=(tz_offset - 3))
        target = local_now + timedelta(hours=2)
        target_date = target.strftime("%Y-%m-%d")
        time_from = (target - timedelta(minutes=15)).strftime("%H:%M")
        time_to = (target + timedelta(minutes=15)).strftime("%H:%M")

        if date != target_date or not (time_from <= time <= time_to):
            continue

        date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
        from database import get_master_id_by_tg
        from database import get_reminder_template
        master_id = await get_master_id_by_tg(master_tg_id) if master_tg_id else None
        custom_template = await get_reminder_template(master_id, "2h") if master_id else None
        if custom_template:
            message_text = custom_template.format(
                date=date_fmt, time=time, procedure=procedure, name=client_name
            )
        else:
            message_text = (
                f"⏰ *Через 2 часа ваша запись!*\n\n"
                f"📅 {date_fmt} в *{time}*\n"
                f"📋 {procedure}\n\n"
                f"Не забудьте!"
            )
        try:
            await bot.send_message(
                client_tg_id,
                message_text,
                parse_mode="Markdown",
                reply_markup=await _reminder_kb(master_id, appt_id),
            )
            await mark_reminder_sent(appt_id, "2h")
            print(f"[REMINDER-2H] ✅ Отправлено клиенту {client_name} (tg={client_tg_id}), запись #{appt_id}")
        except Exception as e:
            print(f"[REMINDER-2H] ❌ Ошибка отправки клиенту {client_name} (tg={client_tg_id}), запись #{appt_id}: {e}")


async def send_correction_reminders(bot: Bot):
    """Ежедневно в 12:00 — «пора на коррекцию» со сроком по каждой услуге мастера.
    Не пишем, если клиентка уже записалась снова. Срок: самый короткий из услуг визита; 0 у всех — не напоминаем."""
    from database import get_correction_candidates, get_service_correction_map, default_correction_days
    today = now_msk().date()
    candidates = await get_correction_candidates(today.strftime("%Y-%m-%d"))
    maps = {}
    for appt_id, client_tg_id, client_name, master_name, procedure, master_id, visit_date in candidates:
        if master_id not in maps:
            maps[master_id] = await get_service_correction_map(master_id)
        proc = (procedure or "").lower()
        matched = [days for name, days in maps[master_id] if name and name in proc]
        if matched:
            active = [d for d in matched if d and d > 0]
            if not active:  # мастер выключила напоминание для этих услуг
                await mark_correction_reminder_sent(appt_id)
                continue
            days = min(active)
        else:
            days = default_correction_days(procedure)
        due = datetime.strptime(visit_date, "%Y-%m-%d").date() + timedelta(days=days)
        if due > today:
            continue  # ещё рано
        if (today - due).days > 3:  # сильно просрочено (например, после первого запуска) — не шлём старое
            await mark_correction_reminder_sent(appt_id)
            continue

        custom_template, enabled = await get_reminder_template_with_enabled(master_id, "correction")
        if not enabled:
            continue
        first = (client_name or "").split()[0] if client_name else ""
        if custom_template:
            message_text = custom_template.format(name=first, master_name=master_name, procedure=procedure)
        else:
            message_text = (
                f"💅 *Привет, {first}!*\n\n"
                f"Самое время записаться снова — с прошлого визита прошло {days} дн.\n\n"
                f"Запишитесь к мастеру {master_name} заранее 🗓"
            )
        try:
            await bot.send_message(client_tg_id, message_text, parse_mode="Markdown")
            await mark_correction_reminder_sent(appt_id)
        except Exception as e:
            print(f"[CORRECTION] ❌ запись #{appt_id}: {e}")


async def send_review_requests(bot: Bot):
    """Каждые 30 минут — просит клиента оценить визит.
    
    Приоритет: сначала проверяем review_requested_at (установлен после нажатия "Услуга оказана"),
    затем — старая логика (через 2 часа после времени записи).
    """
    now = now_msk()
    
    appointments = await get_appointments_for_review_request(now)
    
    for appt in appointments:
        custom_template, enabled = await get_reminder_template_with_enabled(appt['master_id'], "review")
        if not enabled:
            continue
        if custom_template:
            message_text = custom_template.format(name=appt['client_name'].split()[0], procedure=appt['procedure'])
        else:
            message_text = f"💅 *{appt['client_name'].split()[0]}, как прошёл визит?*\n\nОцените процедуру «{appt['procedure']}»:"
        from keyboards import review_rating_keyboard
        try:
            await bot.send_message(
                appt['client_telegram_id'],
                message_text,
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
            custom_template, enabled = await get_reminder_template_with_enabled(master_id, "review")
            if not enabled:
                continue
            if custom_template:
                message_text = custom_template.format(name=client_name.split()[0], procedure=procedure, master_name=master_name)
            else:
                message_text = f"💅 *{client_name.split()[0]}, как прошёл визит?*\n\nОцените процедуру «{procedure}» у мастера {master_name}:"
            from keyboards import review_rating_keyboard
            try:
                await bot.send_message(
                    client_tg_id,
                    message_text,
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
        # Check custom template
        master_id = await get_master_id_by_tg(master_tg_id) if master_tg_id else None
        custom_template, enabled = await get_reminder_template_with_enabled(master_id, "payment_24h") if master_id else (None, False)
        if not enabled:
            continue

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

        if custom_template:
            message_text = custom_template.format(
                date=date_fmt, time=time, name=client_name,
                deposit_pct=deposit_pct, rekv_block=rekv_block,
                procedure=""
            )
        else:
            message_text = (
                f"⚠️ *Напоминание об оплате*\n\n"
                f"Завтра, *{date_fmt}*{time_str} у вас запись.\n\n"
                f"Для подтверждения необходима предоплата *{deposit_pct}%*.{rekv_block}\n\n"
                f"Пожалуйста, внесите оплату — мастер ждёт подтверждения 💳"
            )
        try:
            await bot.send_message(
                client_tg_id,
                message_text,
                parse_mode="Markdown"
            )
        except Exception:
            pass


async def send_payment_reminders_2h(bot: Bot):
    """Каждые 30 минут — напоминает об оплате через 2 часа после записи."""
    appointments = await get_appointments_pending_deposit_2h()
    
    for appt_id, client_tg_id, client_name, master_tg_id, date, time, deposit_pct, payment_card, payment_phone, payment_banks in appointments:
        # Check custom template
        master_id = await get_master_id_by_tg(master_tg_id) if master_tg_id else None
        custom_template, enabled = await get_reminder_template_with_enabled(master_id, "payment_2h") if master_id else (None, False)
        if not enabled:
            continue

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

        if custom_template:
            message_text = custom_template.format(
                date=date_fmt, time=time, name=client_name,
                deposit_pct=deposit_pct, rekv_block=rekv_block,
                procedure=""
            )
        else:
            message_text = (
                f"💳 *Напоминание об оплате*\n\n"
                f"Вы записаны на *{date_fmt}*{time_str}.\n\n"
                f"Для подтверждения записи внесите предоплату *{deposit_pct}%*.{rekv_block}\n\n"
                f"После оплаты мастер подтвердит вашу запись ✅"
            )
        try:
            await bot.send_message(
                client_tg_id,
                message_text,
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


async def send_master_evening_summary(bot: Bot):
    """В 21:00 по местному времени мастера — сводка на завтра: сколько записей и сколько клиенток подтвердили."""
    from database import get_pool
    now = now_msk()
    pool = await get_pool()
    async with pool.acquire() as conn:
        masters = await conn.fetch("SELECT id, telegram_id, COALESCE(timezone_offset, 3) AS tz FROM masters WHERE is_active = 1")
        for m in masters:
            local_now = now + timedelta(hours=(m["tz"] - 3))
            if local_now.hour != 21:
                continue
            tomorrow = (local_now + timedelta(days=1)).strftime("%Y-%m-%d")
            rows = await conn.fetch(
                """SELECT a.time, a.client_confirmed_at, c.name
                   FROM appointments a JOIN clients c ON c.id = a.client_id
                   WHERE a.master_id = $1 AND a.appointment_date = $2 AND a.status <> 'cancelled'
                   ORDER BY a.time""",
                m["id"], tomorrow,
            )
            if not rows:
                continue
            confirmed = sum(1 for r in rows if r["client_confirmed_at"])
            first = rows[0]
            title = f"Завтра {len(rows)} {_plural(len(rows), 'запись', 'записи', 'записей')}"
            body = f"Подтвердили {confirmed} из {len(rows)}. Первая — {first['time']}, {first['name']}"
            try:
                if m["telegram_id"]:
                    lines = "\n".join(f"{'✅' if r['client_confirmed_at'] else '▫️'} {r['time']} — {r['name']}" for r in rows)
                    # без Markdown: имена клиенток могут содержать символы разметки
                    await bot.send_message(m["telegram_id"], f"🌙 {title}\n\n{lines}\n\n{body.split('.')[0]}.")
                else:
                    from main import push_to_master  # ленивый импорт: main уже загружен uvicorn'ом
                    await push_to_master(m["id"], f"🌙 {title}", body)
            except Exception as e:
                print(f"[SUMMARY] мастер #{m['id']}: {e}")


def _plural(n: int, one: str, few: str, many: str) -> str:
    if n % 10 == 1 and n % 100 != 11:
        return one
    if 2 <= n % 10 <= 4 and not 12 <= n % 100 <= 14:
        return few
    return many


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

    # Trial expiry reminder at 10:00 MSK
    scheduler.add_job(send_trial_expiry_reminder, "cron", hour=10, minute=0, args=[bot])

    # Лист ожидания: просроченные предложения → следующей клиентке
    import waitlist
    scheduler.add_job(waitlist.expire_offers, "interval", minutes=5)

    # Вечерняя сводка мастеру на завтра — каждый час, отправляем тем, у кого сейчас 21:00 по местному времени
    scheduler.add_job(send_master_evening_summary, "cron", minute=0, args=[bot])

    scheduler.start()


async def send_trial_expiry_reminder(bot: Bot):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows_2d = await conn.fetch("""
            SELECT id, telegram_id, name FROM masters
            WHERE trial_end_date::date = (NOW() + INTERVAL '2 days')::date
              AND is_active = 1
        """)
        rows_today = await conn.fetch("""
            SELECT id, telegram_id, name FROM masters
            WHERE trial_end_date::date = NOW()::date
              AND is_active = 1
        """)

    for row in rows_2d:
        try:
            await bot.send_message(
                row["telegram_id"],
                f"⏰ {row['name']}, пробный период Solvo Beauty заканчивается через 2 дня.\n\n"
                "Чтобы не потерять доступ к расписанию и клиентам — войдите в личный кабинет:\n"
                "👉 https://solvobeauty.vercel.app/account.html"
            )
            print(f"[TRIAL] Sent 2-day reminder to master #{row['id']}")
        except Exception as e:
            print(f"[TRIAL] Failed to send 2-day reminder to #{row['id']}: {e}")

    for row in rows_today:
        try:
            await bot.send_message(
                row["telegram_id"],
                f"🔒 {row['name']}, пробный период Solvo Beauty завершился сегодня.\n\n"
                "Ваши данные в сохранности. Чтобы продолжить работу — зайдите в личный кабинет:\n"
                "👉 https://solvobeauty.vercel.app/account.html"
            )
            print(f"[TRIAL] Sent end-of-trial message to master #{row['id']}")
        except Exception as e:
            print(f"[TRIAL] Failed to send end-of-trial message to #{row['id']}: {e}")
