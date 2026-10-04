"""Лист ожидания клиенток.

Клиентка встаёт в очередь на занятый день (страница записи). Когда в этот день
освобождается окно (отмена, перенос), бот сам предлагает его первой подходящей
клиентке из очереди: «Освободилось окно в 14:00. Записать вас?» [Да] [Нет].
Не ответила за 30 минут — окно уходит следующей.
Клиентке без Telegram бот написать не может — тогда сообщаем мастеру, чтобы позвонила сама.
"""
import asyncio
from datetime import datetime as _dt, timedelta

from database import get_pool, add_appointment, create_notification

# Пожелания по времени: начало и конец интервала в минутах от полуночи
PREFS = {
    "any": (0, 24 * 60),
    "morning": (0, 12 * 60),
    "day": (12 * 60, 17 * 60),
    "evening": (17 * 60, 24 * 60),
}
PREF_LABELS = {"any": "любое время", "morning": "утро", "day": "день", "evening": "вечер"}
OFFER_TTL_MIN = 30

_RU_WD = ["пн", "вт", "ср", "чт", "пт", "сб", "вс"]
_RU_MON = ["января", "февраля", "марта", "апреля", "мая", "июня",
           "июля", "августа", "сентября", "октября", "ноября", "декабря"]


def _to_min(t: str) -> int:
    h, m = t.split(":")
    return int(h) * 60 + int(m)


def human_date(date: str) -> str:
    d = _dt.strptime(date, "%Y-%m-%d")
    return f"{_RU_WD[d.weekday()]}, {d.day} {_RU_MON[d.month - 1]}"


async def free_slots(booking_link: str, date: str, duration: int) -> list:
    """Свободные слоты так же, как их видит страница записи."""
    from main import v1_public_slots  # ленивый импорт: main уже загружен
    try:
        res = await v1_public_slots(booking_link, date, duration or 0)
        return res.get("slots", [])
    except Exception as e:
        print(f"[WAITLIST] slots error: {e}")
        return []


def slot_freed(master_id: int, date: str):
    """Окно в этот день могло освободиться — запускаем подбор в фоне (не блокируем ответ)."""
    if not master_id or not date:
        return
    date = str(date)[:10]

    async def _run():
        try:
            await offer_next(master_id, date)
        except Exception as e:
            print(f"[WAITLIST] offer_next error: {e}")
    try:
        asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        pass


async def _notify_master(master_id: int, title: str, body: str):
    from main import push_to_master  # ленивый импорт
    try:
        await push_to_master(master_id, title, body)
    except Exception as e:
        print(f"[WAITLIST] push error: {e}")
    try:
        await create_notification(master_id, "waitlist", title, body)
    except Exception as e:
        print(f"[WAITLIST] notification error: {e}")


async def offer_next(master_id: int, date: str):
    from main import bot  # ленивый импорт
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    pool = await get_pool()
    async with pool.acquire() as conn:
        master = await conn.fetchrow(
            "SELECT id, booking_link, COALESCE(timezone_offset, 3) AS tz FROM masters WHERE id=$1", master_id)
        if not master or not master["booking_link"]:
            return
        # Уже ждём ответа от кого-то на этот день — не дублируем предложение
        if await conn.fetchval(
                "SELECT 1 FROM client_waitlist WHERE master_id=$1 AND date=$2 AND status='offered'", master_id, date):
            return
        entries = await conn.fetch(
            """SELECT w.id, w.duration_min, w.pref, w.procedure, c.name, c.phone, c.telegram_id
               FROM client_waitlist w JOIN clients c ON c.id = w.client_id
               WHERE w.master_id=$1 AND w.date=$2 AND w.status='waiting'
               ORDER BY w.created_at""", master_id, date)
    if not entries:
        return
    local_now = _dt.utcnow() + timedelta(hours=master["tz"])
    today = local_now.strftime("%Y-%m-%d")
    if date < today:
        return
    # Сегодня предлагаем только время не раньше чем через час — клиентка должна успеть доехать
    cutoff = (local_now + timedelta(minutes=60)).strftime("%H:%M") if date == today else None

    for e in entries:
        slots = await free_slots(master["booking_link"], date, e["duration_min"] or 0)
        if cutoff:
            slots = [s for s in slots if s >= cutoff]
        lo, hi = PREFS.get(e["pref"], PREFS["any"])
        fit = [s for s in slots if lo <= _to_min(s) < hi]
        if not fit:
            continue
        slot = fit[0]
        when = f"{human_date(date)} в {slot}"
        if e["telegram_id"]:
            kb = InlineKeyboardMarkup(inline_keyboard=[[
                InlineKeyboardButton(text="✅ Записаться", callback_data=f"wl_yes:{e['id']}"),
                InlineKeyboardButton(text="Не подходит", callback_data=f"wl_no:{e['id']}"),
            ]])
            try:
                msg = await bot.send_message(
                    e["telegram_id"],
                    f"🎉 Освободилось окно!\n\n📅 {when}\n💅 {e['procedure']}\n\n"
                    f"Записать вас? Окно держим {OFFER_TTL_MIN} минут.",
                    reply_markup=kb)
            except Exception as ex:
                print(f"[WAITLIST] send offer error: {ex}")
                continue
            async with pool.acquire() as conn:
                await conn.execute(
                    """UPDATE client_waitlist SET status='offered', offered_time=$2, offered_at=NOW(),
                       offer_chat_id=$3, offer_msg_id=$4 WHERE id=$1""",
                    e["id"], slot, e["telegram_id"], msg.message_id)
            await _notify_master(master_id, "Лист ожидания",
                                 f"Освободилось {when} — предложили {e['name']}. Ждём ответа {OFFER_TTL_MIN} мин.")
            return
        # Без Telegram бот написать не может — просим мастера позвонить
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE client_waitlist SET status='master_notified', offered_time=$2, offered_at=NOW() WHERE id=$1",
                e["id"], slot)
        await _notify_master(master_id, "Освободилось окно",
                             f"{when}. В листе ожидания {e['name']} ({e['phone']}) — у неё нет Telegram, позвоните ей.")
        return


async def accept(entry_id: int, tg_user_id: int) -> str:
    """Клиентка нажала «Записаться». Возвращает текст ответа."""
    from main import bot
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT w.*, c.name AS client_name, c.telegram_id, m.booking_link, m.telegram_id AS master_tg
               FROM client_waitlist w JOIN clients c ON c.id = w.client_id JOIN masters m ON m.id = w.master_id
               WHERE w.id=$1""", entry_id)
    if not row or row["telegram_id"] != tg_user_id:
        return "Предложение не найдено."
    if row["status"] != "offered":
        return "Это предложение уже неактуально."
    slots = await free_slots(row["booking_link"], row["date"], row["duration_min"] or 0)
    if row["offered_time"] not in slots:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE client_waitlist SET status='waiting', offered_time=NULL, offered_at=NULL WHERE id=$1", entry_id)
        return "Увы, это окно уже заняли 😔 Вы остаётесь в листе ожидания — напишем, если освободится другое."
    appt_id = await add_appointment(
        client_id=row["client_id"], master_id=row["master_id"], procedure=row["procedure"],
        appointment_date=row["date"], time=row["offered_time"], price=row["price"] or 0,
        status="confirmed", duration_min=row["duration_min"] or 0,
    )
    async with pool.acquire() as conn:
        await conn.execute("UPDATE client_waitlist SET status='booked', appointment_id=$2 WHERE id=$1", entry_id, appt_id)
    when = f"{human_date(row['date'])} в {row['offered_time']}"
    try:
        if row["master_tg"]:
            await bot.send_message(row["master_tg"], f"✅ Окно заняли из листа ожидания\n\n{row['client_name']} · {when} · {row['procedure']}")
    except Exception as e:
        print(f"[WAITLIST] master tg error: {e}")
    await _notify_master(row["master_id"], "📅 Новая запись из листа ожидания",
                         f"{row['client_name']} · {when} · {row['procedure']}")
    return f"Готово! Вы записаны 🎉\n\n📅 {when}\n💅 {row['procedure']}\n\nНапомним за сутки и за 2 часа."


async def decline(entry_id: int, tg_user_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT w.master_id, w.date, w.status, c.telegram_id FROM client_waitlist w
               JOIN clients c ON c.id = w.client_id WHERE w.id=$1""", entry_id)
        if not row or row["telegram_id"] != tg_user_id:
            return "Предложение не найдено."
        if row["status"] != "offered":
            return "Это предложение уже неактуально."
        await conn.execute("UPDATE client_waitlist SET status='declined' WHERE id=$1", entry_id)
    slot_freed(row["master_id"], row["date"])
    return "Хорошо, поняли! Окно предложим другой клиентке."


async def expire_offers():
    """Раз в 5 минут: просроченные предложения → следующей; прошедшие дни — закрываем."""
    from main import bot
    pool = await get_pool()
    async with pool.acquire() as conn:
        expired = await conn.fetch(
            f"""UPDATE client_waitlist SET status='expired'
                WHERE status='offered' AND offered_at < NOW() - INTERVAL '{OFFER_TTL_MIN} minutes'
                RETURNING id, master_id, date, offer_chat_id, offer_msg_id""")
        await conn.execute(
            "UPDATE client_waitlist SET status='expired' WHERE status IN ('waiting','master_notified') "
            "AND date < to_char(NOW() - INTERVAL '1 day', 'YYYY-MM-DD')")
    for r in expired:
        try:
            if r["offer_chat_id"] and r["offer_msg_id"]:
                await bot.edit_message_text(
                    "Время на ответ вышло — окно предложили другой клиентке.",
                    chat_id=r["offer_chat_id"], message_id=r["offer_msg_id"])
        except Exception:
            pass
        slot_freed(r["master_id"], r["date"])
