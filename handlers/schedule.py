from aiogram import Router, F
from aiogram.types import CallbackQuery
from datetime import datetime, timedelta

from database import (
    get_or_create_master, get_master_schedule,
    update_appointment_status, get_appointment_client_telegram,
)
from keyboards import schedule_keyboard, back_to_menu

router = Router()

DAYS_RU = {0: "Понедельник", 1: "Вторник", 2: "Среда",
           3: "Четверг", 4: "Пятница", 5: "Суббота", 6: "Воскресенье"}


@router.callback_query(F.data == "my_schedule")
async def cb_my_schedule(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    today = datetime.now().strftime("%Y-%m-%d")
    await _show_schedule(callback, master_id, today)


@router.callback_query(F.data.startswith("schedule_day:"))
async def cb_schedule_day(callback: CallbackQuery):
    date = callback.data.split(":", 1)[1]
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    await _show_schedule(callback, master_id, date)


async def _show_schedule(callback: CallbackQuery, master_id: int, date: str):
    appointments = await get_master_schedule(master_id, date)
    dt = datetime.strptime(date, "%Y-%m-%d")
    date_fmt = dt.strftime("%d.%m.%Y")
    day_name = DAYS_RU[dt.weekday()]

    if not appointments:
        text = f"📅 *{date_fmt}, {day_name}*\n\n_Записей нет_"
    else:
        text = f"📅 *{date_fmt}, {day_name}*\n\n"
        for appt_id, client_name, procedure, time, status, phone in appointments:
            icon = {"pending": "⏳", "confirmed": "✅", "cancelled": "❌"}.get(status, "•")
            t = time or "—"
            text += f"{icon} *{t}* — {client_name}\n"
            text += f"   📋 {procedure}\n"
            if phone and phone != "—":
                text += f"   📱 {phone}\n"
            text += "\n"

    prev_date = (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    next_date = (dt + timedelta(days=1)).strftime("%Y-%m-%d")

    await callback.message.edit_text(
        text,
        reply_markup=schedule_keyboard(date, prev_date, next_date, appointments),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("booking_confirm:"))
async def cb_booking_confirm(callback: CallbackQuery):
    appt_id = int(callback.data.split(":", 1)[1])
    await update_appointment_status(appt_id, "confirmed")

    client_tg_id, date, time = await get_appointment_client_telegram(appt_id)
    if client_tg_id:
        try:
            date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
            await callback.bot.send_message(
                client_tg_id,
                f"✅ *Ваша запись подтверждена!*\n\n"
                f"📅 {date_fmt} в {time}\n\n"
                f"Ждём вас!",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    await callback.message.edit_text(
        callback.message.text + "\n\n✅ *Запись подтверждена*",
        parse_mode="Markdown"
    )
    await callback.answer("Запись подтверждена!")


@router.callback_query(F.data.startswith("booking_cancel:"))
async def cb_booking_cancel(callback: CallbackQuery):
    appt_id = int(callback.data.split(":", 1)[1])
    await update_appointment_status(appt_id, "cancelled")

    client_tg_id, date, time = await get_appointment_client_telegram(appt_id)
    if client_tg_id:
        try:
            date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
            await callback.bot.send_message(
                client_tg_id,
                f"😔 Запись на *{date_fmt}* в *{time}* отменена мастером.\n\n"
                f"Запишитесь на другое время.",
                parse_mode="Markdown"
            )
        except Exception:
            pass

    await callback.message.edit_text(
        callback.message.text + "\n\n❌ *Запись отменена*",
        parse_mode="Markdown"
    )
    await callback.answer("Запись отменена")
