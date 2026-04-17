from aiogram import Router, F
from aiogram.types import CallbackQuery

from database import get_or_create_master, get_reminder_days, update_reminder_days
from keyboards import settings_keyboard

router = Router()


@router.callback_query(F.data == "settings")
async def cb_settings(callback: CallbackQuery):
    await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    days = await get_reminder_days(callback.from_user.id)

    text = (
        f"⚙️ *Настройки*\n\n"
        f"🔔 Напоминать о клиентах, которые не приходили:\n"
        f"Сейчас выбрано: *{days} дней*\n\n"
        f"Выбери интервал:"
    )
    await callback.message.edit_text(text, reply_markup=settings_keyboard(days), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("set_reminder:"))
async def cb_set_reminder(callback: CallbackQuery):
    days = int(callback.data.split(":")[1])
    await update_reminder_days(callback.from_user.id, days)

    text = (
        f"⚙️ *Настройки*\n\n"
        f"✅ Установлено: напоминать через *{days} дней* после последнего визита\n\n"
        f"Выбери интервал:"
    )
    await callback.message.edit_text(text, reply_markup=settings_keyboard(days), parse_mode="Markdown")
    await callback.answer(f"Установлено: {days} дней")
