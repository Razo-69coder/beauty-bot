import config
from aiogram import Router, F
from aiogram.types import CallbackQuery, Message

from database import (
    get_or_create_master, get_reminder_days, update_reminder_days,
    get_master_info, update_master_work_hours,
    get_master_theme, set_master_theme,
    get_payment_reminder_enabled, set_payment_reminder_enabled,
)
from keyboards import settings_keyboard, work_hours_keyboard, back_to_menu, theme_keyboard, payment_reminder_keyboard
from themes import get_theme

router = Router()


@router.callback_query(F.data == "settings")
async def cb_settings(callback: CallbackQuery):
    await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    days = await get_reminder_days(callback.from_user.id)
    theme_key = await get_master_theme(callback.from_user.id)
    t = get_theme(theme_key)

    text = (
        f"{t['header_settings']}\n\n"
        f"{t['reminder_label']}\n"
        f"Сейчас выбрано: *{days} дней*\n\n"
        f"Выбери интервал:"
    )
    await callback.message.edit_text(text, reply_markup=settings_keyboard(days, theme_key), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data.startswith("set_reminder:"))
async def cb_set_reminder(callback: CallbackQuery):
    days = int(callback.data.split(":")[1])
    await update_reminder_days(callback.from_user.id, days)
    theme_key = await get_master_theme(callback.from_user.id)
    t = get_theme(theme_key)

    text = (
        f"{t['header_settings']}\n\n"
        f"✅ Установлено: напоминать через *{days} дней* после последнего визита\n\n"
        f"Выбери интервал:"
    )
    await callback.message.edit_text(text, reply_markup=settings_keyboard(days, theme_key), parse_mode="Markdown")
    await callback.answer(f"Установлено: {days} дней")


@router.callback_query(F.data == "settings_work_hours")
async def cb_work_hours(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    master = await get_master_info(master_id)

    text = (
        f"🕐 *Рабочее время*\n\n"
        f"Начало: *{master['work_start']}:00*\n"
        f"Конец: *{master['work_end']}:00*\n"
        f"Длительность слота: *{master['slot_duration']} мин*\n\n"
        f"Клиенты будут видеть только эти часы при самозаписи."
    )
    await callback.message.edit_text(
        text,
        reply_markup=work_hours_keyboard(master["work_start"], master["work_end"], master["slot_duration"]),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("wh_start:"))
async def cb_wh_start(callback: CallbackQuery):
    new_start = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    master = await get_master_info(master_id)

    if new_start >= master["work_end"]:
        await callback.answer("Начало должно быть раньше конца рабочего дня", show_alert=True)
        return

    await update_master_work_hours(master_id, new_start, master["work_end"], master["slot_duration"])
    master["work_start"] = new_start

    text = (
        f"🕐 *Рабочее время*\n\n"
        f"Начало: *{new_start}:00*\n"
        f"Конец: *{master['work_end']}:00*\n"
        f"Длительность слота: *{master['slot_duration']} мин*"
    )
    await callback.message.edit_text(
        text,
        reply_markup=work_hours_keyboard(new_start, master["work_end"], master["slot_duration"]),
        parse_mode="Markdown"
    )
    await callback.answer(f"Начало: {new_start}:00")


@router.callback_query(F.data.startswith("wh_end:"))
async def cb_wh_end(callback: CallbackQuery):
    new_end = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    master = await get_master_info(master_id)

    if new_end <= master["work_start"]:
        await callback.answer("Конец должен быть позже начала рабочего дня", show_alert=True)
        return

    await update_master_work_hours(master_id, master["work_start"], new_end, master["slot_duration"])
    master["work_end"] = new_end

    text = (
        f"🕐 *Рабочее время*\n\n"
        f"Начало: *{master['work_start']}:00*\n"
        f"Конец: *{new_end}:00*\n"
        f"Длительность слота: *{master['slot_duration']} мин*"
    )
    await callback.message.edit_text(
        text,
        reply_markup=work_hours_keyboard(master["work_start"], new_end, master["slot_duration"]),
        parse_mode="Markdown"
    )
    await callback.answer(f"Конец: {new_end}:00")


@router.callback_query(F.data.startswith("wh_dur:"))
async def cb_wh_duration(callback: CallbackQuery):
    new_dur = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    master = await get_master_info(master_id)

    await update_master_work_hours(master_id, master["work_start"], master["work_end"], new_dur)
    master["slot_duration"] = new_dur

    text = (
        f"🕐 *Рабочее время*\n\n"
        f"Начало: *{master['work_start']}:00*\n"
        f"Конец: *{master['work_end']}:00*\n"
        f"Длительность слота: *{new_dur} мин*"
    )
    await callback.message.edit_text(
        text,
        reply_markup=work_hours_keyboard(master["work_start"], master["work_end"], new_dur),
        parse_mode="Markdown"
    )
    await callback.answer(f"Слот: {new_dur} мин")


@router.callback_query(F.data == "settings_booking_link")
async def cb_booking_link(callback: CallbackQuery):
    bot_username = getattr(config, "BOT_USERNAME", "")
    telegram_id = callback.from_user.id

    if bot_username:
        link = f"https://t.me/{bot_username}?start=book_{telegram_id}"
        text = (
            f"🔗 *Ваша ссылка для записи*\n\n"
            f"`{link}`\n\n"
            f"Поделитесь этой ссылкой с клиентами — они смогут записаться в любое время."
        )
    else:
        text = (
            f"🔗 *Ваша ссылка для записи*\n\n"
            f"Ваш ID: `book_{telegram_id}`\n\n"
            f"Для получения полной ссылки добавьте `BOT_USERNAME` в переменные окружения Render."
        )

    await callback.message.edit_text(text, reply_markup=back_to_menu(), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "settings_theme")
async def cb_settings_theme(callback: CallbackQuery):
    theme_key = await get_master_theme(callback.from_user.id)
    t = get_theme(theme_key)
    text = (
        f"{t['header_theme']}\n\n"
        f"Текущая тема: *{t['name']}*\n\n"
        f"Выбери стиль оформления бота:"
    )
    await callback.message.edit_text(text, reply_markup=theme_keyboard(theme_key), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "settings_payment_reminder")
async def cb_payment_reminder(callback: CallbackQuery):
    enabled = await get_payment_reminder_enabled(callback.from_user.id)
    await callback.message.edit_text(
        "💳 *Напоминание об оплате*\n\n"
        "Бот отправляет клиенту напоминание за 24 часа до визита, "
        "если предоплата ещё не внесена.\n\n"
        f"Статус: {'✅ Включено' if enabled else '❌ Выключено'}",
        reply_markup=payment_reminder_keyboard(enabled),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.in_({"payment_reminder_enable", "payment_reminder_disable"}))
async def cb_toggle_payment_reminder(callback: CallbackQuery):
    enabled = callback.data == "payment_reminder_enable"
    await set_payment_reminder_enabled(callback.from_user.id, enabled)
    status = "✅ Включено" if enabled else "❌ Выключено"
    await callback.message.edit_text(
        "💳 *Напоминание об оплате*\n\n"
        "Бот отправляет клиенту напоминание за 24 часа до визита, "
        "если предоплата ещё не внесена.\n\n"
        f"Статус: {status}",
        reply_markup=payment_reminder_keyboard(enabled),
        parse_mode="Markdown"
    )
    await callback.answer(status)


@router.callback_query(F.data.startswith("set_theme:"))
async def cb_set_theme(callback: CallbackQuery):
    new_theme = callback.data.split(":")[1]
    await set_master_theme(callback.from_user.id, new_theme)
    t = get_theme(new_theme)
    days = await get_reminder_days(callback.from_user.id)

    text = (
        f"{t['header_settings']}\n\n"
        f"🎨 Тема изменена на *{t['name']}*\n\n"
        f"{t['reminder_label']}\n"
        f"Сейчас выбрано: *{days} дней*\n\n"
        f"Выбери интервал:"
    )
    await callback.message.edit_text(text, reply_markup=settings_keyboard(days, new_theme), parse_mode="Markdown")
    await callback.answer(f"Тема: {t['name']}")
