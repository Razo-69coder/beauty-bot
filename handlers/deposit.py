from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery

from database import (
    get_or_create_master, get_master_deposit_settings, update_master_deposit_settings,
    get_master_full, update_appointment_deposit, get_appointment_with_client,
    update_appointment_status, mark_client_regular,
)
from keyboards import deposit_settings_keyboard, deposit_master_keyboard, back_to_menu

router = Router()


# ── Настройки предоплаты (мастер) ─────────────────────────────────────

@router.callback_query(F.data == "settings_deposit")
async def cb_deposit_settings(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    settings = await get_master_deposit_settings(master_id)
    full = await get_master_full(master_id)

    card = full.get("payment_card") or "_(не указаны)_"
    status = "✅ включена" if settings["deposit_enabled"] else "❌ выключена"

    text = (
        f"💳 *Предоплата для новых клиентов*\n\n"
        f"Статус: {status}\n"
        f"Размер: *{settings['deposit_percent']}%* от стоимости процедуры\n"
        f"Реквизиты: {card}\n\n"
        f"Реквизиты (карта/СБП) задаются в Настройки → Рабочие данные."
    )
    await callback.message.edit_text(
        text,
        reply_markup=deposit_settings_keyboard(settings["deposit_enabled"], settings["deposit_percent"]),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.in_({"deposit_enable", "deposit_disable"}))
async def cb_deposit_toggle(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    settings = await get_master_deposit_settings(master_id)
    new_enabled = callback.data == "deposit_enable"
    await update_master_deposit_settings(master_id, new_enabled, settings["deposit_percent"])

    settings["deposit_enabled"] = new_enabled
    full = await get_master_full(master_id)
    card = full.get("payment_card") or "_(не указаны)_"
    status = "✅ включена" if new_enabled else "❌ выключена"

    text = (
        f"💳 *Предоплата для новых клиентов*\n\n"
        f"Статус: {status}\n"
        f"Размер: *{settings['deposit_percent']}%*\n"
        f"Реквизиты: {card}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=deposit_settings_keyboard(new_enabled, settings["deposit_percent"]),
        parse_mode="Markdown",
    )
    await callback.answer("Сохранено")


@router.callback_query(F.data.startswith("deposit_pct:"))
async def cb_deposit_percent(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    percent = int(callback.data.split(":")[1])
    settings = await get_master_deposit_settings(master_id)
    await update_master_deposit_settings(master_id, settings["deposit_enabled"], percent)

    full = await get_master_full(master_id)
    card = full.get("payment_card") or "_(не указаны)_"
    status = "✅ включена" if settings["deposit_enabled"] else "❌ выключена"

    text = (
        f"💳 *Предоплата для новых клиентов*\n\n"
        f"Статус: {status}\n"
        f"Размер: *{percent}%*\n"
        f"Реквизиты: {card}"
    )
    await callback.message.edit_text(
        text,
        reply_markup=deposit_settings_keyboard(settings["deposit_enabled"], percent),
        parse_mode="Markdown",
    )
    await callback.answer(f"Установлено {percent}%")


# ── Клиент подтверждает оплату ─────────────────────────────────────────

@router.callback_query(F.data.startswith("deposit_paid:"))
async def cb_deposit_paid(callback: CallbackQuery, bot: Bot):
    appointment_id = int(callback.data.split(":")[1])
    appt = await get_appointment_with_client(appointment_id)
    if not appt:
        await callback.answer("Запись не найдена", show_alert=True)
        return

    await update_appointment_deposit(appointment_id, "payment_claimed", appt["deposit_amount"])

    await callback.message.edit_text(
        "⏳ *Ожидаем подтверждения оплаты*\n\n"
        "Мастер проверит поступление и подтвердит вашу запись.\n"
        "Вы получите уведомление.",
        parse_mode="Markdown",
    )
    await callback.answer()

    # Уведомляем мастера
    date_fmt = appt["appointment_date"]
    try:
        from datetime import datetime
        date_fmt = datetime.strptime(appt["appointment_date"], "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        pass

    try:
        await bot.send_message(
            appt["master_tg_id"],
            f"💳 *Клиент сообщил об оплате предоплаты!*\n\n"
            f"👤 {appt['client_name']}\n"
            f"📅 {date_fmt} в {appt['time']}\n"
            f"💰 Сумма: {appt['deposit_amount']} руб.\n\n"
            f"Подтвердите получение:",
            reply_markup=deposit_master_keyboard(appointment_id),
            parse_mode="Markdown",
        )
    except Exception:
        pass


@router.callback_query(F.data.startswith("deposit_cancel:"))
async def cb_deposit_cancel(callback: CallbackQuery):
    appointment_id = int(callback.data.split(":")[1])
    await update_appointment_status(appointment_id, "cancelled")
    await callback.message.edit_text(
        "❌ Запись отменена.\n\nВы можете записаться снова в удобное время.",
        parse_mode="Markdown",
    )
    await callback.answer()


# ── Мастер подтверждает или отклоняет оплату ──────────────────────────

@router.callback_query(F.data.startswith("deposit_confirm:"))
async def cb_deposit_confirm(callback: CallbackQuery, bot: Bot):
    appointment_id = int(callback.data.split(":")[1])
    appt = await get_appointment_with_client(appointment_id)
    if not appt:
        await callback.answer("Запись не найдена", show_alert=True)
        return

    await update_appointment_deposit(appointment_id, "paid", appt["deposit_amount"])
    await update_appointment_status(appointment_id, "confirmed")
    await mark_client_regular(appt["client_id"])

    date_fmt = appt["appointment_date"]
    try:
        from datetime import datetime
        date_fmt = datetime.strptime(appt["appointment_date"], "%Y-%m-%d").strftime("%d.%m.%Y")
    except Exception:
        pass

    await callback.message.edit_text(
        f"✅ Оплата подтверждена! Запись {appt['client_name']} на {date_fmt} в {appt['time']} активна.",
        parse_mode="Markdown",
    )
    await callback.answer("Оплата подтверждена")

    # Уведомляем клиента
    if appt["client_tg_id"]:
        try:
            await bot.send_message(
                appt["client_tg_id"],
                f"✅ *Оплата подтверждена!*\n\n"
                f"Ваша запись на *{date_fmt}* в *{appt['time']}* подтверждена.\n"
                f"Ждём вас! 💅",
                parse_mode="Markdown",
            )
        except Exception:
            pass


@router.callback_query(F.data.startswith("deposit_reject:"))
async def cb_deposit_reject(callback: CallbackQuery, bot: Bot):
    appointment_id = int(callback.data.split(":")[1])
    appt = await get_appointment_with_client(appointment_id)
    if not appt:
        await callback.answer("Запись не найдена", show_alert=True)
        return

    await update_appointment_deposit(appointment_id, "rejected")
    await update_appointment_status(appointment_id, "cancelled")

    await callback.message.edit_text(
        f"❌ Оплата отклонена. Запись {appt['client_name']} отменена.",
        parse_mode="Markdown",
    )
    await callback.answer("Запись отменена")

    # Уведомляем клиента
    if appt["client_tg_id"]:
        try:
            await bot.send_message(
                appt["client_tg_id"],
                "❌ *Предоплата не подтверждена.*\n\n"
                "Ваша запись отменена. Если вы уже оплатили — свяжитесь с мастером напрямую.",
                parse_mode="Markdown",
            )
        except Exception:
            pass
