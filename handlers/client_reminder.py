"""Кнопки в напоминании клиентке: «Подтверждаю» / «Отменить» (+ «Перенести» — ссылка на страницу клиентки)."""
from datetime import datetime

from aiogram import Router, F
from aiogram.types import CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton

from database import get_pool, create_notification

router = Router()

MANAGE_URL = "https://solvobeauty.vercel.app/my/{slug}"


def client_reminder_keyboard(appt_id: int, slug: str | None) -> InlineKeyboardMarkup:
    """Клавиатура под напоминанием. Без ответа клиентки запись остаётся как есть."""
    rows = [[
        InlineKeyboardButton(text="✅ Подтверждаю", callback_data=f"rc_ok:{appt_id}"),
        InlineKeyboardButton(text="❌ Отменить", callback_data=f"rc_cancel:{appt_id}"),
    ]]
    if slug:
        rows.append([InlineKeyboardButton(text="🔁 Перенести", url=MANAGE_URL.format(slug=slug))])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _load_own_appointment(appt_id: int, tg_id: int):
    """Запись + проверка, что кнопку нажала именно клиентка этой записи."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        return await conn.fetchrow(
            """SELECT a.id, a.master_id, a.procedure, a.appointment_date, a.time, a.status,
                      c.name AS client_name, c.telegram_id AS client_tg,
                      m.telegram_id AS master_tg
               FROM appointments a
               JOIN clients c ON c.id = a.client_id
               JOIN masters m ON m.id = a.master_id
               WHERE a.id = $1""",
            appt_id,
        ) if tg_id else None


def _when(row) -> str:
    d = datetime.strptime(str(row["appointment_date"])[:10], "%Y-%m-%d").strftime("%d.%m")
    return f"{d} в {row['time']}"


@router.callback_query(F.data.startswith("rc_ok:"))
async def cb_client_confirm(callback: CallbackQuery):
    appt_id = int(callback.data.split(":")[1])
    row = await _load_own_appointment(appt_id, callback.from_user.id)
    if not row or row["client_tg"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    if row["status"] == "cancelled":
        await callback.answer("Эта запись уже отменена", show_alert=True)
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE appointments SET client_confirmed_at = NOW() WHERE id = $1", appt_id)
    # Оставляем только «Перенести», чтобы клиентка могла передумать
    kb = callback.message.reply_markup
    keep = [r for r in (kb.inline_keyboard if kb else []) if any(b.url for b in r)]
    await callback.message.edit_reply_markup(reply_markup=InlineKeyboardMarkup(inline_keyboard=keep) if keep else None)
    await callback.message.answer("✅ Спасибо! Запись подтверждена, ждём вас 💅")
    await callback.answer()


@router.callback_query(F.data.startswith("rc_cancel:"))
async def cb_client_cancel_ask(callback: CallbackQuery):
    """Шаг 1: переспрашиваем, чтобы не отменить случайным нажатием."""
    appt_id = int(callback.data.split(":")[1])
    row = await _load_own_appointment(appt_id, callback.from_user.id)
    if not row or row["client_tg"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    if row["status"] == "cancelled":
        await callback.answer("Эта запись уже отменена", show_alert=True)
        return
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="Да, отменить", callback_data=f"rc_cancel_yes:{appt_id}"),
        InlineKeyboardButton(text="Нет, приду", callback_data=f"rc_ok:{appt_id}"),
    ]])
    await callback.message.answer(f"Точно отменить запись {_when(row)}?", reply_markup=kb)
    await callback.answer()


@router.callback_query(F.data.startswith("rc_cancel_yes:"))
async def cb_client_cancel(callback: CallbackQuery):
    """Шаг 2: отменяем и сообщаем мастеру."""
    appt_id = int(callback.data.split(":")[1])
    row = await _load_own_appointment(appt_id, callback.from_user.id)
    if not row or row["client_tg"] != callback.from_user.id:
        await callback.answer("Запись не найдена", show_alert=True)
        return
    if row["status"] != "cancelled":
        pool = await get_pool()
        async with pool.acquire() as conn:
            await conn.execute("UPDATE appointments SET status = 'cancelled' WHERE id = $1", appt_id)
        text = f"{row['client_name']} · {_when(row)} · {row['procedure']}"
        try:
            if row["master_tg"]:
                await callback.bot.send_message(row["master_tg"], f"❌ Клиентка отменила запись\n\n{text}")
        except Exception as e:
            print(f"[RC-CANCEL] telegram master notify error: {e}")
        try:
            from main import push_to_master  # ленивый импорт: main уже загружен uvicorn'ом
            await push_to_master(row["master_id"], "Отмена записи", f"{row['client_name']} отменила запись на {_when(row)}")
        except Exception as e:
            print(f"[RC-CANCEL] push error: {e}")
        try:
            await create_notification(row["master_id"], "client_cancel", "❌ Клиент отменил запись", text, appt_id)
        except Exception as e:
            print(f"[RC-CANCEL] notification error: {e}")
    await callback.message.edit_text("❌ Запись отменена. Мастер получил уведомление.")
    await callback.answer()
