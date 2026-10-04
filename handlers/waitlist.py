"""Кнопки клиентки в предложении из листа ожидания: «Записаться» / «Не подходит»."""
from aiogram import Router, F
from aiogram.types import CallbackQuery

import waitlist

router = Router()


@router.callback_query(F.data.startswith("wl_yes:"))
async def cb_wl_yes(callback: CallbackQuery):
    entry_id = int(callback.data.split(":")[1])
    text = await waitlist.accept(entry_id, callback.from_user.id)
    try:
        await callback.message.edit_text(text)
    except Exception:
        await callback.message.answer(text)
    await callback.answer()


@router.callback_query(F.data.startswith("wl_no:"))
async def cb_wl_no(callback: CallbackQuery):
    entry_id = int(callback.data.split(":")[1])
    text = await waitlist.decline(entry_id, callback.from_user.id)
    try:
        await callback.message.edit_text(text)
    except Exception:
        await callback.message.answer(text)
    await callback.answer()
