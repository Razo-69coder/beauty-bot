from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.fsm.context import FSMContext

from database import get_or_create_master
from keyboards import main_menu

router = Router()

WELCOME_TEXT = """
✨ *Beauty Book* — твоя CRM для мастера красоты

Нажми кнопку ниже, чтобы открыть приложение 👇
"""

HELP_TEXT = """
💡 *Как пользоваться Beauty Book*

*Добавить клиента:*
Нажми ➕ Новый клиент → введи имя и телефон

*Записать процедуру:*
Нажми 📅 Записать клиента → выбери клиента → введи процедуру и дату

*Найти потерявшихся клиентов:*
Нажми 🔔 Кто давно не приходил — бот покажет всех, кто не был больше 40 дней

*История клиента:*
Выбери клиента из списка → нажми 📋 История процедур

━━━━━━━━━━━━━━━━━━━━
По вопросам: @your_support
"""


@router.message(CommandStart())
async def cmd_start(message: Message):
    master_id = await get_or_create_master(
        message.from_user.id,
        message.from_user.full_name
    )
    await message.answer(WELCOME_TEXT, reply_markup=main_menu(), parse_mode="Markdown")


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        WELCOME_TEXT, reply_markup=main_menu(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text(
        WELCOME_TEXT, reply_markup=main_menu(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "help")
async def cb_help(callback: CallbackQuery):
    from keyboards import back_to_menu
    await callback.message.edit_text(
        HELP_TEXT, reply_markup=back_to_menu(), parse_mode="Markdown"
    )
    await callback.answer()
