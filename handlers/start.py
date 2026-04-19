from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext

from database import get_or_create_master, create_login_code
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
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    # Обрабатываем deep link для онлайн-записи
    if command.args and command.args.startswith("book_"):
        try:
            master_telegram_id = int(command.args.split("_", 1)[1])
        except (ValueError, IndexError):
            await message.answer("Неверная ссылка для записи.")
            return

        from handlers.booking import start_booking_flow
        await start_booking_flow(message, state, master_telegram_id)
        return

    # Обычный старт для мастера
    await get_or_create_master(message.from_user.id, message.from_user.full_name)
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


@router.callback_query(F.data == "get_login_code")
async def cb_get_login_code(callback: CallbackQuery):
    tg_id = callback.from_user.id
    code = await create_login_code(tg_id)
    await callback.message.answer(
        f"🔑 *Ваш код входа в Beauty Book*\n\n"
        f"`{code}`\n\n"
        f"Введите этот код на странице входа.\n"
        f"_Код действует 10 минут._",
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "help")
async def cb_help(callback: CallbackQuery):
    await callback.message.edit_text(
        HELP_TEXT, reply_markup=main_menu(), parse_mode="Markdown"
    )
    await callback.answer()
