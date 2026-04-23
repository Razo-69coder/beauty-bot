from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import CommandStart
from aiogram.filters.command import CommandObject
from aiogram.fsm.context import FSMContext

from database import get_or_create_master, create_login_code, get_master_theme, get_client_pending_appointments, update_appointment_status, get_appointment_by_id, assign_client_telegram
from keyboards import main_menu, confirm_appointment_keyboard
from themes import get_theme

router = Router()

HELP_TEXT = (
    "💡 *Как пользоваться Beauty Book*\n\n"
    "*Добавить клиента:*\n"
    "Нажми ➕ Новый клиент → введи имя и телефон\n\n"
    "*Записать процедуру:*\n"
    "Нажми 📅 Записать клиента → выбери клиента → введи процедуру и дату\n\n"
    "*Найти потерявшихся клиентов:*\n"
    "Нажми 🔔 Кто давно не приходил — бот покажет всех, кто не был больше 40 дней\n\n"
    "*История клиента:*\n"
    "Выбери клиента из списка → нажми 📋 История процедур\n\n"
    "━━━━━━━━━━━━━━━━━━━━\n"
    "По вопросам: @your\\_support"
)


@router.message(CommandStart(deep_link="reg"))
async def cmd_start_reg(message: Message, state: FSMContext):
    """Клиент подтверждает запись через ссылку"""
    tg_id = message.from_user.id
    
    # Ищем незавершённые записи этого клиента
    appointments = await get_client_pending_appointments(tg_id)
    
    if not appointments:
        await message.answer(
            "У вас нет записей, ожидающих подтверждения.\n"
            "Запишитесь через ссылку мастера."
        )
        return
    
    # Показываем записи для подтверждения
    from keyboards import confirm_appointment_keyboard
    
    for appt in appointments[:3]:  # макс 3 записи
        date_str = appt['appointment_date']
        time_str = appt['time']
        procedure = appt.get('procedure', 'Процедура')
        
        await message.answer(
            f"📅 *Подтверждение записи*\n\n"
            f"{procedure}\n"
            f"{date_str} в {time_str}",
            reply_markup=confirm_appointment_keyboard(appt['id']),
            parse_mode="Markdown"
        )


@router.message(CommandStart(deep_link="confirm_"))
async def cmd_start_confirm(message: Message, state: FSMContext, command: CommandObject):
    """Клиент подтверждает конкретную запись по ID"""
    # command.args = "confirm_123" → нужно "123"
    arg = command.args or ""
    if arg.startswith("confirm_"):
        arg = arg[8:]  # убираем "confirm_"
    
    try:
        appointment_id = int(arg)
    except (ValueError, TypeError):
        await message.answer("Неверная ссылка для подтверждения.")
        return
    
    # Проверяем что запись существует
    from database import get_appointment_by_id, assign_client_telegram
    appt = await get_appointment_by_id(appointment_id)
    
    if not appt:
        await message.answer("Запись не найдена.")
        return
    
    if appt.get('status') != 'pending':
        await message.answer("Эта запись уже подтверждена.")
        return
    
    # Привязываем telegram_id клиента к записи
    client_tg_id = message.from_user.id
    await assign_client_telegram(appt['client_id'], client_tg_id)
    
    # Подтверждаем
    await update_appointment_status(appointment_id, "confirmed")
    await message.answer("✅ Запись подтверждена! Ждём вас.")


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    if command.args and command.args.startswith("book_"):
        try:
            master_telegram_id = int(command.args.split("_", 1)[1])
        except (ValueError, IndexError):
            await message.answer("Неверная ссылка для записи.")
            return
        from handlers.booking import start_booking_flow
        await start_booking_flow(message, state, master_telegram_id)
        return

    # Проверяем есть ли незавершённые записи (клиент может заходить через ?start=reg)
    tg_id = message.from_user.id
    appointments = await get_client_pending_appointments(tg_id)
    if appointments:
        await cmd_start_reg(message, state)
        return

    await get_or_create_master(message.from_user.id, message.from_user.full_name)
    theme_key = await get_master_theme(message.from_user.id)
    t = get_theme(theme_key)
    await message.answer(t["welcome"], reply_markup=main_menu(), parse_mode="Markdown")


@router.callback_query(F.data == "main_menu")
async def cb_main_menu(callback: CallbackQuery):
    theme_key = await get_master_theme(callback.from_user.id)
    t = get_theme(theme_key)
    await callback.message.edit_text(t["welcome"], reply_markup=main_menu(), parse_mode="Markdown")
    await callback.answer()


@router.callback_query(F.data == "cancel")
async def cb_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    theme_key = await get_master_theme(callback.from_user.id)
    t = get_theme(theme_key)
    await callback.message.edit_text(t["welcome"], reply_markup=main_menu(), parse_mode="Markdown")
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


@router.callback_query(F.data.startswith("confirm_book:"))
async def cb_confirm_booking(callback: CallbackQuery):
    appointment_id = int(callback.data.split(":")[1])
    await update_appointment_status(appointment_id, "confirmed")
    await callback.message.edit_text("✅ Запись подтверждена! Ждём вас.")
    await callback.answer()


@router.callback_query(F.data == "help")
async def cb_help(callback: CallbackQuery):
    await callback.message.edit_text(
        HELP_TEXT, reply_markup=main_menu(), parse_mode="Markdown"
    )
    await callback.answer()
