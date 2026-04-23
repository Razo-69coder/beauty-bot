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


# ─── Старт бота ────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext, command: CommandObject):
    """Обработка любого start"""
    args = command.args or ""
    
    # ?start=confirm_123 - подтверждение записи
    if args.startswith("confirm_"):
        try:
            # убираем "confirm_" prefix
            appt_id = int(args.replace("confirm_", ""))
        except Exception as e:
            await message.answer(f"Ошибка: {e}")
            return
        
        appt = await get_appointment_by_id(appt_id)
        
        if not appt:
            await message.answer("Запись не найдена.")
            return
        
        if appt.get('status') != 'pending':
            await message.answer("Уже подтверждено.")
            return
        
        # Привязываем telegram и подтверждаем
        await assign_client_telegram(appt['client_id'], message.from_user.id)
        await update_appointment_status(appt_id, "confirmed")
        await message.answer("✅ Запись подтверждена!")
        return
    
    # ?start=book_XXX - запись к мастеру
    if args.startswith("book_"):
        try:
            master_telegram_id = int(args.split("_", 1)[1])
        except (ValueError, IndexError):
            await message.answer("Неверная ссылка.")
            return
        from handlers.booking import start_booking_flow
        await start_booking_flow(message, state, master_telegram_id)
        return
    
    # ?start=reg - старый формат подтверждения
    if args == "reg":
        appointments = await get_client_pending_appointments(message.from_user.id)
        if not appointments:
            await message.answer("Нет записей. Запишитесь через ссылку мастера.")
            return
        for appt in appointments[:3]:
            await message.answer(
                f"📅 Подтверждение: {appt['appointment_date']} {appt['time']}",
                reply_markup=confirm_appointment_keyboard(appt['id'])
            )
        return
    
    # Обычный старт - показываем меню мастера
    await get_or_create_master(message.from_user.id, message.from_user.full_name)
    theme_key = await get_master_theme(message.from_user.id)
    t = get_theme(theme_key)
    await message.answer(t["welcome"], reply_markup=main_menu(), parse_mode="Markdown")


# ─── Callbacks ───────────────────────────────────────────

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