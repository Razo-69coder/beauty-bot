from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime, timedelta

from database import (
    get_master_info_by_telegram, get_available_slots,
    get_client_by_telegram, add_client_with_telegram, add_appointment,
)
from keyboards import dates_keyboard, slots_keyboard, booking_confirm_keyboard

router = Router()

DAYS_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}


class BookingForm(StatesGroup):
    date = State()
    time = State()


async def start_booking_flow(message: Message, state: FSMContext, master_telegram_id: int):
    """Запускает процесс записи клиента к мастеру"""
    master = await get_master_info_by_telegram(master_telegram_id)
    if not master:
        await message.answer("Мастер не найден. Проверьте ссылку.")
        return

    await state.update_data(
        master_telegram_id=master_telegram_id,
        master_id=master["id"],
        master_name=master["name"],
        work_start=master["work_start"],
        work_end=master["work_end"],
        slot_duration=master["slot_duration"],
    )

    # Собираем ближайшие 7 дней, у которых есть свободные слоты
    today = datetime.now().date()
    available_dates = []
    for i in range(1, 8):
        d = today + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        slots = await get_available_slots(
            master["id"], date_str,
            master["work_start"], master["work_end"], master["slot_duration"]
        )
        if slots:
            available_dates.append(date_str)

    if not available_dates:
        await message.answer(
            f"😔 У мастера *{master['name']}* нет свободных слотов на ближайшую неделю.\n\n"
            f"Свяжитесь с мастером напрямую.",
            parse_mode="Markdown"
        )
        return

    await state.set_state(BookingForm.date)
    await message.answer(
        f"💅 *Запись к мастеру {master['name']}*\n\n"
        f"Выберите удобную дату:",
        reply_markup=dates_keyboard(available_dates),
        parse_mode="Markdown"
    )


@router.callback_query(BookingForm.date, F.data.startswith("book_date:"))
async def cb_select_date(callback: CallbackQuery, state: FSMContext):
    date = callback.data.split(":", 1)[1]
    data = await state.get_data()

    slots = await get_available_slots(
        data["master_id"], date,
        data["work_start"], data["work_end"], data["slot_duration"]
    )
    if not slots:
        await callback.answer("На этот день нет свободных слотов", show_alert=True)
        return

    await state.update_data(date=date)
    await state.set_state(BookingForm.time)

    date_fmt = datetime.strptime(date, "%Y-%m-%d").strftime("%d.%m.%Y")
    day_name = DAYS_RU[datetime.strptime(date, "%Y-%m-%d").weekday()]
    await callback.message.edit_text(
        f"📅 *{date_fmt} ({day_name})*\n\nВыберите время:",
        reply_markup=slots_keyboard(slots, date),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(BookingForm.date, F.data == "book_back")
async def cb_back_to_dates(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(BookingForm.date)

    today = datetime.now().date()
    available_dates = []
    for i in range(1, 8):
        d = today + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        slots = await get_available_slots(
            data["master_id"], date_str,
            data["work_start"], data["work_end"], data["slot_duration"]
        )
        if slots:
            available_dates.append(date_str)

    await callback.message.edit_text(
        f"💅 *Запись к мастеру {data['master_name']}*\n\nВыберите удобную дату:",
        reply_markup=dates_keyboard(available_dates),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(BookingForm.time, F.data.startswith("book_time:"))
async def cb_select_time(callback: CallbackQuery, state: FSMContext):
    time = callback.data.split(":", 1)[1]
    data = await state.get_data()

    # Ищем клиента по telegram_id или создаём нового
    client = await get_client_by_telegram(data["master_id"], callback.from_user.id)
    if client:
        client_id = client["id"]
        client_name = client["name"]
    else:
        client_name = callback.from_user.full_name or "Клиент"
        client_id = await add_client_with_telegram(
            master_id=data["master_id"],
            name=client_name,
            phone="—",
            telegram_id=callback.from_user.id,
        )

    appt_id = await add_appointment(
        client_id=client_id,
        master_id=data["master_id"],
        procedure="Запись",
        appointment_date=data["date"],
        time=time,
        status="pending",
    )

    await state.clear()

    date_fmt = datetime.strptime(data["date"], "%Y-%m-%d").strftime("%d.%m.%Y")
    day_name = DAYS_RU[datetime.strptime(data["date"], "%Y-%m-%d").weekday()]

    # Уведомляем мастера
    try:
        await callback.bot.send_message(
            data["master_telegram_id"],
            f"🔔 *Новая запись!*\n\n"
            f"👤 Клиент: {client_name}\n"
            f"📅 Дата: {date_fmt} ({day_name})\n"
            f"🕐 Время: {time}\n\n"
            f"Подтвердить запись?",
            reply_markup=booking_confirm_keyboard(appt_id),
            parse_mode="Markdown"
        )
    except Exception:
        pass

    await callback.message.edit_text(
        f"✅ *Запись создана!*\n\n"
        f"📅 {date_fmt} ({day_name}) в {time}\n\n"
        f"Мастер подтвердит запись — вы получите уведомление в боте.",
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(BookingForm.time, F.data == "book_back")
async def cb_back_from_time(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    await state.set_state(BookingForm.date)

    today = datetime.now().date()
    available_dates = []
    for i in range(1, 8):
        d = today + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        slots = await get_available_slots(
            data["master_id"], date_str,
            data["work_start"], data["work_end"], data["slot_duration"]
        )
        if slots:
            available_dates.append(date_str)

    await callback.message.edit_text(
        f"💅 *Запись к мастеру {data['master_name']}*\n\nВыберите удобную дату:",
        reply_markup=dates_keyboard(available_dates),
        parse_mode="Markdown"
    )
    await callback.answer()
