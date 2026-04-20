import re
from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from datetime import datetime

from database import get_or_create_master, get_clients, add_appointment
from keyboards import select_client_keyboard, cancel_keyboard, client_card_keyboard, back_to_menu

router = Router()


# ─── Состояния FSM для добавления записи ────────────────────────
class AddAppointmentForm(StatesGroup):
    client_id = State()
    procedure = State()
    date = State()
    time = State()
    price = State()
    notes = State()
    photo = State()


# ─── Начало — выбор клиента ──────────────────────────────────────
@router.callback_query(F.data == "appointment_new")
async def cb_appointment_new(callback: CallbackQuery, state: FSMContext):
    master_id = await get_or_create_master(
        callback.from_user.id, callback.from_user.full_name
    )
    clients = await get_clients(master_id)

    if not clients:
        await callback.message.edit_text(
            "👥 *Нет клиентов*\n\nСначала добавь клиента через ➕ Новый клиент",
            reply_markup=back_to_menu(),
            parse_mode="Markdown"
        )
        await callback.answer()
        return

    await state.set_state(AddAppointmentForm.client_id)
    await callback.message.edit_text(
        "📅 *Новая запись*\n\nВыбери клиента:",
        reply_markup=select_client_keyboard(clients),
        parse_mode="Markdown"
    )
    await callback.answer()


# ─── Клиент выбран сразу из карточки ─────────────────────────────
@router.callback_query(F.data.startswith("appointment_for:"))
async def cb_appointment_for(callback: CallbackQuery, state: FSMContext):
    client_id = int(callback.data.split(":")[1])
    await state.update_data(client_id=client_id)
    await state.set_state(AddAppointmentForm.procedure)

    from database import get_client
    client = await get_client(client_id)

    await callback.message.edit_text(
        f"📅 *Запись для {client['name']}*\n\n"
        f"Что будем делать?\n\n"
        f"_Например: Маникюр, Наращивание ресниц, Коррекция бровей_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(AddAppointmentForm.procedure)
async def process_procedure(message: Message, state: FSMContext):
    await state.update_data(procedure=message.text.strip())
    await state.set_state(AddAppointmentForm.date)
    today = datetime.now().strftime("%d.%m.%Y")
    await message.answer(
        f"✅ Процедура: *{message.text.strip()}*\n\n"
        f"📅 Введи дату визита:\n\n"
        f"_Формат: ДД.ММ.ГГГГ  —  например {today}_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddAppointmentForm.date)
async def process_date(message: Message, state: FSMContext):
    date_str = message.text.strip()
    # Проверяем формат даты
    try:
        datetime.strptime(date_str, "%d.%m.%Y")
    except ValueError:
        await message.answer(
            "❌ Неверный формат даты\n\nВведи в формате *ДД.ММ.ГГГГ*\n_Например: 25.04.2025_",
            reply_markup=cancel_keyboard(),
            parse_mode="Markdown"
        )
        return

    # Сохраняем в формате ГГГГ-ММ-ДД для сортировки
    dt = datetime.strptime(date_str, "%d.%m.%Y")
    await state.update_data(date=dt.strftime("%Y-%m-%d"))
    await state.set_state(AddAppointmentForm.time)
    await message.answer(
        f"✅ Дата: *{date_str}*\n\n"
        f"🕐 Введи время визита:\n\n"
        f"_Формат: ЧЧ:ММ — например 14:30_\n"
        f"Или напиши *-* чтобы пропустить",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddAppointmentForm.time)
async def process_time(message: Message, state: FSMContext):
    text = message.text.strip()
    if text == "-":
        time_val = ""
    elif re.match(r'^\d{1,2}:\d{2}$', text):
        time_val = text
    else:
        await message.answer(
            "❌ Неверный формат. Введи время как *ЧЧ:ММ*\n_Например: 14:30_\n\nИли напиши *-* чтобы пропустить",
            reply_markup=cancel_keyboard(),
            parse_mode="Markdown"
        )
        return
    await state.update_data(time=time_val)
    await state.set_state(AddAppointmentForm.price)
    await message.answer(
        f"✅ Время: *{time_val if time_val else 'не указано'}*\n\n"
        f"💰 Сколько стоила процедура?\n\n"
        f"_Введи сумму в рублях или напиши *-* чтобы пропустить_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddAppointmentForm.price)
async def process_price(message: Message, state: FSMContext):
    text = message.text.strip()
    price = 0
    if text != "-":
        try:
            price = int(text.replace("₽", "").replace(" ", ""))
        except ValueError:
            await message.answer(
                "❌ Введи число или *-* чтобы пропустить",
                reply_markup=cancel_keyboard(),
                parse_mode="Markdown"
            )
            return

    await state.update_data(price=price)
    await state.set_state(AddAppointmentForm.notes)
    await message.answer(
        "📝 Добавить заметку к процедуре?\n\n"
        "_Например: клиент попросил арочную форму, использовали гель Bluesky_\n\n"
        "Или напиши *-* чтобы пропустить",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddAppointmentForm.notes)
async def process_notes(message: Message, state: FSMContext):
    notes = "" if message.text.strip() == "-" else message.text.strip()
    await state.update_data(notes=notes)
    await state.set_state(AddAppointmentForm.photo)
    await message.answer(
        "📸 Хочешь прикрепить фото работы?\n\n"
        "Отправь фото или напиши *-* чтобы пропустить",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddAppointmentForm.photo, F.photo)
async def process_photo(message: Message, state: FSMContext):
    photo_id = message.photo[-1].file_id
    await _save_appointment(message, state, photo_id)


@router.message(AddAppointmentForm.photo, F.text)
async def process_no_photo(message: Message, state: FSMContext):
    await _save_appointment(message, state, "")


async def _save_appointment(message: Message, state: FSMContext, photo_id: str):
    data = await state.get_data()
    master_id = await get_or_create_master(
        message.from_user.id, message.from_user.full_name
    )

    appointment_id = await add_appointment(
        client_id=data["client_id"],
        master_id=master_id,
        procedure=data["procedure"],
        appointment_date=data["date"],
        price=data.get("price", 0),
        notes=data.get("notes", ""),
        photo_id=photo_id,
        time=data.get("time", ""),
    )
    await state.clear()

    # Красивый итог
    date_formatted = datetime.strptime(data["date"], "%Y-%m-%d").strftime("%d.%m.%Y")
    text = (
        f"🎉 *Запись сохранена!*\n\n"
        f"💅 Процедура: {data['procedure']}\n"
        f"📅 Дата: {date_formatted}\n"
    )
    if data.get("time"):
        text += f"🕐 Время: {data['time']}\n"
    if data.get("price"):
        text += f"💰 Стоимость: {data['price']}₽\n"
    if data.get("notes"):
        text += f"📝 Заметка: {data['notes']}\n"

    await message.answer(
        text,
        reply_markup=client_card_keyboard(data["client_id"]),
        parse_mode="Markdown"
    )
