from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import (
    get_or_create_master, get_clients, get_client,
    add_client, update_client, delete_client, get_inactive_clients, search_clients
)
from keyboards import (
    clients_keyboard, search_results_keyboard, client_card_keyboard,
    edit_client_keyboard, confirm_delete_keyboard, cancel_keyboard,
    back_to_menu, inactive_clients_keyboard, main_menu
)
from config import REMINDER_DAYS, PAGE_SIZE

router = Router()


# ─── Состояния FSM ───────────────────────────────────────────────
class AddClientForm(StatesGroup):
    name = State()
    phone = State()
    notes = State()


class SearchClientForm(StatesGroup):
    query = State()


class EditClientForm(StatesGroup):
    value = State()


# ─── Вспомогательная функция показа страницы клиентов ────────────
async def show_clients_page(callback: CallbackQuery, master_id: int, page: int):
    clients = await get_clients(master_id)
    total = len(clients)
    page_clients = clients[page * PAGE_SIZE: (page + 1) * PAGE_SIZE]

    if not clients:
        await callback.message.edit_text(
            "👥 *Список клиентов пуст*\n\nДобавь первого клиента, нажав кнопку ниже 👇",
            reply_markup=clients_keyboard([], 0, 0),
            parse_mode="Markdown"
        )
    else:
        pages_total = (total + PAGE_SIZE - 1) // PAGE_SIZE
        text = f"👥 *Твои клиенты* — {total} чел."
        if pages_total > 1:
            text += f"  (стр. {page + 1}/{pages_total})"
        text += "\n\nВыбери клиента для просмотра:"
        await callback.message.edit_text(
            text, reply_markup=clients_keyboard(page_clients, page, total), parse_mode="Markdown"
        )


# ─── Список клиентов (страница 0) ───────────────────────────────
@router.callback_query(F.data == "clients_list")
async def cb_clients_list(callback: CallbackQuery):
    master_id = await get_or_create_master(
        callback.from_user.id, callback.from_user.full_name
    )
    await show_clients_page(callback, master_id, 0)
    await callback.answer()


# ─── Переключение страниц ────────────────────────────────────────
@router.callback_query(F.data.startswith("clients_page:"))
async def cb_clients_page(callback: CallbackQuery):
    page = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(
        callback.from_user.id, callback.from_user.full_name
    )
    await show_clients_page(callback, master_id, page)
    await callback.answer()


# ─── Поиск клиента ───────────────────────────────────────────────
@router.callback_query(F.data == "client_search")
async def cb_client_search(callback: CallbackQuery, state: FSMContext):
    await state.set_state(SearchClientForm.query)
    await callback.message.edit_text(
        "🔍 *Поиск клиента*\n\nВведи имя (или его часть):\n\n_Например: Маша_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(SearchClientForm.query)
async def process_search_query(message: Message, state: FSMContext):
    query = message.text.strip()
    master_id = await get_or_create_master(
        message.from_user.id, message.from_user.full_name
    )
    results = await search_clients(master_id, query)
    await state.clear()

    if not results:
        await message.answer(
            f"🔍 По запросу *«{query}»* ничего не найдено\n\n"
            f"Попробуй другое имя или проверь написание",
            reply_markup=back_to_menu(),
            parse_mode="Markdown"
        )
    else:
        text = f"🔍 *Результаты поиска «{query}»* — {len(results)} чел.\n\nВыбери клиента:"
        await message.answer(
            text, reply_markup=search_results_keyboard(results), parse_mode="Markdown"
        )


# ─── Карточка клиента ─────────────────────────────────────────────
@router.callback_query(F.data.startswith("client_view:"))
async def cb_client_view(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)

    if not client:
        await callback.answer("Клиент не найден", show_alert=True)
        return

    # Последний визит
    from database import get_client_history
    history = await get_client_history(client_id)
    last_visit = history[0][1][:10] if history else "нет записей"
    visits_count = len(history)

    text = (
        f"💅 *{client['name']}*\n\n"
        f"📱 Телефон: `{client['phone']}`\n"
        f"📅 Последний визит: {last_visit}\n"
        f"🔢 Всего процедур: {visits_count}\n"
    )
    if client["notes"]:
        text += f"📝 Заметка: {client['notes']}\n"

    await callback.message.edit_text(
        text, reply_markup=client_card_keyboard(client_id), parse_mode="Markdown"
    )
    await callback.answer()


# ─── Начало добавления клиента ───────────────────────────────────
@router.callback_query(F.data == "client_add")
async def cb_client_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddClientForm.name)
    await callback.message.edit_text(
        "➕ *Новый клиент*\n\nКак зовут клиента?\n\n_Например: Маша Иванова_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(AddClientForm.name)
async def process_client_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AddClientForm.phone)
    await message.answer(
        f"✅ Имя: *{message.text.strip()}*\n\n📱 Теперь введи номер телефона:\n\n_Например: +79001234567_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddClientForm.phone)
async def process_client_phone(message: Message, state: FSMContext):
    await state.update_data(phone=message.text.strip())
    await state.set_state(AddClientForm.notes)
    await message.answer(
        "📝 Хочешь добавить заметку о клиенте?\n\n"
        "_Например: аллергия на лак, любит миндаль_\n\n"
        "Или напиши *-* чтобы пропустить",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddClientForm.notes)
async def process_client_notes(message: Message, state: FSMContext):
    data = await state.get_data()
    notes = "" if message.text.strip() == "-" else message.text.strip()

    master_id = await get_or_create_master(
        message.from_user.id, message.from_user.full_name
    )
    client_id = await add_client(master_id, data["name"], data["phone"], notes)
    await state.clear()

    text = (
        f"🎉 *Клиент добавлен!*\n\n"
        f"💅 {data['name']}\n"
        f"📱 {data['phone']}\n"
    )
    if notes:
        text += f"📝 {notes}\n"

    await message.answer(text, reply_markup=client_card_keyboard(client_id), parse_mode="Markdown")


# ─── История процедур клиента ────────────────────────────────────
@router.callback_query(F.data.startswith("client_history:"))
async def cb_client_history(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    from database import get_client_history
    history = await get_client_history(client_id)

    if not history:
        text = f"📋 *История процедур — {client['name']}*\n\n_Процедур пока нет_"
    else:
        text = f"📋 *История процедур — {client['name']}*\n\n"
        for i, (procedure, date, price, notes, photo_id) in enumerate(history, 1):
            text += f"*{i}. {procedure}*\n"
            text += f"   📅 {date[:10]}"
            if price:
                text += f"  💰 {price}₽"
            if notes:
                text += f"\n   📝 {notes}"
            if photo_id:
                text += "  📸"
            text += "\n\n"

    from keyboards import InlineKeyboardMarkup, InlineKeyboardButton
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ Назад к клиенту", callback_data=f"client_view:{client_id}")]
    ])
    await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")

    # Отправляем фото отдельными сообщениями (edit_text не поддерживает фото)
    if history:
        for procedure, date, price, notes, photo_id in history:
            if photo_id:
                caption = f"📸 *{procedure}* — {date[:10]}"
                await callback.message.answer_photo(photo_id, caption=caption, parse_mode="Markdown")

    await callback.answer()


# ─── Редактирование клиента ──────────────────────────────────────
@router.callback_query(F.data.startswith("client_edit:"))
async def cb_client_edit(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    await callback.message.edit_text(
        f"✏️ *Редактирование — {client['name']}*\n\nЧто изменить?",
        reply_markup=edit_client_keyboard(client_id),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_edit_name:"))
async def cb_edit_name(callback: CallbackQuery, state: FSMContext):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    await state.update_data(client_id=client_id, field="name")
    await state.set_state(EditClientForm.value)
    await callback.message.edit_text(
        f"👤 *Изменить имя*\n\nСейчас: *{client['name']}*\n\nВведи новое имя:",
        reply_markup=cancel_keyboard(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_edit_phone:"))
async def cb_edit_phone(callback: CallbackQuery, state: FSMContext):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    await state.update_data(client_id=client_id, field="phone")
    await state.set_state(EditClientForm.value)
    await callback.message.edit_text(
        f"📱 *Изменить телефон*\n\nСейчас: `{client['phone']}`\n\nВведи новый номер:",
        reply_markup=cancel_keyboard(), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_edit_notes:"))
async def cb_edit_notes(callback: CallbackQuery, state: FSMContext):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    await state.update_data(client_id=client_id, field="notes")
    await state.set_state(EditClientForm.value)
    notes_text = client['notes'] or "_нет заметки_"
    await callback.message.edit_text(
        f"📝 *Изменить заметку*\n\nСейчас: {notes_text}\n\nВведи новую заметку (или `-` чтобы очистить):",
        reply_markup=cancel_keyboard(), parse_mode="Markdown"
    )
    await callback.answer()


@router.message(EditClientForm.value)
async def process_edit_value(message: Message, state: FSMContext):
    data = await state.get_data()
    client_id = data["client_id"]
    field = data["field"]
    new_value = message.text.strip()

    client = await get_client(client_id)
    master_id = await get_or_create_master(message.from_user.id, message.from_user.full_name)

    name = client["name"]
    phone = client["phone"]
    notes = client["notes"] or ""

    if field == "name":
        name = new_value
    elif field == "phone":
        phone = new_value
    elif field == "notes":
        notes = "" if new_value == "-" else new_value

    await update_client(client_id, master_id, name, phone, notes)
    await state.clear()

    field_names = {"name": "Имя", "phone": "Телефон", "notes": "Заметка"}
    await message.answer(
        f"✅ *{field_names[field]} обновлено!*",
        reply_markup=client_card_keyboard(client_id),
        parse_mode="Markdown"
    )


# ─── Удаление клиента ────────────────────────────────────────────
@router.callback_query(F.data.startswith("client_delete:"))
async def cb_client_delete(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    client = await get_client(client_id)
    await callback.message.edit_text(
        f"🗑 Удалить клиента *{client['name']}*?\n\n"
        f"⚠️ Вся история процедур тоже удалится",
        reply_markup=confirm_delete_keyboard(client_id),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("client_delete_confirm:"))
async def cb_client_delete_confirm(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(
        callback.from_user.id, callback.from_user.full_name
    )
    await delete_client(client_id, master_id)
    await callback.message.edit_text(
        "✅ Клиент удалён", reply_markup=back_to_menu(), parse_mode="Markdown"
    )
    await callback.answer()


# ─── Кто давно не приходил ───────────────────────────────────────
@router.callback_query(F.data == "inactive_clients")
async def cb_inactive_clients(callback: CallbackQuery):
    master_id = await get_or_create_master(
        callback.from_user.id, callback.from_user.full_name
    )
    clients = await get_inactive_clients(master_id, REMINDER_DAYS)

    if not clients:
        await callback.message.edit_text(
            f"✨ *Все клиенты активны!*\n\n"
            f"Нет клиентов, которые не приходили больше {REMINDER_DAYS} дней 🎉",
            reply_markup=back_to_menu(),
            parse_mode="Markdown"
        )
    else:
        text = (
            f"🔔 *Давно не приходили* — {len(clients)} чел.\n\n"
            f"Возможно, им стоит написать 💌\n\n"
        )
        await callback.message.edit_text(
            text,
            reply_markup=inactive_clients_keyboard(clients),
            parse_mode="Markdown"
        )
    await callback.answer()
