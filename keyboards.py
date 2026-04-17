from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton


# ─── Главное меню ───────────────────────────────────────────────
def main_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="👥 Мои клиенты", callback_data="clients_list"),
            InlineKeyboardButton(text="➕ Новый клиент", callback_data="client_add"),
        ],
        [
            InlineKeyboardButton(text="🔍 Найти клиента", callback_data="client_search"),
            InlineKeyboardButton(text="📅 Записать клиента", callback_data="appointment_new"),
        ],
        [
            InlineKeyboardButton(text="🔔 Кто давно не приходил", callback_data="inactive_clients"),
        ],
        [
            InlineKeyboardButton(text="📊 Статистика", callback_data="stats"),
            InlineKeyboardButton(text="⚙️ Настройки", callback_data="settings"),
        ],
        [
            InlineKeyboardButton(text="💡 Помощь", callback_data="help"),
        ],
    ])


# ─── Список клиентов ─────────────────────────────────────────────
def clients_keyboard(clients: list, page: int = 0, total: int = 0) -> InlineKeyboardMarkup:
    from config import PAGE_SIZE
    buttons = []
    for client in clients:
        cid, name, phone, notes, last_visit = client
        label = f"💅 {name}"
        if last_visit:
            label += f"  · {last_visit[:10]}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"client_view:{cid}")])

    # Кнопки пагинации (◀️ и ▶️)
    nav = []
    if page > 0:
        nav.append(InlineKeyboardButton(text="◀️ Назад", callback_data=f"clients_page:{page - 1}"))
    if (page + 1) * PAGE_SIZE < total:
        nav.append(InlineKeyboardButton(text="Вперёд ▶️", callback_data=f"clients_page:{page + 1}"))
    if nav:
        buttons.append(nav)

    buttons.append([
        InlineKeyboardButton(text="➕ Добавить клиента", callback_data="client_add"),
        InlineKeyboardButton(text="🔍 Найти", callback_data="client_search"),
    ])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def search_results_keyboard(clients: list) -> InlineKeyboardMarkup:
    buttons = []
    for client in clients:
        cid, name, phone, notes, last_visit = client
        label = f"💅 {name}"
        if last_visit:
            label += f"  · {last_visit[:10]}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"client_view:{cid}")])
    buttons.append([InlineKeyboardButton(text="◀️ К списку клиентов", callback_data="clients_list")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Карточка клиента ─────────────────────────────────────────────
def client_card_keyboard(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 История процедур", callback_data=f"client_history:{client_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"client_edit:{client_id}"),
        ],
        [
            InlineKeyboardButton(text="📅 Записать на процедуру", callback_data=f"appointment_for:{client_id}"),
        ],
        [
            InlineKeyboardButton(text="🗑 Удалить клиента", callback_data=f"client_delete:{client_id}"),
            InlineKeyboardButton(text="◀️ Назад", callback_data="clients_list"),
        ],
    ])


def edit_client_keyboard(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👤 Изменить имя", callback_data=f"client_edit_name:{client_id}")],
        [InlineKeyboardButton(text="📱 Изменить телефон", callback_data=f"client_edit_phone:{client_id}")],
        [InlineKeyboardButton(text="📝 Изменить заметку", callback_data=f"client_edit_notes:{client_id}")],
        [InlineKeyboardButton(text="◀️ Назад к клиенту", callback_data=f"client_view:{client_id}")],
    ])


def settings_keyboard(current_days: int) -> InlineKeyboardMarkup:
    options = [30, 40, 60, 90]
    row = []
    for days in options:
        mark = "✅ " if days == current_days else ""
        row.append(InlineKeyboardButton(text=f"{mark}{days} дн.", callback_data=f"set_reminder:{days}"))
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])


def stats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Выгрузить базу в Excel", callback_data="export_excel")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])


# ─── Подтверждение удаления ──────────────────────────────────────
def confirm_delete_keyboard(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"client_delete_confirm:{client_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"client_view:{client_id}"),
        ]
    ])


# ─── Выбор клиента для записи ────────────────────────────────────
def select_client_keyboard(clients: list) -> InlineKeyboardMarkup:
    buttons = []
    for client in clients:
        cid, name, phone, notes, last_visit = client
        buttons.append([InlineKeyboardButton(text=f"💅 {name}", callback_data=f"appointment_for:{cid}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Неактивные клиенты ──────────────────────────────────────────
def inactive_clients_keyboard(clients: list) -> InlineKeyboardMarkup:
    buttons = []
    for client in clients:
        cid, name, phone, last_visit, days_ago = client
        buttons.append([
            InlineKeyboardButton(
                text=f"💅 {name} — {days_ago} дн. назад",
                callback_data=f"client_view:{cid}"
            )
        ])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Кнопка отмены в любом диалоге ──────────────────────────────
def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


# ─── Назад в меню ────────────────────────────────────────────────
def back_to_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
    ])
