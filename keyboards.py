from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import datetime
from config import WEBHOOK_URL
from themes import get_theme, THEMES

WEBAPP_URL = f"{WEBHOOK_URL}/app/dashboard.html?v=5" if WEBHOOK_URL else ""

DAYS_SHORT = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}


# ─── Главное меню ────────────────────────────────────────────────────
def main_menu() -> InlineKeyboardMarkup:
    if WEBAPP_URL:
        return InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💅 Открыть Beauty Book", web_app={"url": WEBAPP_URL})],
        ])
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔑 Войти в личный кабинет", callback_data="get_login_code")],
    ])


# ─── Расписание мастера ──────────────────────────────────────────────
def schedule_keyboard(date: str, prev_date: str, next_date: str, appointments: list) -> InlineKeyboardMarkup:
    buttons = []

    # Кнопки подтверждения/отмены для ожидающих записей
    for appt_id, client_name, procedure, time, status, phone in appointments:
        if status == "pending":
            buttons.append([
                InlineKeyboardButton(
                    text=f"✅ Подтвердить {time} — {client_name}",
                    callback_data=f"booking_confirm:{appt_id}"
                ),
            ])
            buttons.append([
                InlineKeyboardButton(
                    text=f"❌ Отменить {time} — {client_name}",
                    callback_data=f"booking_cancel:{appt_id}"
                ),
            ])

    # Навигация по дням
    buttons.append([
        InlineKeyboardButton(text="◀️", callback_data=f"schedule_day:{prev_date}"),
        InlineKeyboardButton(text="Сегодня", callback_data=f"schedule_day:{datetime.now().strftime('%Y-%m-%d')}"),
        InlineKeyboardButton(text="▶️", callback_data=f"schedule_day:{next_date}"),
    ])
    buttons.append([InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Онлайн-запись: выбор даты ───────────────────────────────────────
def dates_keyboard(dates: list[str]) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for date_str in dates:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        label = f"{dt.strftime('%d.%m')} {DAYS_SHORT[dt.weekday()]}"
        row.append(InlineKeyboardButton(text=label, callback_data=f"book_date:{date_str}"))
        if len(row) == 3:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Онлайн-запись: выбор времени ────────────────────────────────────
def slots_keyboard(slots: list[str], date: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for slot in slots:
        row.append(InlineKeyboardButton(text=slot, callback_data=f"book_time:{slot}"))
        if len(row) == 4:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="book_back")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Уведомление мастеру о новой записи ─────────────────────────────
def booking_confirm_keyboard(appt_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"booking_confirm:{appt_id}"),
            InlineKeyboardButton(text="❌ Отменить", callback_data=f"booking_cancel:{appt_id}"),
        ]
    ])


# ─── Настройки: рабочие часы ─────────────────────────────────────────
def work_hours_keyboard(work_start: int, work_end: int, slot_duration: int) -> InlineKeyboardMarkup:
    # Выбор времени начала
    start_row = []
    for h in [8, 9, 10, 11]:
        mark = "✅ " if h == work_start else ""
        start_row.append(InlineKeyboardButton(text=f"{mark}{h}:00", callback_data=f"wh_start:{h}"))

    # Выбор времени конца
    end_row = []
    for h in [18, 19, 20, 21, 22]:
        mark = "✅ " if h == work_end else ""
        end_row.append(InlineKeyboardButton(text=f"{mark}{h}:00", callback_data=f"wh_end:{h}"))

    # Длительность слота
    duration_row = []
    for d in [30, 45, 60, 90]:
        mark = "✅ " if d == slot_duration else ""
        label = f"{mark}{d} мин"
        duration_row.append(InlineKeyboardButton(text=label, callback_data=f"wh_dur:{d}"))

    return InlineKeyboardMarkup(inline_keyboard=[
        start_row,
        end_row,
        duration_row,
        [InlineKeyboardButton(text="◀️ Назад к настройкам", callback_data="settings")],
    ])


# ─── Настройки: главная ──────────────────────────────────────────────
def settings_keyboard(current_days: int, theme_key: str = "pink") -> InlineKeyboardMarkup:
    t = get_theme(theme_key)
    options = [30, 40, 60, 90]
    row = []
    for days in options:
        mark = "✅ " if days == current_days else ""
        row.append(InlineKeyboardButton(text=f"{mark}{days} дн.", callback_data=f"set_reminder:{days}"))
    return InlineKeyboardMarkup(inline_keyboard=[
        row,
        [InlineKeyboardButton(text=f"{t['icon_time']} Рабочее время", callback_data="settings_work_hours")],
        [InlineKeyboardButton(text=f"{t['icon_link']} Ссылка для записи", callback_data="settings_booking_link")],
        [InlineKeyboardButton(text=f"{t['icon_deposit']} Предоплата", callback_data="settings_deposit")],
        [InlineKeyboardButton(text=f"{t['icon_template']} Шаблоны сообщений", callback_data="tpl_templates")],
        [InlineKeyboardButton(text=f"{t['icon_theme']} Тема оформления", callback_data="settings_theme")],
        [InlineKeyboardButton(text=f"{t['icon_home']} Главное меню", callback_data="main_menu")],
    ])


# ─── Настройки: выбор темы ───────────────────────────────────────────
def theme_keyboard(current_theme: str) -> InlineKeyboardMarkup:
    buttons = []
    for key, data in THEMES.items():
        mark = "✅ " if key == current_theme else ""
        buttons.append([
            InlineKeyboardButton(text=f"{mark}{data['name']}", callback_data=f"set_theme:{key}")
        ])
    buttons.append([InlineKeyboardButton(text="◀️ Назад к настройкам", callback_data="settings")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Список клиентов ─────────────────────────────────────────────────
def clients_keyboard(clients: list, page: int = 0, total: int = 0) -> InlineKeyboardMarkup:
    from config import PAGE_SIZE
    buttons = []
    for client in clients:
        cid, name, phone, notes, last_visit = client
        label = f"💅 {name}"
        if last_visit:
            label += f"  · {last_visit[:10]}"
        buttons.append([InlineKeyboardButton(text=label, callback_data=f"client_view:{cid}")])

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


# ─── Карточка клиента ────────────────────────────────────────────────
def client_card_keyboard(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="📋 История процедур", callback_data=f"client_history:{client_id}"),
            InlineKeyboardButton(text="✏️ Редактировать", callback_data=f"client_edit:{client_id}"),
        ],
        [InlineKeyboardButton(text="📅 Записать на процедуру", callback_data=f"appointment_for:{client_id}")],
        [InlineKeyboardButton(text="📦 Абонементы", callback_data=f"sub_menu:{client_id}")],
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


# ─── Подтверждение удаления ──────────────────────────────────────────
def confirm_delete_keyboard(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да, удалить", callback_data=f"client_delete_confirm:{client_id}"),
            InlineKeyboardButton(text="❌ Отмена", callback_data=f"client_view:{client_id}"),
        ]
    ])


# ─── Выбор клиента для записи ────────────────────────────────────────
def select_client_keyboard(clients: list) -> InlineKeyboardMarkup:
    buttons = []
    for client in clients:
        cid, name, phone, notes, last_visit = client
        buttons.append([InlineKeyboardButton(text=f"💅 {name}", callback_data=f"appointment_for:{cid}")])
    buttons.append([InlineKeyboardButton(text="◀️ Назад", callback_data="main_menu")])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ─── Неактивные клиенты ──────────────────────────────────────────────
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


# ─── Статистика ──────────────────────────────────────────────────────
def stats_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📥 Выгрузить базу в Excel", callback_data="export_excel")],
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")],
    ])


# ─── Абонементы ──────────────────────────────────────────────────────
def subscriptions_keyboard(subs: list, client_id: int) -> InlineKeyboardMarkup:
    buttons = []
    for sub_id, name, total, used, price in subs:
        if used < total:
            buttons.append([
                InlineKeyboardButton(
                    text=f"➖ Списать сеанс: {name}",
                    callback_data=f"sub_use:{sub_id}:{client_id}"
                )
            ])
    buttons.append([
        InlineKeyboardButton(text="➕ Новый абонемент", callback_data=f"sub_create:{client_id}")
    ])
    buttons.append([
        InlineKeyboardButton(text="◀️ К клиенту", callback_data=f"client_view:{client_id}")
    ])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def back_to_client(client_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="◀️ К клиенту", callback_data=f"client_view:{client_id}")]
    ])


# ─── Вспомогательные ─────────────────────────────────────────────────
def cancel_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


def back_to_menu() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🏠 Главное меню", callback_data="main_menu")]
    ])


# ─── Предоплата ──────────────────────────────────────────────────────
def deposit_client_keyboard(appointment_id: int) -> InlineKeyboardMarkup:
    """Клиент видит это после выбора времени — если нужна предоплата."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Я оплатил(а)", callback_data=f"deposit_paid:{appointment_id}")],
        [InlineKeyboardButton(text="❌ Отменить запись", callback_data=f"deposit_cancel:{appointment_id}")],
    ])


def deposit_master_keyboard(appointment_id: int) -> InlineKeyboardMarkup:
    """Мастер подтверждает или отклоняет оплату клиента."""
    return InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Оплата получена", callback_data=f"deposit_confirm:{appointment_id}"),
            InlineKeyboardButton(text="❌ Не оплачено", callback_data=f"deposit_reject:{appointment_id}"),
        ]
    ])


def deposit_settings_keyboard(enabled: bool, percent: int) -> InlineKeyboardMarkup:
    status = "✅ Включена" if enabled else "❌ Выключена"
    toggle = "deposit_disable" if enabled else "deposit_enable"
    percents = [10, 20, 30, 50]
    prow = [
        InlineKeyboardButton(
            text=f"{'✅ ' if p == percent else ''}{p}%",
            callback_data=f"deposit_pct:{p}"
        ) for p in percents
    ]
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=f"Предоплата: {status}", callback_data=toggle)],
        prow,
        [InlineKeyboardButton(text="◀️ Назад к настройкам", callback_data="settings")],
    ])


# ─── Шаблоны сообщений ───────────────────────────────────────────────
def templates_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔄 Приглашение на коррекцию (14–30 дн.)", callback_data="tpl_send:correction")],
        [InlineKeyboardButton(text="💔 «Скучаем по вам» (30+ дн.)", callback_data="tpl_send:miss_you")],
        [InlineKeyboardButton(text="🎉 Поздравление (все клиенты)", callback_data="tpl_send:congrats")],
        [InlineKeyboardButton(text="◀️ Назад к настройкам", callback_data="settings")],
    ])


def tpl_confirm_keyboard(tpl_type: str, count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=f"✅ Отправить {count} клиентам",
            callback_data=f"tpl_confirm:{tpl_type}"
        )],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="tpl_templates")],
    ])


# ─── Отзывы ──────────────────────────────────────────────────────────
def review_rating_keyboard(appointment_id: int) -> InlineKeyboardMarkup:
    stars = ["⭐", "⭐⭐", "⭐⭐⭐", "⭐⭐⭐⭐", "⭐⭐⭐⭐⭐"]
    row = [
        InlineKeyboardButton(text=s, callback_data=f"review_rating:{appointment_id}:{i + 1}")
        for i, s in enumerate(stars)
    ]
    return InlineKeyboardMarkup(inline_keyboard=[row])
