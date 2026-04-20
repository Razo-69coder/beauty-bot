"""
Темы оформления Telegram-бота Beauty Book.
Тема влияет на: эмодзи, заголовки, тон сообщений, иконки кнопок.
"""

THEMES = {
    "pink": {
        "name": "💅 Розовая (Bratz)",
        # Главное приветствие
        "welcome": (
            "✨💅 *Beauty Book* 💅✨\n\n"
            "Твоя CRM для мастера красоты.\n"
            "Управляй записями, клиентами и доходом — всё в одном месте 🔥\n\n"
            "👇 Открой приложение:"
        ),
        # Иконки разделов
        "icon_clients":     "💅",
        "icon_schedule":    "🗓",
        "icon_stats":       "📊",
        "icon_settings":    "⚙️",
        "icon_reminder":    "🔔",
        "icon_back":        "◀️",
        "icon_home":        "🏠",
        "icon_add":         "➕",
        "icon_search":      "🔍",
        "icon_card":        "💳",
        "icon_link":        "🔗",
        "icon_time":        "🕐",
        "icon_template":    "💌",
        "icon_deposit":     "💳",
        "icon_theme":       "🎨",
        "icon_ok":          "✅",
        "icon_cancel":      "❌",
        "icon_minus":       "➖",
        "icon_history":     "📋",
        "icon_edit":        "✏️",
        "icon_delete":      "🗑",
        "icon_export":      "📥",
        "icon_sub":         "📦",
        "icon_appointment": "📅",
        # Заголовки разделов
        "header_settings":  "💅 *Настройки Beauty Book*",
        "header_clients":   "💅 *Мои клиентки*",
        "header_schedule":  "🗓 *Расписание*",
        "header_stats":     "📊 *Статистика*",
        "header_work_hours": "🕐 *Рабочее время*",
        "header_booking_link": "🔗 *Ссылка для записи*",
        "header_deposit":   "💳 *Предоплата*",
        "header_templates": "💌 *Шаблоны сообщений*",
        "header_theme":     "🎨 *Выбор темы*",
        # Фраза в настройках (напоминалки)
        "reminder_label": "🔔 Напоминать о клиентах, которые не приходили:",
        # Декор разделителя
        "divider": "━━━━━━━━━━━━━━━━",
    },
    "peach": {
        "name": "🪙 Платина (холодный гламур)",
        "welcome": (
            "🌸 *Beauty Book* 🌸\n\n"
            "Ваша CRM для мастера красоты.\n"
            "Записи, клиенты и аналитика — всё под рукой.\n\n"
            "👇 Перейдите в приложение:"
        ),
        "icon_clients":     "🌺",
        "icon_schedule":    "📅",
        "icon_stats":       "📈",
        "icon_settings":    "⚙️",
        "icon_reminder":    "🔔",
        "icon_back":        "◀️",
        "icon_home":        "🏡",
        "icon_add":         "＋",
        "icon_search":      "🔎",
        "icon_card":        "💳",
        "icon_link":        "🔗",
        "icon_time":        "⏰",
        "icon_template":    "📩",
        "icon_deposit":     "💰",
        "icon_theme":       "🎨",
        "icon_ok":          "✓",
        "icon_cancel":      "✕",
        "icon_minus":       "−",
        "icon_history":     "📜",
        "icon_edit":        "🖊",
        "icon_delete":      "🗑",
        "icon_export":      "📤",
        "icon_sub":         "🎁",
        "icon_appointment": "🗓",
        "header_settings":  "⚙️ *Настройки*",
        "header_clients":   "🌺 *Клиенты*",
        "header_schedule":  "📅 *Расписание*",
        "header_stats":     "📈 *Статистика*",
        "header_work_hours": "⏰ *Рабочее время*",
        "header_booking_link": "🔗 *Ссылка для записи*",
        "header_deposit":   "💰 *Предоплата*",
        "header_templates": "📩 *Шаблоны сообщений*",
        "header_theme":     "🎨 *Тема оформления*",
        "reminder_label": "🔔 Напоминать о клиентах, которые давно не приходили:",
        "divider": "· · · · · · · · · · · · · · · ·",
    },
}

DEFAULT_THEME = "pink"


def get_theme(theme_key: str) -> dict:
    return THEMES.get(theme_key, THEMES[DEFAULT_THEME])
