from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery

from database import (
    get_or_create_master, get_master_info,
    get_clients_inactive_range, get_clients_with_telegram,
)
from keyboards import templates_keyboard, tpl_confirm_keyboard, back_to_menu

router = Router()

TEMPLATES = {
    "correction": {
        "title": "Приглашение на коррекцию",
        "text": "💅 Привет, {name}!\n\nПрошло около 2–3 недель после вашего визита — самое время записаться на коррекцию! Жду вас 🗓",
        "min_days": 14,
        "max_days": 30,
    },
    "miss_you": {
        "title": "Скучаем по вам",
        "text": "💔 {name}, мы по вам скучаем!\n\nДавно не видели вас. Запишитесь на процедуру — будем рады встрече! ✨",
        "min_days": 30,
        "max_days": None,
    },
    "congrats": {
        "title": "Поздравление",
        "text": "🎉 Привет, {name}!\n\nСпасибо, что выбираете нас. Вы — лучшие клиенты! Ждём вас снова 💅",
        "min_days": 0,
        "max_days": None,
    },
}


async def _get_clients_for_template(master_id: int, tpl_type: str) -> list:
    tpl = TEMPLATES[tpl_type]
    if tpl["min_days"] == 0:
        return await get_clients_with_telegram(master_id)
    return await get_clients_inactive_range(master_id, tpl["min_days"], tpl["max_days"])


@router.callback_query(F.data == "tpl_templates")
async def cb_templates_menu(callback: CallbackQuery):
    await callback.message.edit_text(
        "💌 *Шаблоны сообщений*\n\n"
        "Бот отправит сообщение подходящим клиентам.\n"
        "Выбери шаблон:",
        reply_markup=templates_keyboard(),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tpl_send:"))
async def cb_tpl_send(callback: CallbackQuery):
    tpl_type = callback.data.split(":")[1]
    if tpl_type not in TEMPLATES:
        await callback.answer("Неизвестный шаблон", show_alert=True)
        return

    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    clients = await _get_clients_for_template(master_id, tpl_type)
    tpl = TEMPLATES[tpl_type]

    if not clients:
        await callback.message.edit_text(
            f"💌 *{tpl['title']}*\n\n"
            "Нет подходящих клиентов для этого шаблона.\n\n"
            "_(Клиенты должны быть подключены к боту и иметь подходящий статус)_",
            reply_markup=back_to_menu(),
            parse_mode="Markdown",
        )
        await callback.answer()
        return

    preview = tpl["text"].format(name="Анна")
    await callback.message.edit_text(
        f"💌 *{tpl['title']}*\n\n"
        f"Получателей: *{len(clients)}* чел.\n\n"
        f"Текст сообщения:\n_{preview}_",
        reply_markup=tpl_confirm_keyboard(tpl_type, len(clients)),
        parse_mode="Markdown",
    )
    await callback.answer()


@router.callback_query(F.data.startswith("tpl_confirm:"))
async def cb_tpl_confirm(callback: CallbackQuery, bot: Bot):
    tpl_type = callback.data.split(":")[1]
    if tpl_type not in TEMPLATES:
        await callback.answer("Неизвестный шаблон", show_alert=True)
        return

    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    clients = await _get_clients_for_template(master_id, tpl_type)
    tpl = TEMPLATES[tpl_type]

    sent = 0
    for client_id, name, telegram_id, *_ in clients:
        try:
            await bot.send_message(
                telegram_id,
                tpl["text"].format(name=name),
                parse_mode="Markdown",
            )
            sent += 1
        except Exception:
            pass

    await callback.message.edit_text(
        f"✅ *Рассылка завершена!*\n\n"
        f"Отправлено: *{sent}* из {len(clients)} клиентов.",
        reply_markup=back_to_menu(),
        parse_mode="Markdown",
    )
    await callback.answer(f"Отправлено {sent} сообщений")
