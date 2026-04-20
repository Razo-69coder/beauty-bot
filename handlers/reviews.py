from aiogram import Router, F, Bot
from aiogram.types import CallbackQuery

from database import save_review, get_master_info

router = Router()

STARS = {1: "⭐", 2: "⭐⭐", 3: "⭐⭐⭐", 4: "⭐⭐⭐⭐", 5: "⭐⭐⭐⭐⭐"}


@router.callback_query(F.data.startswith("review_rating:"))
async def cb_review_rating(callback: CallbackQuery, bot: Bot):
    _, appt_id_str, rating_str = callback.data.split(":")
    appointment_id = int(appt_id_str)
    rating = int(rating_str)

    client_id = callback.from_user.id

    # Сохраняем отзыв — master_id подтянем через запись
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT client_id, master_id FROM appointments WHERE id=$1", appointment_id
        )
    if not row:
        await callback.answer("Запись не найдена", show_alert=True)
        return

    db_client_id = row["client_id"]
    master_id = row["master_id"]

    await save_review(appointment_id, db_client_id, master_id, rating)

    stars = STARS.get(rating, "⭐")
    await callback.message.edit_text(
        f"Спасибо за оценку! {stars}\n\nВаш отзыв помогает нам становиться лучше 💅",
    )
    await callback.answer()

    # Уведомляем мастера
    master = await get_master_info(master_id)
    if master:
        client_name = callback.from_user.full_name or "Клиент"
        try:
            await bot.send_message(
                master["telegram_id"],
                f"⭐ *Новый отзыв!*\n\n"
                f"Клиент: {client_name}\n"
                f"Оценка: {stars} ({rating}/5)",
                parse_mode="Markdown",
            )
        except Exception:
            pass
