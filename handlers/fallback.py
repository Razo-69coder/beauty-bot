from aiogram import Router
from aiogram.types import Message
from aiogram.filters import StateFilter
from aiogram.fsm.context import FSMContext

from database import get_or_create_master, get_master_theme
from keyboards import main_menu
from themes import get_theme

router = Router()


@router.message(StateFilter(None))
async def handle_any_message(message: Message, state: FSMContext):
    """
    Ловит любое сообщение когда мастер не в FSM-флоу.
    Регистрируется последним роутером — не перебивает другие хендлеры.
    """
    await get_or_create_master(message.from_user.id, message.from_user.full_name)
    theme_key = await get_master_theme(message.from_user.id)
    t = get_theme(theme_key)
    await message.answer(t["welcome"], reply_markup=main_menu(), parse_mode="Markdown")
