from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import get_or_create_master, get_services, add_service, delete_service
from keyboards import services_keyboard, cancel_keyboard, back_to_menu

router = Router()


class AddServiceForm(StatesGroup):
    name = State()
    price = State()


@router.callback_query(F.data == "services_list")
async def cb_services_list(callback: CallbackQuery):
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    services = await get_services(master_id)

    if services:
        text = "💅 *Мои услуги*\n\nНажми 🗑 чтобы удалить услугу:"
    else:
        text = (
            "💅 *Мои услуги*\n\n"
            "Список пуст. Добавь услуги, и при записи клиента "
            "они будут появляться кнопками для быстрого выбора."
        )

    await callback.message.edit_text(
        text, reply_markup=services_keyboard(services), parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data == "svc_add")
async def cb_svc_add(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AddServiceForm.name)
    await callback.message.edit_text(
        "💅 *Новая услуга*\n\nВведи название услуги:\n\n"
        "_Например: Маникюр, Педикюр, Брови, Ресницы, Визаж_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(AddServiceForm.name)
async def process_service_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(AddServiceForm.price)
    await message.answer(
        f"✅ Услуга: *{message.text.strip()}*\n\n"
        f"💰 Введи стандартную цену (₽):\n\n"
        f"_Или напиши *-* чтобы не указывать цену_",
        reply_markup=cancel_keyboard(),
        parse_mode="Markdown"
    )


@router.message(AddServiceForm.price)
async def process_service_price(message: Message, state: FSMContext):
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

    data = await state.get_data()
    await state.clear()

    master_id = await get_or_create_master(message.from_user.id, message.from_user.full_name)
    await add_service(master_id, data["name"], price)

    services = await get_services(master_id)
    price_str = f" — {price}₽" if price else ""
    await message.answer(
        f"✅ Услуга *{data['name']}{price_str}* добавлена!\n\n"
        f"Теперь при записи клиента она появится в списке для быстрого выбора.",
        reply_markup=services_keyboard(services),
        parse_mode="Markdown"
    )


@router.callback_query(F.data.startswith("svc_delete:"))
async def cb_svc_delete(callback: CallbackQuery):
    svc_id = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    await delete_service(svc_id, master_id)

    services = await get_services(master_id)
    text = "💅 *Мои услуги*\n\nУслуга удалена."
    if not services:
        text += "\n\nСписок пуст."

    await callback.message.edit_text(
        text, reply_markup=services_keyboard(services), parse_mode="Markdown"
    )
    await callback.answer("Удалено")
