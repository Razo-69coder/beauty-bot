from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import (
    get_or_create_master, get_client, get_client_subscriptions,
    add_subscription, use_subscription_session,
)
from keyboards import subscriptions_keyboard, back_to_client

router = Router()


class SubForm(StatesGroup):
    name = State()
    total = State()
    price = State()


@router.callback_query(F.data.startswith("sub_menu:"))
async def cb_sub_menu(callback: CallbackQuery):
    client_id = int(callback.data.split(":")[1])
    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    client = await get_client(client_id)
    subs = await get_client_subscriptions(client_id, master_id)

    if not subs:
        text = f"📦 *Абонементы клиента {client['name']}*\n\n_Абонементов нет_"
    else:
        text = f"📦 *Абонементы клиента {client['name']}*\n\n"
        for sub_id, name, total, used, price in subs:
            remaining = total - used
            bar = "🟢" * remaining + "⚪" * used
            status = "✅ Активен" if remaining > 0 else "🏁 Исчерпан"
            text += f"*{name}*\n{bar} {remaining}/{total} сеансов · {price}₽ · {status}\n\n"

    await callback.message.edit_text(
        text,
        reply_markup=subscriptions_keyboard(subs, client_id),
        parse_mode="Markdown"
    )
    await callback.answer()


@router.callback_query(F.data.startswith("sub_create:"))
async def cb_sub_create(callback: CallbackQuery, state: FSMContext):
    client_id = int(callback.data.split(":")[1])
    await state.update_data(client_id=client_id)
    await state.set_state(SubForm.name)
    await callback.message.edit_text(
        "📦 *Новый абонемент*\n\nВведите название (например: «5 маникюров»):",
        parse_mode="Markdown"
    )
    await callback.answer()


@router.message(SubForm.name)
async def sub_got_name(message: Message, state: FSMContext):
    await state.update_data(name=message.text.strip())
    await state.set_state(SubForm.total)
    await message.answer("Сколько сеансов включает абонемент? (введите число)")


@router.message(SubForm.total)
async def sub_got_total(message: Message, state: FSMContext):
    if not message.text.strip().isdigit() or int(message.text.strip()) < 1:
        await message.answer("Введите число больше 0:")
        return
    await state.update_data(total=int(message.text.strip()))
    await state.set_state(SubForm.price)
    await message.answer("Стоимость абонемента (₽)? Введите 0 если бесплатно:")


@router.message(SubForm.price)
async def sub_got_price(message: Message, state: FSMContext):
    if not message.text.strip().isdigit():
        await message.answer("Введите число:")
        return
    data = await state.get_data()
    await state.clear()

    master_id = await get_or_create_master(message.from_user.id, message.from_user.full_name)
    await add_subscription(
        master_id=master_id,
        client_id=data["client_id"],
        name=data["name"],
        total=data["total"],
        price=int(message.text.strip()),
    )

    client = await get_client(data["client_id"])
    subs = await get_client_subscriptions(data["client_id"], master_id)
    await message.answer(
        f"✅ Абонемент *{data['name']}* создан!\n\n"
        f"Сеансов: {data['total']} · Стоимость: {message.text.strip()}₽",
        reply_markup=subscriptions_keyboard(subs, data["client_id"]),
        parse_mode="Markdown"
    )


@router.callback_query(F.data.startswith("sub_use:"))
async def cb_sub_use(callback: CallbackQuery):
    parts = callback.data.split(":")
    sub_id = int(parts[1])
    client_id = int(parts[2])

    master_id = await get_or_create_master(callback.from_user.id, callback.from_user.full_name)
    success = await use_subscription_session(sub_id, master_id)

    if not success:
        await callback.answer("Сеансы в абонементе исчерпаны!", show_alert=True)
        return

    subs = await get_client_subscriptions(client_id, master_id)
    client = await get_client(client_id)

    # Проверяем остаток — если остался 1 сеанс, предупреждаем
    for s in subs:
        if s[0] == sub_id:
            remaining = s[2] - s[3]
            if remaining == 1:
                await callback.answer(f"Сеанс списан. Остался 1 сеанс!", show_alert=True)
            elif remaining == 0:
                await callback.answer("Сеанс списан. Абонемент исчерпан!", show_alert=True)
            else:
                await callback.answer(f"Сеанс списан. Осталось: {remaining}")
            break

    text = f"📦 *Абонементы клиента {client['name']}*\n\n"
    for sub_id2, name, total, used, price in subs:
        remaining = total - used
        bar = "🟢" * remaining + "⚪" * used
        status = "✅ Активен" if remaining > 0 else "🏁 Исчерпан"
        text += f"*{name}*\n{bar} {remaining}/{total} сеансов · {price}₽ · {status}\n\n"

    await callback.message.edit_text(
        text,
        reply_markup=subscriptions_keyboard(subs, client_id),
        parse_mode="Markdown"
    )
