from aiogram import Router, F
from aiogram.types import Message
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup

from database import get_pool

router = Router()


# ─── FSM для ожидания номера телефона ────────────────────────────────

class PhoneState(StatesGroup):
    waiting_for_phone = State()


# ─── Обработка команды /start ───────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    """Обработка команды /start для клиентов"""
    await state.clear()
    await state.set_state(PhoneState.waiting_for_phone)

    await message.answer(
        "👋 Привет! Я помогу вам не пропустить запись к мастеру.\n\n"
        "После привязки вы будете получать:\n"
        "🔔 Напоминание за 24 часа и за 2 часа до визита\n"
        "🎂 Поздравление и подарок в день рождения\n"
        "🏆 Уведомление о накопленной скидке за постоянство\n\n"
        "Отправьте ваш номер телефона в формате +7XXXXXXXXXX:"
    )


# ─── Обработка номера телефона ───────────────────────────────────────────

@router.message(PhoneState.waiting_for_phone)
async def process_phone(message: Message, state: FSMContext):
    """Обработка номера телефона от клиента"""
    phone = message.text.strip()
    
    # Простая валидация формата +7XXXXXXXXXX
    if not (phone.startswith('+7') and len(phone) == 12 and phone[2:].isdigit()):
        await message.answer(
            "❌ Неверный формат номера. Пожалуйста, введите номер в формате +7XXXXXXXXXX\n"
            "Пример: +79123456789"
        )
        return
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Поиск клиента по номеру телефона
        client = await conn.fetchrow(
            "SELECT id, name, master_id FROM clients WHERE phone = $1 LIMIT 1",
            phone
        )
        
        if client:
            # Клиент найден - привязываем telegram_id
            await conn.execute(
                "UPDATE clients SET telegram_id = $1 WHERE phone = $2",
                message.from_user.id, phone
            )
            
            await state.clear()
            await message.answer(
                f"✅ Отлично, {client['name']}! Теперь вы будете получать напоминания о записях. До встречи! 💅"
            )
        else:
            # Клиент не найден
            await message.answer(
                "❌ Номер не найден. Убедитесь что записывались через ссылку мастера, или попросите мастера добавить вас вручную."
            )
            await state.clear()


# ─── Обработка команды /stop ───────────────────────────────────────────

@router.message(Command("stop"))
async def cmd_stop(message: Message, state: FSMContext):
    """Отключение уведомлений"""
    await state.clear()
    
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Удаляем telegram_id у клиента
        result = await conn.execute(
            "UPDATE clients SET telegram_id = NULL WHERE telegram_id = $1",
            message.from_user.id
        )
    
    await message.answer(
        "Уведомления отключены. Напишите /start чтобы включить снова."
    )