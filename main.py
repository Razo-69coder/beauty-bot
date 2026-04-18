import asyncio
import hmac
import hashlib
import json
from contextlib import asynccontextmanager
from urllib.parse import unquote

import config
from fastapi import FastAPI, Request, Header, HTTPException, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aiogram import Bot, Dispatcher
from aiogram.types import Update
from aiogram.fsm.storage.memory import MemoryStorage

from config import BOT_TOKEN, WEBHOOK_URL, WEBHOOK_SECRET
from database import (
    init_db, get_or_create_master, get_clients, get_client,
    get_client_history, get_statistics, add_client, update_client,
    delete_client, add_appointment, get_inactive_clients,
    get_reminder_days, update_reminder_days,
    get_reminder_days_by_master, update_reminder_days_by_master,
    get_master_info, get_master_info_by_telegram, get_available_slots,
    get_master_schedule, update_appointment_status,
)

from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats
from handlers import booking, schedule, subscriptions


# ── Dispatcher ────────────────────────────────────────────────────────
def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(start.router)
    dp.include_router(booking.router)
    dp.include_router(schedule.router)
    dp.include_router(subscriptions.router)
    dp.include_router(clients.router)
    dp.include_router(appointments.router)
    dp.include_router(settings.router)
    dp.include_router(stats.router)
    return dp


bot = Bot(token=BOT_TOKEN)
dp = build_dispatcher()


# ── Lifespan ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    setup_scheduler(bot)

    # Сохраняем username бота для ссылок самозаписи
    try:
        me = await bot.get_me()
        config.BOT_USERNAME = me.username
    except Exception:
        config.BOT_USERNAME = ""

    if WEBHOOK_URL:
        await bot.delete_webhook(drop_pending_updates=True)
        await bot.set_webhook(
            url=f"{WEBHOOK_URL}/webhook",
            secret_token=WEBHOOK_SECRET,
        )
        print(f"✅ Beauty Book WebApp запущен")
        print(f"🌐 {WEBHOOK_URL}/app")
    yield
    await bot.session.close()


app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


# ── Telegram Webhook ──────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401)
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot=bot, update=update)
    return {"ok": True}


# ── Auth ──────────────────────────────────────────────────────────────
def _extract_user_from_init_data(init_data: str) -> dict | None:
    try:
        from urllib.parse import parse_qs
        params = parse_qs(init_data, keep_blank_values=True)
        flat = {k: v[0] for k, v in params.items()}
        received_hash = flat.pop("hash", None)

        if received_hash:
            data_check = "\n".join(f"{k}={v}" for k, v in sorted(flat.items()))
            secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
            computed = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
            if not hmac.compare_digest(computed, received_hash):
                import logging
                logging.warning("initData HMAC mismatch")

        user_str = flat.get("user")
        if user_str:
            return json.loads(unquote(user_str))
        return None
    except Exception as e:
        import logging
        logging.error(f"initData parse error: {e}")
        return None


async def get_master_id(
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
    x_dev_username: str = Header(None),
) -> int:
    if x_dev_telegram_id:
        telegram_id = int(x_dev_telegram_id)
        name = x_dev_username or "Мастер"
    elif x_init_data:
        user = _extract_user_from_init_data(x_init_data)
        if not user or not user.get("id"):
            raise HTTPException(status_code=401, detail="Не удалось получить user из initData")
        telegram_id = user["id"]
        name = user.get("first_name", "Мастер")
    else:
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    return await get_or_create_master(telegram_id, name)


# ── Pydantic models ───────────────────────────────────────────────────
class ClientCreate(BaseModel):
    name: str
    phone: str
    notes: str = ""

class ClientUpdate(BaseModel):
    name: str
    phone: str
    notes: str = ""

class AppointmentCreate(BaseModel):
    client_id: int
    procedure: str
    appointment_date: str
    time: str = ""
    price: int = 0
    notes: str = ""

class ReminderUpdate(BaseModel):
    days: int

class StatusUpdate(BaseModel):
    status: str


# ── API: Клиенты ──────────────────────────────────────────────────────
@app.get("/api/clients")
async def api_clients(master_id: int = Depends(get_master_id)):
    rows = await get_clients(master_id)
    return [{"id": r[0], "name": r[1], "phone": r[2], "notes": r[3], "last_visit": r[4]} for r in rows]


@app.post("/api/clients", status_code=201)
async def api_add_client(body: ClientCreate, master_id: int = Depends(get_master_id)):
    client_id = await add_client(master_id, body.name, body.phone, body.notes)
    return {"id": client_id}


@app.get("/api/clients/{client_id}")
async def api_client(client_id: int, master_id: int = Depends(get_master_id)):
    client = await get_client(client_id)
    if not client:
        raise HTTPException(status_code=404)
    history = await get_client_history(client_id)
    hist = [{"procedure": h[0], "date": h[1], "price": h[2], "notes": h[3]} for h in history]
    return {**client, "history": hist}


@app.put("/api/clients/{client_id}")
async def api_update_client(client_id: int, body: ClientUpdate, master_id: int = Depends(get_master_id)):
    ok = await update_client(client_id, master_id, body.name, body.phone, body.notes)
    if not ok:
        raise HTTPException(status_code=404)
    return {"ok": True}


@app.delete("/api/clients/{client_id}")
async def api_delete_client(client_id: int, master_id: int = Depends(get_master_id)):
    ok = await delete_client(client_id, master_id)
    if not ok:
        raise HTTPException(status_code=404)
    return {"ok": True}


# ── API: Записи ───────────────────────────────────────────────────────
@app.post("/api/appointments", status_code=201)
async def api_add_appointment(body: AppointmentCreate, master_id: int = Depends(get_master_id)):
    appt_id = await add_appointment(
        body.client_id, master_id, body.procedure,
        body.appointment_date, body.price, body.notes,
        time=body.time,
    )
    return {"id": appt_id}


@app.patch("/api/appointments/{appt_id}/status")
async def api_update_status(appt_id: int, body: StatusUpdate, master_id: int = Depends(get_master_id)):
    if body.status not in ("confirmed", "cancelled", "pending"):
        raise HTTPException(status_code=400, detail="Неверный статус")
    await update_appointment_status(appt_id, body.status)
    return {"ok": True}


# ── API: Расписание и слоты ───────────────────────────────────────────
@app.get("/api/schedule")
async def api_schedule(date: str, master_id: int = Depends(get_master_id)):
    rows = await get_master_schedule(master_id, date)
    return [
        {"id": r[0], "client_name": r[1], "procedure": r[2], "time": r[3], "status": r[4], "phone": r[5]}
        for r in rows
    ]


@app.get("/api/slots")
async def api_slots(date: str, master_id: int = Depends(get_master_id)):
    master = await get_master_info(master_id)
    if not master:
        raise HTTPException(status_code=404)
    slots = await get_available_slots(
        master_id, date,
        master["work_start"], master["work_end"], master["slot_duration"]
    )
    return {"slots": slots, "work_start": master["work_start"], "work_end": master["work_end"]}


# ── API: Статистика и прочее ──────────────────────────────────────────
@app.get("/api/stats")
async def api_stats(master_id: int = Depends(get_master_id)):
    return await get_statistics(master_id)


@app.get("/api/inactive")
async def api_inactive(master_id: int = Depends(get_master_id)):
    days = await get_reminder_days_by_master(master_id)
    rows = await get_inactive_clients(master_id, days)
    return {"clients": [{"id": r[0], "name": r[1], "phone": r[2], "last_visit": r[3], "days_ago": r[4]} for r in rows]}


@app.put("/api/settings/reminder")
async def api_set_reminder(body: ReminderUpdate, master_id: int = Depends(get_master_id)):
    await update_reminder_days_by_master(master_id, body.days)
    return {"ok": True}


# ── Public API (для клиентов, без авторизации) ────────────────────────

class PublicBooking(BaseModel):
    master_telegram_id: int
    date: str
    time: str
    client_name: str
    client_phone: str


@app.get("/api/public/master/{telegram_id}")
async def api_public_master(telegram_id: int):
    from datetime import datetime as dt, timedelta
    master = await get_master_info_by_telegram(telegram_id)
    if not master:
        raise HTTPException(status_code=404, detail="Мастер не найден")
    today = dt.now().date()
    available_dates = []
    for i in range(1, 14):
        d = today + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        slots = await get_available_slots(
            master["id"], date_str,
            master["work_start"], master["work_end"], master["slot_duration"]
        )
        if slots:
            available_dates.append(date_str)
        if len(available_dates) == 7:
            break
    return {"name": master["name"], "master_id": master["id"], "available_dates": available_dates}


@app.get("/api/public/slots")
async def api_public_slots(master: int, date: str):
    master_info = await get_master_info_by_telegram(master)
    if not master_info:
        raise HTTPException(status_code=404)
    slots = await get_available_slots(
        master_info["id"], date,
        master_info["work_start"], master_info["work_end"], master_info["slot_duration"]
    )
    return {"slots": slots}


@app.post("/api/public/book", status_code=201)
async def api_public_book(body: PublicBooking):
    from datetime import datetime as dt
    DAYS_RU = {0: "Пн", 1: "Вт", 2: "Ср", 3: "Чт", 4: "Пт", 5: "Сб", 6: "Вс"}
    master = await get_master_info_by_telegram(body.master_telegram_id)
    if not master:
        raise HTTPException(status_code=404, detail="Мастер не найден")
    slots = await get_available_slots(
        master["id"], body.date,
        master["work_start"], master["work_end"], master["slot_duration"]
    )
    if body.time not in slots:
        raise HTTPException(status_code=409, detail="Этот слот уже занят")
    client_id = await add_client(master["id"], body.client_name, body.client_phone)
    appt_id = await add_appointment(
        client_id=client_id,
        master_id=master["id"],
        procedure="Запись",
        appointment_date=body.date,
        time=body.time,
        status="pending",
    )
    date_fmt = dt.strptime(body.date, "%Y-%m-%d").strftime("%d.%m.%Y")
    day_name = DAYS_RU[dt.strptime(body.date, "%Y-%m-%d").weekday()]
    try:
        from keyboards import booking_confirm_keyboard
        await bot.send_message(
            body.master_telegram_id,
            f"🔔 *Новая запись через сайт!*\n\n"
            f"👤 {body.client_name}\n"
            f"📱 {body.client_phone}\n"
            f"📅 {date_fmt} ({day_name}), {body.time}\n\n"
            f"Подтвердить?",
            reply_markup=booking_confirm_keyboard(appt_id),
            parse_mode="Markdown",
        )
    except Exception:
        pass
    return {"id": appt_id, "ok": True}


# ── WebApp static ─────────────────────────────────────────────────────
app.mount("/app", StaticFiles(directory="webapp", html=True), name="webapp")
