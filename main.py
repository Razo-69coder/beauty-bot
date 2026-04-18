import asyncio
import hmac
import hashlib
import json
from contextlib import asynccontextmanager
from urllib.parse import unquote

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
)
from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats


# ── Dispatcher ────────────────────────────────────────────────────
def build_dispatcher() -> Dispatcher:
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(start.router)
    dp.include_router(clients.router)
    dp.include_router(appointments.router)
    dp.include_router(settings.router)
    dp.include_router(stats.router)
    return dp


bot = Bot(token=BOT_TOKEN)
dp = build_dispatcher()


# ── Lifespan ──────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    setup_scheduler(bot)
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


# ── Telegram Webhook ──────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401)
    update = Update.model_validate(await request.json())
    await dp.feed_update(bot=bot, update=update)
    return {"ok": True}


# ── Auth ──────────────────────────────────────────────────────────
def _parse_init_data(init_data: str) -> dict | None:
    try:
        vals = dict(x.split("=", 1) for x in init_data.split("&"))
        received_hash = vals.pop("hash", None)
        if not received_hash:
            return None
        data_check = "\n".join(f"{k}={v}" for k, v in sorted(vals.items()))
        secret = hmac.new(b"WebAppData", BOT_TOKEN.encode(), hashlib.sha256).digest()
        computed = hmac.new(secret, data_check.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(computed, received_hash):
            return None
        return json.loads(unquote(vals.get("user", "{}")))
    except Exception:
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
        user = _parse_init_data(x_init_data)
        if not user:
            raise HTTPException(status_code=401, detail="Неверный initData")
        telegram_id = user["id"]
        name = user.get("first_name", "Мастер")
    else:
        raise HTTPException(status_code=401, detail="Требуется авторизация")
    return await get_or_create_master(telegram_id, name)


# ── Pydantic models ───────────────────────────────────────────────
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
    price: int = 0
    notes: str = ""

class ReminderUpdate(BaseModel):
    days: int


# ── API: Клиенты ──────────────────────────────────────────────────
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


# ── API: Записи ───────────────────────────────────────────────────
@app.post("/api/appointments", status_code=201)
async def api_add_appointment(body: AppointmentCreate, master_id: int = Depends(get_master_id)):
    appt_id = await add_appointment(
        body.client_id, master_id, body.procedure,
        body.appointment_date, body.price, body.notes,
    )
    return {"id": appt_id}


# ── API: Статистика и прочее ──────────────────────────────────────
@app.get("/api/stats")
async def api_stats(master_id: int = Depends(get_master_id)):
    return await get_statistics(master_id)


@app.get("/api/inactive")
async def api_inactive(master_id: int = Depends(get_master_id), x_init_data: str = Header(None), x_dev_telegram_id: str = Header(None)):
    telegram_id = int(x_dev_telegram_id) if x_dev_telegram_id else None
    if not telegram_id and x_init_data:
        user = _parse_init_data(x_init_data)
        telegram_id = user["id"] if user else None
    days = await get_reminder_days(telegram_id) if telegram_id else 40
    rows = await get_inactive_clients(master_id, days)
    return {"clients": [{"id": r[0], "name": r[1], "phone": r[2], "last_visit": r[3], "days_ago": r[4]} for r in rows]}


@app.put("/api/settings/reminder")
async def api_set_reminder(body: ReminderUpdate, x_init_data: str = Header(None), x_dev_telegram_id: str = Header(None)):
    telegram_id = int(x_dev_telegram_id) if x_dev_telegram_id else None
    if not telegram_id and x_init_data:
        user = _parse_init_data(x_init_data)
        telegram_id = user["id"] if user else None
    if not telegram_id:
        raise HTTPException(status_code=401)
    await update_reminder_days(telegram_id, body.days)
    return {"ok": True}


# ── WebApp static ─────────────────────────────────────────────────
app.mount("/app", StaticFiles(directory="webapp", html=True), name="webapp")
