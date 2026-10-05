import asyncio
import bcrypt
import hmac
import hashlib
import json
import os
import secrets
from typing import Optional
from contextlib import asynccontextmanager
from urllib.parse import unquote

import config
from fastapi import FastAPI, Request, Header, HTTPException, Depends, Form, UploadFile, File
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from aiogram import Bot, Dispatcher
from aiogram.types import Update

ADMIN_TG_ID = 550421233  # Telegram ID администратора
ADMIN_SECRET = os.getenv("ADMIN_PASSWORD", "")
from aiogram.fsm.storage.memory import MemoryStorage

APNS_KEY_ID = os.getenv("APNS_KEY_ID", "")
APNS_TEAM_ID = os.getenv("APNS_TEAM_ID", "")
APNS_PRIVATE_KEY = os.getenv("APNS_PRIVATE_KEY", "").replace("\\n", "\n")
APNS_BUNDLE_ID = os.getenv("APNS_BUNDLE_ID", "")
APNS_USE_SANDBOX = os.getenv("APNS_USE_SANDBOX", "true").lower() == "true"

from config import BOT_TOKEN, WEBHOOK_URL, WEBHOOK_SECRET
import jwt
import httpx
from datetime import datetime as _dt, timedelta as _td
import waitlist
import wallet
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.util import get_remote_address
from slowapi.errors import RateLimitExceeded

from database import (
    init_db, get_or_create_master, get_clients, get_client,
    get_client_history, get_statistics, add_client, update_client, update_client_username,
    delete_client, add_appointment, get_inactive_clients,
    get_reminder_days, update_reminder_days,
    get_reminder_days_by_master, update_reminder_days_by_master,
    get_master_info, get_master_info_by_telegram, get_available_slots,
    get_master_schedule, update_appointment_status,
    create_login_code, verify_login_code, verify_login_code_by_code,
    get_master_full, update_master_full_settings, update_master_loyalty_settings, update_master_payment, update_master_timezone,
    search_clients, merge_duplicate_clients,
    get_services, add_service, delete_service, update_service,
    get_earnings_by_service, get_earnings_by_client, get_earnings_by_day, get_earnings_by_period,
    get_appointment_with_client, update_appointment_service_done,
    get_master_by_booking_link, update_booking_link, get_master_booking_link, is_booking_linkTaken,
    get_master_by_email, create_master_with_email,
    get_expenses, add_expense, delete_expense,
    get_blocked_days, add_blocked_day, remove_blocked_day,
    get_pool,
    get_custom_slots_for_date, get_custom_slots_available,
    get_custom_slots_for_month, add_custom_slot, remove_custom_slot,
    get_reminder_templates_v1, upsert_reminder_template,
    save_device_token,
    get_device_tokens_for_master,
    create_notification, get_notifications, get_unread_count,
    mark_notification_read, mark_all_notifications_read, broadcast_notification,
    get_personal_notes, create_personal_note, delete_personal_note,
    save_password_reset_code, verify_password_reset_code,
)

from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats, services
from handlers import booking, schedule, subscriptions, templates, reviews, deposit, fallback


import random
import time as _time

async def send_push_detailed(device_token: str, title: str, body_text: str) -> dict:
    if not all([APNS_KEY_ID, APNS_TEAM_ID, APNS_PRIVATE_KEY, APNS_BUNDLE_ID]):
        return {"ok": False, "error": "missing_env_vars", "key_id": bool(APNS_KEY_ID), "team_id": bool(APNS_TEAM_ID), "private_key": bool(APNS_PRIVATE_KEY), "bundle_id": bool(APNS_BUNDLE_ID)}
    try:
        import jwt as _jwt
        jwt_token = _jwt.encode(
            {"iss": APNS_TEAM_ID, "iat": int(_time.time())},
            APNS_PRIVATE_KEY,
            algorithm="ES256",
            headers={"kid": APNS_KEY_ID}
        )
        host = "api.sandbox.push.apple.com" if APNS_USE_SANDBOX else "api.push.apple.com"
        url = f"https://{host}/3/device/{device_token}"
        headers = {
            "authorization": f"bearer {jwt_token}",
            "apns-topic": APNS_BUNDLE_ID,
            "apns-push-type": "alert",
            "apns-priority": "10",
        }
        payload = {"aps": {"alert": {"title": title, "body": body_text}, "sound": "default"}}
        async with httpx.AsyncClient(http2=True) as client:
            resp = await client.post(url, json=payload, headers=headers, timeout=10)
            body = resp.text
            print(f"[APNs] status={resp.status_code} body={body!r} token={device_token[:16]}...")
            return {"ok": resp.status_code == 200, "status": resp.status_code, "apple_response": body, "host": host, "bundle_id": APNS_BUNDLE_ID, "key_id": APNS_KEY_ID}
    except Exception as e:
        print(f"[APNs] error: {e}")
        return {"ok": False, "error": str(e)}


async def send_push(device_token: str, title: str, body_text: str) -> bool:
    result = await send_push_detailed(device_token, title, body_text)
    return result.get("ok", False)


async def push_to_master(master_id: int, title: str, body_text: str):
    tokens = await get_device_tokens_for_master(master_id)
    for token in tokens:
        await send_push(token, title, body_text)


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
    dp.include_router(services.router)
    dp.include_router(templates.router)
    dp.include_router(reviews.router)
    dp.include_router(deposit.router)
    from handlers import client_reminder
    dp.include_router(client_reminder.router)
    from handlers import waitlist as waitlist_handlers
    dp.include_router(waitlist_handlers.router)
    dp.include_router(fallback.router)  # всегда последним
    return dp


bot = Bot(token=BOT_TOKEN)
dp = build_dispatcher()

# ── Lifespan ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        await init_db()
        from database import get_pool
        pool = await get_pool()
        async with pool.acquire() as conn:
            for sql in [
                "ALTER TABLE clients ADD COLUMN IF NOT EXISTS username VARCHAR(50) DEFAULT ''",
                "ALTER TABLE masters ADD COLUMN IF NOT EXISTS timezone VARCHAR(50) DEFAULT 'Europe/Moscow'",
                "ALTER TABLE clients ADD COLUMN IF NOT EXISTS timezone VARCHAR(50) DEFAULT 'Europe/Moscow'",
                "ALTER TABLE masters ALTER COLUMN telegram_id DROP NOT NULL",
                "ALTER TABLE masters ADD COLUMN IF NOT EXISTS trial_end_date TIMESTAMP",
                "ALTER TABLE masters ADD COLUMN IF NOT EXISTS early_price BOOLEAN DEFAULT FALSE",
                "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS client_confirmed_at TIMESTAMP",
                "ALTER TABLE services ADD COLUMN IF NOT EXISTS correction_days INTEGER",
                "ALTER TABLE clients ADD COLUMN IF NOT EXISTS marketing_consent BOOLEAN",
                "ALTER TABLE clients ADD COLUMN IF NOT EXISTS marketing_asked_at TIMESTAMP",
                "CREATE TABLE IF NOT EXISTS broadcasts (id SERIAL PRIMARY KEY, master_id INTEGER, created_at TIMESTAMP DEFAULT NOW(), text TEXT, total INTEGER DEFAULT 0, delivered INTEGER DEFAULT 0)",
                # Лист ожидания клиенток (волна 4)
                """CREATE TABLE IF NOT EXISTS client_waitlist (
                    id SERIAL PRIMARY KEY, master_id INTEGER, client_id INTEGER, date TEXT,
                    service_id INTEGER, procedure TEXT, duration_min INTEGER DEFAULT 0, price INTEGER DEFAULT 0,
                    pref TEXT DEFAULT 'any', status TEXT DEFAULT 'waiting', offered_time TEXT, offered_at TIMESTAMP,
                    offer_chat_id BIGINT, offer_msg_id BIGINT, appointment_id INTEGER,
                    created_at TIMESTAMP DEFAULT NOW())""",
                "CREATE INDEX IF NOT EXISTS idx_client_waitlist_md ON client_waitlist (master_id, date, status)",
                *wallet.MIGRATIONS,
                "CREATE TABLE IF NOT EXISTS client_merge_dismissed (master_id INTEGER, a_id INTEGER, b_id INTEGER, created_at TIMESTAMP DEFAULT NOW(), PRIMARY KEY (master_id, a_id, b_id))",
                "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS auto_completed BOOLEAN DEFAULT FALSE",
            ]:
                try:
                    await conn.execute(sql)
                except Exception:
                    pass
    except Exception as e:
        print(f"[STARTUP] DB warning: {e}")
    
    os.environ.setdefault("YUKASSA_SHOP_ID", "")
    os.environ.setdefault("YUKASSA_SECRET_KEY", "")
    
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
app.include_router(wallet.router)  # Apple Wallet: скачивание карты и веб-сервис PassKit
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

limiter = Limiter(key_func=get_remote_address)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)


# ── Health checks (для keep-alive) ─────────────────────────────────────
@app.get("/health")
async def health():
    return {"status": "ok", "message": "Beauty Book v5", "v": 5}

@app.get("/", response_class=HTMLResponse)
async def root():
    with open("webapp/index.html", "r", encoding="utf-8") as f:
        return f.read()

@app.get("/api/all-masters")
async def get_all_v6():
    """Публичный список мастеров"""
    from database import get_all_masters as gam
    masters = await gam()
    return {"masters": masters}


@app.get("/api/admin/master/{master_id}/appointment/{appt_id}")
async def admin_get_appointment(master_id: int, appt_id: int):
    from database import get_appointment_with_client
    appt = await get_appointment_with_client(appt_id)
    if not appt or appt["master_id"] != master_id:
        from fastapi import HTTPException
        raise HTTPException(404, "Запись не найдена")
    return {
        "id": appt["id"],
        "client": appt.get("client_name"),
        "phone": appt.get("client_telegram_id") or "",
        "procedure": appt.get("procedure"),
        "date": str(appt.get("appointment_date")) if appt.get("appointment_date") else "",
        "time": appt.get("time") or "",
        "price": appt.get("deposit_amount") or 0,
        "status": appt.get("status") or "confirmed",
        "notes": appt.get("notes") or "",
        "service_done_at": appt.get("service_done_at")
    }


@app.get("/health")
async def health():
    return {"status": "healthy"}





# ── Telegram Webhook ──────────────────────────────────────────────────
@app.post("/webhook")
async def webhook(request: Request):
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != WEBHOOK_SECRET:
        raise HTTPException(status_code=401)
    try:
        update = Update.model_validate(await request.json())
        await dp.feed_update(bot=bot, update=update)
    except Exception as e:
        import traceback
        print(f"[WEBHOOK ERROR] {e}\n{traceback.format_exc()}")
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


# ── JWT утилиты (определены здесь — до всех JWT-эндпоинтов) ──────────

def _jwt_secret() -> str:
    return config.JWT_SECRET or BOT_TOKEN or "beauty_fallback_secret"

def _create_jwt(telegram_id: int, master_id: int) -> str:
    return jwt.encode(
        {"tg": telegram_id, "mid": master_id, "exp": _dt.utcnow() + _td(days=30)},
        _jwt_secret(), algorithm="HS256"
    )

def _decode_jwt(token: str) -> dict | None:
    try:
        return jwt.decode(token, _jwt_secret(), algorithms=["HS256"])
    except jwt.InvalidTokenError:
        return None

def _generate_jwt(master_id: int) -> str:
    return jwt.encode(
        {"mid": master_id, "exp": _dt.utcnow() + _td(days=30)},
        _jwt_secret(), algorithm="HS256"
    )

async def get_jwt_master_id(
    authorization: str = Header(None),
    master_id: int = None
) -> int:
    """Получает master_id из токена или из URL-параметра (для админа)"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = _decode_jwt(authorization[7:])
    if not payload:
        raise HTTPException(401, "Неверный или устаревший токен")
    
    # Если передан master_id в URL и это админ — используем его
    if master_id:
        admin_tg = int(payload.get("tg"))
        if admin_tg == ADMIN_TG_ID:
            return master_id
    
    master_id = int(payload["mid"])

    # Ручная блокировка доступа (is_active) — выставляется только вручную из админки,
    # не автоматикой по истечении триала. Универсальное сообщение, без упоминания оплаты.
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_active FROM masters WHERE id=$1", master_id)
        if row and row['is_active'] == 0:
            raise HTTPException(status_code=403, detail="Доступ запрещён")
    
    return master_id


async def get_jwt_master_id_any(authorization: str = Header(None)) -> int:
    """Как get_jwt_master_id, но без проверки is_active — для subscription/notify"""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = _decode_jwt(authorization[7:])
    if not payload:
        raise HTTPException(401, "Неверный или устаревший токен")
    return int(payload["mid"])


# ── Pydantic models ───────────────────────────────────────────────────
class ClientCreate(BaseModel):
    name: str
    phone: str
    notes: str = ""


class EmailRegisterRequest(BaseModel):
    email: str
    password: str
    name: str
    phone: Optional[str] = None


class EmailLoginRequest(BaseModel):
    email: str
    password: str


class BookingLinkUpdateRequest(BaseModel):
    link: str


class PublicBookingRequest(BaseModel):
    client_name: str
    client_phone: str
    service_id: int | None = None
    procedure: str = ""
    date: str
    time: str
    birthday: str = ""
    price: int = 0
    duration: int = 0
    marketing_consent: bool = False  # необязательная галочка «Получать акции и свободные окошки»


class LoyaltySettingsRequest(BaseModel):
    loyalty_enabled: bool = False
    loyalty_threshold: int = 10
    loyalty_discount_percent: int = 10
    birthday_enabled: bool = False
    birthday_discount_percent: int = 10
    loyalty_discount_type: str = "percent"
    loyalty_discount_rub: int = 0


class FeedbackBody(BaseModel):
    text: str


class ClientUpdate(BaseModel):
    name: str | None = None
    phone: str | None = None
    notes: str | None = None
    username: str | None = None
    birthday: str | None = None
    source: str | None = None
    allergies: str | None = None

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
    for i in range(0, 31):  # 0 = сегодня, 30 = через 30 дней
        d = today + timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        slots = await get_available_slots(
            master["id"], date_str,
            master["work_start"], master["work_end"], master["slot_duration"]
        )
        if slots:
            available_dates.append(date_str)
        if len(available_dates) == 30:
            break
    return {"name": master["name"], "master_id": master["id"], "available_dates": available_dates}


@app.post("/api/debug/test-notification")
async def debug_test_notification(telegram_id: int, type: str = "2h", secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "Forbidden")
    messages = {
        "2h": "⏰ *Через 2 часа ваша запись!*\n\n📅 Тест-дата в *Тест-время*\n📋 Тест-процедура\n\nНе забудьте!",
        "24h": "🔔 *Напоминание о записи*\n\nЗавтра, *Тест-дата* в *Тест-время*\n📋 Тест-процедура\n\nЖдём вас!",
        "review": "💅 *Тест-клиент, как прошёл визит?*\n\nОцените процедуру «Тест-процедура»:",
        "correction": "💅 *Привет!*\n\nПрошло 3 недели после визита — самое время на коррекцию!\n\nЗапишитесь к мастеру заранее 🗓",
        "birthday": "🎂 *С днём рождения!*\n\nМастер поздравляет вас с праздником! 🎉\n\nЖдём вас на любимой процедуре 💅",
        "loyalty": "🏆 Вы у нас уже 10 раз!\n\nВы заработали скидку на следующий визит 🎉\n\nЗапишитесь и скажите мастеру что вы постоянный клиент 💅",
        "booking": "🔔 *Новая запись через ссылку!*\n\n👤 Тест-клиент\n📱 +7 (999) 000-00-00\n📅 Тест-дата в Тест-время\n💅 Тест-процедура",
    }
    text = messages.get(type, f"🔔 Тест-уведомление типа: {type}")
    await bot.send_message(telegram_id, text, parse_mode="Markdown")
    return {"ok": True, "sent_to": telegram_id, "type": type}


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

async def _send_tg(chat_id: int, text: str):
    if not BOT_TOKEN:
        return
    async with httpx.AsyncClient(timeout=5) as c:
        try:
            await c.post(
                f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage",
                json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            )
        except Exception:
            pass


async def send_telegram(text: str):
    """Отправка сообщения администратору (ADMIN_TG_ID)"""
    await _send_tg(ADMIN_TG_ID, text)


# ── Авторизация веб-панели ────────────────────────────────────────────

class _RequestCode(BaseModel):
    telegram_id: int

class _VerifyCode(BaseModel):
    telegram_id: int
    code: str

class _VerifyCodeOnly(BaseModel):
    code: str

class _MasterSettings(BaseModel):
    name: str
    work_start: int = 10
    work_end: int = 20
    slot_duration: int = 60
    reminder_days: int = 40

class _PaymentUpdate(BaseModel):
    payment_card: str = ""
    payment_phone: str = ""
    payment_banks: str = ""


@app.post("/api/auth/request-code")
async def auth_request_code(body: _RequestCode):
    m = await get_master_info_by_telegram(body.telegram_id)
    if not m:
        raise HTTPException(404, "Telegram ID не зарегистрирован. Сначала запустите бота командой /start")
    code = await create_login_code(body.telegram_id)
    await _send_tg(body.telegram_id, f"🔑 *Код входа в Beauty Book*\n\n`{code}`\n\nДействует 10 минут.")
    return {"ok": True}


@app.post("/api/auth/verify")
async def auth_verify(body: _VerifyCode):
    ok = await verify_login_code(body.telegram_id, body.code)
    if not ok:
        raise HTTPException(400, "Неверный или устаревший код")
    m = await get_master_info_by_telegram(body.telegram_id)
    full = await get_master_full(m["id"])
    token = _create_jwt(body.telegram_id, m["id"])
    return {"token": token, "master": full}


@app.post("/api/auth/verify-code")
async def auth_verify_code(body: _VerifyCodeOnly):
    """Новый вход: только код, без ввода Telegram ID."""
    tg_id = await verify_login_code_by_code(body.code)
    if not tg_id:
        raise HTTPException(400, "Неверный или устаревший код")
    m = await get_master_info_by_telegram(tg_id)
    if not m:
        raise HTTPException(404, "Мастер не найден")
    full = await get_master_full(m["id"])
    token = _create_jwt(tg_id, m["id"])
    return {"token": token, "master": full}


# ── API: Email/Password авторизация ───────────────────────────────

@app.post("/api/v1/auth/register")
@limiter.limit("5/minute")
async def register(request: Request, body: EmailRegisterRequest):
    try:
        password_hash = bcrypt.hashpw(body.password.encode(), bcrypt.gensalt()).decode()

        existing = await get_master_by_email(body.email)
        if existing:
            raise HTTPException(400, "Email уже занят")

        master_id = await create_master_with_email(body.email, password_hash, body.name, body.phone)
        master = await get_master_by_email(body.email)
        if not master:
            raise HTTPException(500, "Ошибка создания мастера")

        token = _generate_jwt(master_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(500, f"DB error: {e}")
    
    from database import get_master_trial_status
    trial = await get_master_trial_status(master["id"])
    
    return {"token": token, "trial": trial, "master": {
        "id": master["id"], "name": master["name"], "email": master["email"],
        "phone": master.get("phone") or "",
        "work_start": master["work_start"], "work_end": master["work_end"],
        "slot_duration": master["slot_duration"], "timezone": master["timezone"],
        "reminder_days": master["reminder_days"], "payment_card": master["payment_card"],
        "payment_phone": master["payment_phone"], "payment_banks": master["payment_banks"],
        "deposit_enabled": False, "deposit_percent": 30,
        "theme": master["theme"],
    }}


@app.post("/api/v1/auth/login")
@limiter.limit("5/minute")
async def login(request: Request, body: EmailLoginRequest):
    def _pw_ok(m) -> bool:
        stored = m.get("password_hash") or ""
        if not stored:
            return False
        try:
            return bcrypt.checkpw(body.password.encode(), stored.encode())
        except ValueError:
            return stored == hashlib.sha256(body.password.encode()).hexdigest()

    ident = (body.email or "").strip()
    master = None
    ok = False
    if "@" in ident:
        master = await get_master_by_email(ident)
        # Быстрый путь для demo-аккаунта (Apple Review)
        if master and ident.lower() == "test@solvobeauty.com" and body.password == "TestSolvo123!":
            ok = True
        elif master:
            ok = _pw_ok(master)
    else:
        # Вход по номеру телефона: ищем аккаунты с этим номером и проверяем пароль
        digits = "".join(ch for ch in ident if ch.isdigit())
        if len(digits) == 11 and digits[0] == "8":
            digits = "7" + digits[1:]
        if len(digits) >= 10:
            pool = await get_pool()
            async with pool.acquire() as conn:
                emails = [r["email"] for r in await conn.fetch(
                    """SELECT email FROM masters WHERE email IS NOT NULL
                       AND right(regexp_replace(COALESCE(phone,''),'[^0-9]','','g'), 10) = $1
                       ORDER BY id""", digits[-10:])]
            for em in emails:
                cand = await get_master_by_email(em)
                if cand and _pw_ok(cand):
                    master, ok = cand, True
                    break
    if not master or not ok:
        raise HTTPException(401, "Неверный email, телефон или пароль")
    
    token = _generate_jwt(master["id"])

    from database import get_master_trial_status
    trial = await get_master_trial_status(master["id"])
    pool = await get_pool()
    async with pool.acquire() as conn:
        has_data = await conn.fetchval(
            """
            SELECT EXISTS(
                SELECT 1 FROM clients WHERE master_id=$1
                UNION ALL
                SELECT 1 FROM appointments WHERE master_id=$1
            )
            """,
            master["id"]
        )

    return {"token": token, "trial": trial, "master": {
        "id": master["id"], "name": master["name"], "email": master["email"],
        "work_start": master["work_start"], "work_end": master["work_end"],
        "slot_duration": master["slot_duration"], "timezone": master["timezone"],
        "reminder_days": master["reminder_days"], "payment_card": master["payment_card"],
        "payment_phone": master["payment_phone"], "payment_banks": master["payment_banks"],
        "deposit_enabled": False, "deposit_percent": 30,
        "theme": master["theme"], "has_data": bool(has_data),
    }}


# ── API v1: Онлайн-запись по ссылке ──────────────────────────────────

import re as _re

@app.get("/api/v1/masters/me/booking-link")
async def v1_get_booking_link(master_id: int = Depends(get_jwt_master_id)):
    link = await get_master_booking_link(master_id)
    return {"booking_link": link}


@app.put("/api/v1/masters/booking-link")
async def v1_update_booking_link(body: BookingLinkUpdateRequest, master_id: int = Depends(get_jwt_master_id)):
    link = body.link.strip().lower()
    if not _re.match(r'^[a-z0-9-]{3,30}$', link):
        raise HTTPException(400, "Ссылка должна содержать только буквы a-z, цифры и дефис (3–30 символов)")
    current = await get_master_booking_link(master_id)
    if current != link and await is_booking_linkTaken(link, exclude_master_id=master_id):
        raise HTTPException(400, "Эта ссылка уже занята, выберите другую")
    await update_booking_link(master_id, link)
    return {"ok": True, "booking_link": link}


@app.get("/api/v1/book/{link}")
async def v1_public_master_info(link: str):
    master = await get_master_by_booking_link(link)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    svcs = await get_services(master["id"])
    blocked = await get_blocked_days(master["id"])
    return {
        "master_name": master["name"],
        "work_start": master["work_start"],
        "work_end": master["work_end"],
        "services": [{"id": s[0], "name": s[1], "price_default": s[2], "duration_min": s[3] if len(s) > 3 else 0} for s in svcs],
        "blocked_days": blocked,
    }


def _time_str_to_min(t: str) -> int:
    parts = t.split(":")
    return int(parts[0]) * 60 + (int(parts[1]) if len(parts) > 1 else 0)


async def _get_busy_min(master_id: int, date: str, default_dur: int) -> list[tuple[int, int]]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT time, COALESCE(NULLIF(duration_min,0),$3) as dur "
            "FROM appointments WHERE master_id=$1 AND appointment_date=$2 AND status!='cancelled'",
            master_id, date, default_dur
        )
    return [
        (s := _time_str_to_min(r["time"]), s + r["dur"])
        for r in rows if r["time"]
    ]


async def _slot_in_past(master_id: int, date: str, time: str, margin_min: int = 30) -> bool:
    """Время уже прошло (по поясу мастера) или до него меньше margin_min минут."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        tz = await conn.fetchval("SELECT COALESCE(timezone_offset, 3) FROM masters WHERE id=$1", master_id)
    local_now = _dt.utcnow() + _td(hours=tz or 3)
    try:
        slot_dt = _dt.strptime(f"{date} {time}", "%Y-%m-%d %H:%M")
    except ValueError:
        return False
    return slot_dt < local_now + _td(minutes=margin_min)


@app.get("/api/v1/book/{link}/slots")
async def v1_public_slots(link: str, date: str, duration: int = 0):
    master = await get_master_by_booking_link(link)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    blocked = await get_blocked_days(master["id"])
    if date in blocked:
        return {"slots": []}
    custom = await get_custom_slots_for_date(master["id"], date)
    if custom:
        slots = await get_custom_slots_available(master["id"], date)
    else:
        slots = await get_available_slots(
            master["id"], date,
            master["work_start"], master["work_end"], master["slot_duration"]
        )
    slots = [x for x in slots if not await _slot_in_past(master["id"], date, x)]
    total_dur = duration if duration > master["slot_duration"] else 0
    if total_dur > 0:
        busy = await _get_busy_min(master["id"], date, master["slot_duration"])
        work_end_min = master["work_end"] * 60
        def fits(slot: str) -> bool:
            s = _time_str_to_min(slot)
            e = s + total_dur
            if e > work_end_min:
                return False
            return not any(s < b_end and e > b_start for b_start, b_end in busy)
        slots = [s for s in slots if fits(s)]
    return {"slots": slots}


class _PhoneLookup(BaseModel):
    phone: str


@app.post("/api/v1/book/{link}/lookup")
@limiter.limit("20/minute")
async def v1_public_lookup(request: Request, link: str, body: _PhoneLookup):
    """Страница записи: клиентка ввела номер — узнаём её, чтобы не заполнять данные заново.
    Показываем только имя и первую букву фамилии — посторонний по чужому номеру ничего лишнего не увидит."""
    master = await get_master_by_booking_link(link)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    digits = "".join(ch for ch in body.phone if ch.isdigit())
    if len(digits) == 11 and digits[0] == "8":
        digits = "7" + digits[1:]
    if len(digits) != 11:
        return {"found": False}
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """SELECT name FROM clients WHERE master_id=$1
               AND right(regexp_replace(COALESCE(phone,''),'[^0-9]','','g'), 10) = $2
               ORDER BY id LIMIT 1""", master["id"], digits[-10:])
    if not row or not (row["name"] or "").strip():
        return {"found": False}
    parts = row["name"].split()
    short = parts[0] + (f" {parts[1][0].upper()}." if len(parts) > 1 and parts[1] else "")
    return {"found": True, "name": short}


@app.post("/api/v1/book/{link}", status_code=201)
async def v1_public_book(link: str, body: PublicBookingRequest):
    master = await get_master_by_booking_link(link)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    blocked = await get_blocked_days(master["id"])
    if body.date in blocked:
        raise HTTPException(409, "Этот день недоступен для записи")
    custom = await get_custom_slots_for_date(master["id"], body.date)
    if custom:
        slots = await get_custom_slots_available(master["id"], body.date)
    else:
        slots = await get_available_slots(
            master["id"], body.date,
            master["work_start"], master["work_end"], master["slot_duration"]
        )
    if body.time not in slots:
        raise HTTPException(409, "Этот слот уже занят")
    if await _slot_in_past(master["id"], body.date, body.time, margin_min=0):
        raise HTTPException(409, "Это время уже прошло — выберите другое")
    procedure = body.procedure
    price = 0
    service_duration = 0
    if body.service_id:
        svcs = await get_services(master["id"])
        for s in svcs:
            if s[0] == body.service_id:
                if not procedure:
                    procedure = s[1]
                if not body.price:
                    price = s[2] or 0
                if not body.duration:
                    service_duration = s[3] if len(s) > 3 else 0
                break
    if body.price > 0:
        price = body.price
    if body.duration > 0:
        service_duration = body.duration
    if service_duration > master["slot_duration"]:
        busy = await _get_busy_min(master["id"], body.date, master["slot_duration"])
        s_min = _time_str_to_min(body.time)
        e_min = s_min + service_duration
        if any(s_min < b_end and e_min > b_start for b_start, b_end in busy):
            raise HTTPException(409, "Выбранное время пересекается с другой записью")
    if not procedure:
        procedure = "Запись"
    # add_client нормализует номер и сам проверяет дубли
    if not body.client_name.strip():
        # Клиентку узнали по номеру — берём имя из её карточки
        _digits = "".join(ch for ch in body.client_phone if ch.isdigit())
        if len(_digits) == 11 and _digits[0] == "8":
            _digits = "7" + _digits[1:]
        _pool = await get_pool()
        async with _pool.acquire() as _conn:
            _known = await _conn.fetchval(
                """SELECT name FROM clients WHERE master_id=$1
                   AND right(regexp_replace(COALESCE(phone,''),'[^0-9]','','g'), 10) = $2
                   ORDER BY id LIMIT 1""", master["id"], _digits[-10:])
        if not _known:
            raise HTTPException(400, "Укажите имя и фамилию")
        body.client_name = _known
    client_id = await add_client(master["id"], body.client_name, body.client_phone, birthday=body.birthday)
    if body.marketing_consent:
        _pool = await get_pool()
        async with _pool.acquire() as _conn:
            await _conn.execute("UPDATE clients SET marketing_consent = TRUE WHERE id = $1", client_id)
    appt_id = await add_appointment(
        client_id=client_id,
        master_id=master["id"],
        procedure=procedure,
        appointment_date=body.date,
        time=body.time,
        price=price,
        status="pending",
        duration_min=service_duration,
    )
    
    date_fmt = _dt.strptime(body.date, "%Y-%m-%d").strftime("%d.%m.%Y")
    try:
        if master.get("telegram_id"):
            await bot.send_message(
                master["telegram_id"],
                f"🔔 *Новая запись через ссылку!*\n\n"
                f"👤 {body.client_name}\n"
                f"📱 {body.client_phone}\n"
                f"📅 {date_fmt} в {body.time}\n"
                f"💅 {procedure}",
                parse_mode="Markdown"
            )
    except Exception as e:
        print(f"[NOTIFY] booking telegram error: {e}")
    try:
        await push_to_master(
            master["id"],
            "Новая запись!",
            f"{body.client_name} — {date_fmt} в {body.time}"
        )
    except Exception as e:
        print(f"[NOTIFY] booking push error: {e}")
    try:
        await create_notification(
            master["id"], "new_booking",
            "📅 Новая запись",
            f"{body.client_name} · {date_fmt} в {body.time} · {procedure}",
            appt_id
        )
    except Exception as e:
        print(f"[NOTIFY] booking create_notification error: {e}")

    wallet_url = ""
    if wallet.enabled():
        try:
            wallet_url = await wallet.pass_link(master["id"], client_id)
        except Exception as e:
            print(f"[WALLET] link error: {e}")
    return {"ok": True, "appointment_id": appt_id, "client_id": client_id, "bot_username": config.BOT_USERNAME,
            "wallet_url": wallet_url}


# ── Управление записью клиентом (отмена/перенос) ─────────────────────

class ClientLookupRequest(BaseModel):
    phone: str

class ClientRescheduleRequest(BaseModel):
    phone: str
    appointment_id: int
    new_date: str
    new_time: str
    duration: int = 0


@app.post("/api/v1/my/{slug}/lookup")
async def my_lookup(slug: str, body: ClientLookupRequest):
    master = await get_master_by_booking_link(slug)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    phone = body.phone.strip()
    if not phone.startswith("+"):
        phone = "+" + phone.lstrip("+")
    pool = await get_pool()
    async with pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT id, name FROM clients WHERE master_id=$1 AND phone=$2",
            master["id"], phone
        )
        if not client:
            return {"appointments": [], "client_name": ""}
        today = _dt.now().strftime("%Y-%m-%d")
        rows = await conn.fetch(
            """SELECT a.id, a.procedure, a.appointment_date, a.time,
                      a.price, a.status, a.duration_min
               FROM appointments a
               WHERE a.master_id=$1 AND a.client_id=$2
                 AND a.appointment_date >= $3 AND a.status != 'cancelled'
               ORDER BY a.appointment_date, a.time""",
            master["id"], client["id"], today
        )
    return {
        "client_name": client["name"],
        "appointments": [
            {
                "id": r["id"],
                "procedure": r["procedure"],
                "date": str(r["appointment_date"])[:10],
                "time": r["time"],
                "price": r["price"],
                "status": r["status"],
                "duration_min": r["duration_min"] or 0,
            }
            for r in rows
        ]
    }


@app.post("/api/v1/my/{slug}/cancel")
async def my_cancel(slug: str, body: ClientLookupRequest, appointment_id: int):
    master = await get_master_by_booking_link(slug)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    phone = body.phone.strip()
    if not phone.startswith("+"):
        phone = "+" + phone.lstrip("+")
    pool = await get_pool()
    async with pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT id, name FROM clients WHERE master_id=$1 AND phone=$2",
            master["id"], phone
        )
        if not client:
            raise HTTPException(403, "Клиент не найден")
        appt = await conn.fetchrow(
            "SELECT id, procedure, appointment_date, time FROM appointments WHERE id=$1 AND master_id=$2 AND client_id=$3 AND status!='cancelled'",
            appointment_id, master["id"], client["id"]
        )
        if not appt:
            raise HTTPException(404, "Запись не найдена")
        await conn.execute("UPDATE appointments SET status='cancelled' WHERE id=$1", appt["id"])
    waitlist.slot_freed(master["id"], appt["appointment_date"])
    wallet.touch_appointment(appt["id"])
    date_fmt = _dt.strptime(str(appt["appointment_date"])[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    try:
        if master.get("telegram_id"):
            await bot.send_message(
                master["telegram_id"],
                f"❌ *Клиент отменил запись*\n\n"
                f"👤 {client['name']}\n"
                f"📅 {date_fmt} в {appt['time']}\n"
                f"💅 {appt['procedure']}",
                parse_mode="Markdown"
            )
        await push_to_master(master["id"], "Отмена записи", f"{client['name']} отменил(а) запись на {date_fmt}")
    except Exception as e:
        print(f"[NOTIFY] cancel telegram/push error: {e}")
    try:
        await create_notification(
            master["id"], "client_cancel",
            "❌ Клиент отменил запись",
            f"{client['name']} · {date_fmt} в {appt['time']} · {appt['procedure']}",
            appt["id"]
        )
    except Exception as e:
        print(f"[NOTIFY] cancel create_notification error: {e}")
    return {"ok": True}


@app.post("/api/v1/my/{slug}/reschedule")
async def my_reschedule(slug: str, body: ClientRescheduleRequest):
    master = await get_master_by_booking_link(slug)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    phone = body.phone.strip()
    if not phone.startswith("+"):
        phone = "+" + phone.lstrip("+")
    pool = await get_pool()
    async with pool.acquire() as conn:
        client = await conn.fetchrow(
            "SELECT id, name FROM clients WHERE master_id=$1 AND phone=$2",
            master["id"], phone
        )
        if not client:
            raise HTTPException(403, "Клиент не найден")
        appt = await conn.fetchrow(
            "SELECT id, procedure, appointment_date, time, price, duration_min FROM appointments WHERE id=$1 AND master_id=$2 AND client_id=$3 AND status!='cancelled'",
            body.appointment_id, master["id"], client["id"]
        )
        if not appt:
            raise HTTPException(404, "Запись не найдена")
    # Проверяем что новый слот свободен
    blocked = await get_blocked_days(master["id"])
    if body.new_date in blocked:
        raise HTTPException(409, "Этот день недоступен")
    slots = await get_available_slots(
        master["id"], body.new_date,
        master["work_start"], master["work_end"], master["slot_duration"]
    )
    if body.new_time not in slots:
        raise HTTPException(409, "Выбранное время уже занято")
    if await _slot_in_past(master["id"], body.new_date, body.new_time, margin_min=0):
        raise HTTPException(409, "Это время уже прошло — выберите другое")
    total_dur = body.duration or appt["duration_min"] or master["slot_duration"]
    if total_dur > master["slot_duration"]:
        busy = await _get_busy_min(master["id"], body.new_date, master["slot_duration"])
        s_min = _time_str_to_min(body.new_time)
        if any(s_min < b_end and s_min + total_dur > b_start for b_start, b_end in busy):
            raise HTTPException(409, "Выбранное время пересекается с другой записью")
    # Отменяем старую, создаём новую
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE appointments SET status='cancelled' WHERE id=$1", appt["id"])
    waitlist.slot_freed(master["id"], appt["appointment_date"])
    new_appt_id = await add_appointment(
        client_id=client["id"],
        master_id=master["id"],
        procedure=appt["procedure"],
        appointment_date=body.new_date,
        time=body.new_time,
        price=appt["price"],
        status="pending",
        duration_min=total_dur,
    )
    old_date_fmt = _dt.strptime(str(appt["appointment_date"])[:10], "%Y-%m-%d").strftime("%d.%m.%Y")
    new_date_fmt = _dt.strptime(body.new_date, "%Y-%m-%d").strftime("%d.%m.%Y")
    try:
        if master.get("telegram_id"):
            await bot.send_message(
                master["telegram_id"],
                f"🔄 *Клиент перенёс запись*\n\n"
                f"👤 {client['name']}\n"
                f"📅 С {old_date_fmt} {appt['time']} → на {new_date_fmt} {body.new_time}\n"
                f"💅 {appt['procedure']}\n\n"
                f"Подтвердите новое время:",
                parse_mode="Markdown",
                reply_markup={"inline_keyboard": [[
                    {"text": "✅ Подтвердить", "callback_data": f"confirm_{new_appt_id}"},
                    {"text": "❌ Отменить", "callback_data": f"cancel_{new_appt_id}"}
                ]]}
            )
        await push_to_master(master["id"], "Перенос записи", f"{client['name']} перенёс(ла) на {new_date_fmt} {body.new_time}")
    except Exception as e:
        print(f"[NOTIFY] reschedule telegram/push error: {e}")
    try:
        await create_notification(
            master["id"], "client_reschedule",
            "🔄 Клиент перенёс запись",
            f"{client['name']} · с {old_date_fmt} {appt['time']} → {new_date_fmt} {body.new_time} · {appt['procedure']}",
            new_appt_id
        )
    except Exception as e:
        print(f"[NOTIFY] reschedule create_notification error: {e}")
    return {"ok": True, "new_appointment_id": new_appt_id}


# ── Уведомления ──────────────────────────────────────────────────────

@app.get("/api/v1/notifications")
async def v1_get_notifications(master_id: int = Depends(get_jwt_master_id)):
    items = await get_notifications(master_id)
    return {"notifications": [
        {
            "id": r["id"], "type": r["type"], "title": r["title"], "body": r["body"],
            "is_read": r["is_read"],
            "created_at": str(r["created_at"]),
            "appointment_id": r["appointment_id"],
            "appointment": {
                "procedure": r["procedure"],
                "date": str(r["appointment_date"])[:10] if r["appointment_date"] else None,
                "time": r["time"],
                "status": r["appt_status"],
                "client_name": r["client_name"],
                "client_phone": r["client_phone"],
            } if r["appointment_id"] else None
        }
        for r in items
    ]}


@app.get("/api/v1/notifications/unread-count")
async def v1_unread_count(master_id: int = Depends(get_jwt_master_id)):
    count = await get_unread_count(master_id)
    return {"count": count}


@app.post("/api/v1/notifications/{notif_id}/read")
async def v1_mark_read(notif_id: int, master_id: int = Depends(get_jwt_master_id)):
    await mark_notification_read(notif_id, master_id)
    return {"ok": True}


@app.post("/api/v1/notifications/read-all")
async def v1_read_all(master_id: int = Depends(get_jwt_master_id)):
    await mark_all_notifications_read(master_id)
    return {"ok": True}


class _BroadcastBody(BaseModel):
    title: str
    body: str
    secret: str

@app.post("/api/v1/admin/broadcast")
async def v1_broadcast(body: _BroadcastBody):
    if body.secret != (config.ADMIN_SECRET if hasattr(config, "ADMIN_SECRET") else ""):
        raise HTTPException(403, "Forbidden")
    count = await broadcast_notification(body.title, body.body)
    return {"ok": True, "sent_to": count}


@app.get("/api/v1/telegram-link-token")
async def v1_telegram_link_token(master_id: int = Depends(get_jwt_master_id)):
    from datetime import datetime, timedelta
    token = secrets.token_urlsafe(16)
    expires_at = datetime.utcnow() + timedelta(hours=1)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO telegram_link_tokens (master_id, token, expires_at) VALUES ($1, $2, $3)",
            master_id, token, expires_at
        )
    return {"token": token, "bot_username": config.BOT_USERNAME}


# ─── iOS API v1 — все маршруты для приложения ────────────────────────

class _V1MasterSettings(BaseModel):
    name: str = ""
    work_start: int = 10
    work_end: int = 20
    slot_duration: int = 60
    reminder_days: int = 40
    timezone: str = "Europe/Moscow"

class _V1PaymentUpdate(BaseModel):
    payment_card: str = ""
    payment_phone: str = ""
    payment_banks: str = ""

class _V1ClientCreate(BaseModel):
    name: str
    phone: str
    notes: str = ""
    birthday: str = ""
    source: str = ""
    allergies: str = ""

class _V1AppointmentCreate(BaseModel):
    client_id: int
    procedure: str
    appointment_date: str
    time: str
    price: int = 0
    notes: str = ""
    duration_min: int = 0

class _V1AppointmentUpdate(BaseModel):
    procedure: str = ""
    appointment_date: str = ""
    time: str = ""
    price: int = 0
    service_id: int = 0
    status: str = "confirmed"

class _V1ServiceCreate(BaseModel):
    name: str
    price_default: int = 0
    duration_min: int = 60
    category: str = "Основные"


@app.post("/api/v1/subscription/notify")
async def v1_subscription_notify(master_id: int = Depends(get_jwt_master_id_any)):
    await send_telegram(
        f"💳 Мастер (ID {master_id}) отправил уведомление об оплате подписки.\n"
        f"Проверь и активуй в /admin"
    )
    return {"ok": True}


@app.get("/api/v1/subscription/status")
async def v1_subscription_status(master_id: int = Depends(get_jwt_master_id_any)):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_active FROM masters WHERE id=$1", master_id)
    is_active = bool(row["is_active"]) if row and row["is_active"] is not None else False
    return {"is_active": is_active}


@app.get("/api/v1/masters/me/trial")
async def v1_trial_status(master_id: int = Depends(get_jwt_master_id_any)):
    from database import get_master_trial_status
    trial = await get_master_trial_status(master_id)
    if not trial:
        raise HTTPException(404, "Мастер не найден")
    return trial


@app.get("/api/v1/reminders/templates")
async def v1_get_reminder_templates(master_id: int = Depends(get_jwt_master_id)):
    templates = await get_reminder_templates_v1(master_id)
    return {"templates": templates}


class _ReminderTemplateUpdate(BaseModel):
    template: str
    enabled: bool = True


@app.put("/api/v1/reminders/templates/{type_}")
async def v1_update_reminder_template(
    type_: str,
    body: _ReminderTemplateUpdate,
    master_id: int = Depends(get_jwt_master_id)
):
    await upsert_reminder_template(master_id, type_, body.template, body.enabled)
    return {"ok": True}


@app.get("/api/v1/masters/me")
async def v1_master_me(master_id: int = Depends(get_jwt_master_id)):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, email, telegram_id, COALESCE(phone,'') as phone, work_start, work_end, slot_duration, reminder_days, "
            "COALESCE(timezone,'Europe/Moscow') as timezone, "
            "COALESCE(payment_card,'') as payment_card, "
            "COALESCE(payment_phone,'') as payment_phone, "
            "COALESCE(payment_banks,'') as payment_banks, "
            "COALESCE(deposit_enabled,false) as deposit_enabled, "
            "COALESCE(deposit_percent,30) as deposit_percent, "
            "COALESCE(theme,'pink') as theme, "
            "loyalty_threshold, "
            "COALESCE(birthday_discount_enabled,false) as birthday_discount_enabled, "
            "COALESCE(birthday_discount_percent,10) as birthday_discount_percent, "
            "COALESCE(loyalty_discount_enabled,false) as loyalty_discount_enabled, "
            "COALESCE(loyalty_discount_percent,10) as loyalty_discount_percent, "
            "COALESCE(loyalty_discount_type,'percent') as loyalty_discount_type, "
            "COALESCE(loyalty_discount_rub,0) as loyalty_discount_rub, "
            "COALESCE(timezone_offset,3) as timezone_offset "
            "FROM masters WHERE id=$1", master_id
        )
    if not row:
        raise HTTPException(404, "Мастер не найден")
    return {
        "id": row['id'], "name": row['name'] or "", "email": row['email'] or "", "phone": row['phone'],
        "telegram_id": row['telegram_id'],
        "work_start": row['work_start'] or 10, "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60,
        "reminder_days": row['reminder_days'] or 40,
        "timezone": row['timezone'], "payment_card": row['payment_card'],
        "payment_phone": row['payment_phone'], "payment_banks": row['payment_banks'],
        "deposit_enabled": bool(row['deposit_enabled']),
        "deposit_percent": row['deposit_percent'] or 30,
        "theme": row['theme'],
        "loyalty_threshold": row['loyalty_threshold'] or 10,
        "birthday_discount_enabled": bool(row['birthday_discount_enabled']) if row['birthday_discount_enabled'] is not None else False,
        "birthday_discount_percent": row['birthday_discount_percent'] or 10,
        "loyalty_discount_enabled": bool(row['loyalty_discount_enabled']) if row['loyalty_discount_enabled'] is not None else False,
        "loyalty_discount_percent": row['loyalty_discount_percent'] or 10,
        "loyalty_discount_type": row['loyalty_discount_type'] or "percent",
        "loyalty_discount_rub": row['loyalty_discount_rub'] or 0,
        "timezone_offset": row['timezone_offset'],
    }


@app.put("/api/v1/masters/me")
async def v1_update_master(body: _V1MasterSettings, master_id: int = Depends(get_jwt_master_id)):
    await update_master_full_settings(
        master_id, body.name, body.work_start, body.work_end,
        body.slot_duration, body.reminder_days,
    )
    await update_master_timezone(master_id, body.timezone)
    return {"ok": True}

class _V1ProfileUpdate(BaseModel):
    name: str = ""
    email: str = ""
    phone: str = ""

@app.put("/api/v1/profile")
async def v1_update_profile(body: _V1ProfileUpdate, master_id: int = Depends(get_jwt_master_id)):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET name=$1, email=$2, phone=$3 WHERE id=$4",
            body.name, body.email, body.phone, master_id
        )
    return {"ok": True}

class _TimezoneOffsetUpdate(BaseModel):
    timezone_offset: int

@app.put("/api/v1/masters/me/timezone")
async def v1_update_timezone(body: _TimezoneOffsetUpdate, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET timezone_offset=$1 WHERE id=$2",
            body.timezone_offset, master_id
        )
    return {"ok": True}

@app.delete("/api/v1/masters/me")
async def v1_delete_account(master_id: int = Depends(get_jwt_master_id)):
    """Полное удаление аккаунта мастера и всех его данных (требование Apple 5.1.1(v)).
    Одна транзакция: либо удаляется всё, либо ничего. Порядок — сначала зависимые таблицы."""
    ordered = [
        "notifications", "reviews", "client_waitlist", "client_merge_dismissed", "broadcasts",
        "subscriptions", "appointments", "expenses", "personal_notes", "blocked_days", "custom_slots",
        "device_tokens", "reminder_templates", "telegram_link_tokens", "payment_history",
        "password_reset_codes", "services",
    ]
    pool = await get_pool()
    async with pool.acquire() as conn:
        with_master = {r["table_name"] for r in await conn.fetch(
            """SELECT table_name FROM information_schema.columns
               WHERE table_schema='public' AND column_name='master_id'""")}
        try:
            async with conn.transaction():
                # Карты Wallet клиенток мастера и их регистрации на устройствах
                if "wallet_passes" in with_master:
                    await conn.execute(
                        """DELETE FROM wallet_registrations WHERE serial IN
                           (SELECT serial FROM wallet_passes WHERE master_id=$1)""", master_id)
                    await conn.execute("DELETE FROM wallet_passes WHERE master_id=$1", master_id)
                for t in ordered:
                    if t in with_master:
                        await conn.execute(f"DELETE FROM {t} WHERE master_id=$1", master_id)
                # Всё, что появится в будущем с полем master_id (кроме клиенток и самого мастера)
                for t in sorted(with_master - set(ordered) - {"clients", "masters", "wallet_passes"}):
                    await conn.execute(f"DELETE FROM {t} WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM clients WHERE master_id=$1", master_id)
                res = await conn.execute("DELETE FROM masters WHERE id=$1", master_id)
        except Exception as e:
            print(f"[DELETE ACCOUNT] master {master_id}: {e}")
            raise HTTPException(500, "Не получилось удалить аккаунт, напишите в поддержку")
    if res.endswith(" 0"):
        raise HTTPException(404, "Аккаунт не найден")
    return {"ok": True}

@app.put("/api/v1/masters/me/payment")
async def v1_update_payment(body: _V1PaymentUpdate, master_id: int = Depends(get_jwt_master_id)):
    await update_master_payment(master_id, body.payment_card, body.payment_phone, body.payment_banks)
    return {"ok": True}


@app.post("/api/v1/feedback")
async def v1_feedback(body: FeedbackBody, master_id: int = Depends(get_jwt_master_id)):
    from database import get_master_full
    master = await get_master_full(master_id)
    name = master["name"] if master else "Мастер"
    try:
        await bot.send_message(
            550421233,
            f"💬 *Фидбек от мастера*\n\n👤 {name}\n\n{body.text}",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    return {"ok": True}


@app.put("/api/v1/loyalty-settings")
async def v1_update_loyalty_settings(body: LoyaltySettingsRequest, master_id: int = Depends(get_jwt_master_id)):
    await update_master_loyalty_settings(
        master_id, body.loyalty_enabled, body.loyalty_threshold,
        body.loyalty_discount_percent, body.birthday_enabled, body.birthday_discount_percent,
        body.loyalty_discount_type, body.loyalty_discount_rub
    )
    await wallet.touch(master_id)
    return {"ok": True}


@app.get("/api/v1/masters/me/stats")
async def v1_master_stats(master_id: int = Depends(get_jwt_master_id)):
    return await get_statistics(master_id)


@app.get("/api/v1/masters/me/stats/earnings-by-day")
async def v1_earnings_by_day(days: int = 30, master_id: int = Depends(get_jwt_master_id)):
    from datetime import date, timedelta
    today = date.today()
    cutoff = (today - timedelta(days=days - 1)).isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT appointment_date, COALESCE(SUM(price), 0)::int AS total, COUNT(*)::int AS count "
            "FROM appointments "
            "WHERE master_id=$1 AND appointment_date >= $2 AND status != 'cancelled' "
            "GROUP BY appointment_date",
            master_id, cutoff
        )
    by_date = {r["appointment_date"]: {"total": r["total"], "count": r["count"]} for r in rows}
    result = []
    for i in range(days - 1, -1, -1):
        d = (today - timedelta(days=i)).isoformat()
        e = by_date.get(d, {"total": 0, "count": 0})
        result.append({"date": d, "total": e["total"], "count": e["count"]})
    return {"days": result}


@app.get("/api/v1/masters/me/stats/yearly")
async def v1_yearly_stats(year: int, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT COUNT(*)::int, COALESCE(SUM(price), 0)::int FROM appointments "
            "WHERE master_id=$1 AND appointment_date LIKE $2 AND status != 'cancelled'",
            master_id, f"{year}-%"
        )
        top_rows = await conn.fetch(
            "SELECT procedure, COUNT(*)::int AS cnt FROM appointments "
            "WHERE master_id=$1 AND appointment_date LIKE $2 AND status != 'cancelled' "
            "GROUP BY procedure ORDER BY cnt DESC LIMIT 5",
            master_id, f"{year}-%"
        )
    total_appointments, total_revenue = int(row[0]), int(row[1])
    top_services = [{"procedure": r["procedure"], "count": r["cnt"]} for r in top_rows]
    return {"total_revenue": total_revenue, "total_appointments": total_appointments, "top_services": top_services}


@app.get("/api/v1/masters/me/stats/earnings-by-range")
async def v1_earnings_by_range(start: str, end: str, master_id: int = Depends(get_jwt_master_id)):
    from datetime import date as date_type, timedelta
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT appointment_date, COALESCE(SUM(price), 0)::int AS total, COUNT(*)::int AS count "
            "FROM appointments "
            "WHERE master_id=$1 AND appointment_date >= $2 AND appointment_date <= $3 AND status != 'cancelled' "
            "GROUP BY appointment_date",
            master_id, start, end
        )
    by_date = {r["appointment_date"]: {"total": r["total"], "count": r["count"]} for r in rows}
    start_d = date_type.fromisoformat(start)
    end_d = date_type.fromisoformat(end)
    result = []
    d = start_d
    while d <= end_d:
        ds = d.isoformat()
        e = by_date.get(ds, {"total": 0, "count": 0})
        result.append({"date": ds, "total": e["total"], "count": e["count"]})
        d += timedelta(days=1)
    return {"days": result}


# --- v1 clients ---

@app.get("/api/v1/clients")
async def v1_clients(page: int = 0, search: str = "", master_id: int = Depends(get_jwt_master_id)):
    page_size = 200
    if search:
        rows = await search_clients(master_id, search)
        clients = [
            {"id": r[0], "name": r[1], "phone": r[2], "notes": r[3],
             "last_visit": str(r[4])[:10] if r[4] else None,
             "telegram_id": None, "username": None, "appointments_count": None}
            for r in rows
        ]
        return {"clients": clients, "total": len(clients), "page": 0}
    rows = await get_clients(master_id)
    total = len(rows)
    paged = rows[page * page_size: (page + 1) * page_size]
    clients = [
        {"id": r[0], "name": r[1], "phone": r[2], "notes": r[3],
         "last_visit": str(r[4])[:10] if r[4] else None,
         "telegram_id": None, "username": None, "appointments_count": None}
        for r in paged
    ]
    return {"clients": clients, "total": total, "page": page}


# ── Похожие карточки клиенток и объединение ──────────────────────────
# Дубли появляются, когда номер вводят с опечаткой или при переносе базы ставили номер-заглушку.
# Сервер подсказывает пары, мастер объединяет в одно нажатие (визиты, абонементы, отзывы переносятся).

def _sim_name(n: str) -> str:
    """«Ёлкина  анна» → «анна елкина» (порядок слов не важен)."""
    words = (n or "").lower().replace("ё", "е").split()
    return " ".join(sorted(words))


def _sim_digits(p: str) -> str:
    d = "".join(ch for ch in (p or "") if ch.isdigit())
    if len(d) == 11 and d[0] == "8":
        d = "7" + d[1:]
    return d


def _sim_placeholder(d: str) -> bool:
    return d.startswith("7900001") or d.startswith("7000000") or len(set(d[1:])) <= 2


@app.get("/api/v1/clients/similar")
async def v1_clients_similar(master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT c.id, c.name, c.phone, c.telegram_id IS NOT NULL AS tg,
                      COUNT(a.id) FILTER (WHERE a.status <> 'cancelled') AS visits,
                      MAX(a.appointment_date) FILTER (WHERE a.status <> 'cancelled') AS last_visit
               FROM clients c LEFT JOIN appointments a ON a.client_id = c.id
               WHERE c.master_id=$1 GROUP BY c.id""", master_id)
        dismissed = {(r["a_id"], r["b_id"]) for r in await conn.fetch(
            "SELECT a_id, b_id FROM client_merge_dismissed WHERE master_id=$1", master_id)}
    items = [dict(r) for r in rows]
    for it in items:
        it["_n"] = _sim_name(it["name"])
        it["_d"] = _sim_digits(it["phone"])
        it["_first"] = (it["_n"].split() or [""])[0]
    # Union-find по найденным парам
    parent = {it["id"]: it["id"] for it in items}
    reason = {}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    for i in range(len(items)):
        for j in range(i + 1, len(items)):
            a, b = items[i], items[j]
            key = (min(a["id"], b["id"]), max(a["id"], b["id"]))
            if key in dismissed:
                continue
            why = None
            if a["_n"] and a["_n"] == b["_n"]:
                why = "Одинаковое имя"
            elif (len(a["_d"]) == 11 and len(b["_d"]) == 11
                  and not _sim_placeholder(a["_d"]) and not _sim_placeholder(b["_d"])):
                diff = sum(1 for x, y in zip(a["_d"], b["_d"]) if x != y)
                same_word = bool(set(a["_n"].split()) & set(b["_n"].split()))
                if 0 < diff <= 2 and same_word:
                    why = f"Номер отличается на {diff} {'цифру' if diff == 1 else 'цифры'}"
            if why:
                ra, rb = find(a["id"]), find(b["id"])
                if ra != rb:
                    parent[rb] = ra
                reason[find(a["id"])] = reason.get(find(a["id"])) or why
    groups = {}
    for it in items:
        groups.setdefault(find(it["id"]), []).append(it)
    out = []
    for root, members in groups.items():
        if len(members) < 2:
            continue
        # Предлагаем оставить карточку с настоящим номером, Telegram и большим числом визитов
        members.sort(key=lambda m: (_sim_placeholder(m["_d"]), not m["tg"], -(m["visits"] or 0), m["id"]))
        out.append({
            "reason": reason.get(root) or "Похожие карточки",
            "suggested_keep_id": members[0]["id"],
            "clients": [{
                "id": m["id"], "name": m["name"], "phone": m["phone"], "has_telegram": m["tg"],
                "visits": m["visits"] or 0, "last_visit": str(m["last_visit"])[:10] if m["last_visit"] else None,
                "placeholder_phone": _sim_placeholder(m["_d"]),
            } for m in members],
        })
    return {"groups": out}


class _MergeBody(BaseModel):
    other_ids: list[int]
    phone: str | None = None  # какой номер оставить (по умолчанию — номер карточки, которую оставляем)


@app.post("/api/v1/clients/{keep_id}/merge")
async def v1_clients_merge(keep_id: int, body: _MergeBody, master_id: int = Depends(get_jwt_master_id)):
    others = [i for i in dict.fromkeys(body.other_ids) if i != keep_id]
    if not others or len(others) > 10:
        raise HTTPException(400, "Выберите карточки для объединения")
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            keep = await conn.fetchrow("SELECT * FROM clients WHERE id=$1 AND master_id=$2", keep_id, master_id)
            if not keep:
                raise HTTPException(404, "Карточка не найдена")
            notes_add = []
            for oid in others:
                o = await conn.fetchrow("SELECT * FROM clients WHERE id=$1 AND master_id=$2", oid, master_id)
                if not o:
                    raise HTTPException(404, "Карточка не найдена")
                if o["telegram_id"] and not keep["telegram_id"]:
                    await conn.execute("UPDATE clients SET telegram_id=$1 WHERE id=$2", o["telegram_id"], keep_id)
                if o["birthday"] and not keep["birthday"]:
                    await conn.execute("UPDATE clients SET birthday=$1 WHERE id=$2", o["birthday"], keep_id)
                if o["marketing_consent"] is not None and keep["marketing_consent"] is None:
                    await conn.execute("UPDATE clients SET marketing_consent=$1 WHERE id=$2", o["marketing_consent"], keep_id)
                for t in ("appointments", "subscriptions", "reviews", "client_waitlist"):
                    await conn.execute(f"UPDATE {t} SET client_id=$1 WHERE client_id=$2", keep_id, oid)
                await conn.execute("DELETE FROM wallet_passes WHERE client_id=$1", oid)
                await conn.execute("DELETE FROM client_merge_dismissed WHERE a_id=$1 OR b_id=$1", oid)
                if (o["notes"] or "").strip():
                    notes_add.append(o["notes"].strip())
                notes_add.append(f"Объединено с карточкой «{o['name']}» ({o['phone']})")
                await conn.execute("DELETE FROM clients WHERE id=$1", oid)
            phone = keep["phone"]
            if body.phone:
                d = _sim_digits(body.phone)
                if len(d) != 11:
                    raise HTTPException(400, "Неверный номер")
                phone = "+" + d
            notes = "\n".join([x for x in [(keep["notes"] or "").strip(), *notes_add] if x])
            await conn.execute("UPDATE clients SET phone=$1, notes=$2 WHERE id=$3", phone, notes[:2000], keep_id)
    try:
        await wallet.touch(master_id, keep_id)
    except Exception:
        pass
    return {"ok": True, "merged": len(others)}


class _DismissBody(BaseModel):
    ids: list[int]


@app.post("/api/v1/clients/similar/dismiss")
async def v1_clients_similar_dismiss(body: _DismissBody, master_id: int = Depends(get_jwt_master_id)):
    """«Это разные люди» — больше не предлагать эту группу."""
    ids = sorted(set(body.ids))
    if len(ids) < 2:
        raise HTTPException(400, "Нужно минимум 2 карточки")
    pool = await get_pool()
    async with pool.acquire() as conn:
        n = await conn.fetchval("SELECT COUNT(*) FROM clients WHERE master_id=$1 AND id = ANY($2::int[])", master_id, ids)
        if n != len(ids):
            raise HTTPException(404, "Карточка не найдена")
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                await conn.execute(
                    """INSERT INTO client_merge_dismissed (master_id, a_id, b_id) VALUES ($1,$2,$3)
                       ON CONFLICT DO NOTHING""", master_id, ids[i], ids[j])
    return {"ok": True}


@app.get("/api/v1/clients/{client_id}")
async def v1_client_detail(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client or client.get("master_id") != master_id:
        raise HTTPException(404, "Клиент не найден")
    history_rows = await get_client_history(client_id)
    history = [
        {"procedure": h[0],
         "appointment_date": str(h[1])[:10] if h[1] else "",
         "time": "", "price": h[2] or 0, "notes": h[3], "status": "completed"}
        for h in history_rows
    ]
    return {
        "id": client["id"], "name": client["name"], "phone": client["phone"],
        "notes": client.get("notes"), "username": client.get("username"),
        "telegram_id": client.get("telegram_id"), "history": history,
    }


@app.post("/api/v1/clients/merge-duplicates")
async def v1_merge_clients(master_id: int = Depends(get_jwt_master_id)):
    result = await merge_duplicate_clients(master_id)
    return result

@app.post("/api/v1/clients", status_code=201)
async def v1_create_client(body: _V1ClientCreate, master_id: int = Depends(get_jwt_master_id)):
    client_id = await add_client(master_id, body.name, body.phone, body.notes)
    # iOS читает ответ как MessageResponse — поле ok обязательно
    return {"ok": True, "id": client_id}


@app.put("/api/v1/clients/{client_id}")
async def v1_update_client(client_id: int, body: _V1ClientCreate, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client or client.get("master_id") != master_id:
        raise HTTPException(404, "Клиент не найден")
    await update_client(client_id, master_id, body.name, body.phone, body.notes,
                        birthday=body.birthday or None, source=body.source or None, allergies=body.allergies or None)
    return {"ok": True}


@app.delete("/api/v1/clients/{client_id}")
async def v1_delete_client(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client or client.get("master_id") != master_id:
        raise HTTPException(404, "Клиент не найден")
    await delete_client(client_id, master_id)
    return {"ok": True}


# --- v1 appointments ---

def _fmt_appt(r) -> dict:
    return {
        "id": r['id'], "client_id": r['client_id'], "master_id": r['master_id'],
        "procedure": r['procedure'] or "",
        "appointment_date": str(r['appointment_date'])[:10] if r['appointment_date'] else "",
        "time": r['time'] or "", "price": r['price'] or 0,
        "notes": r['notes'] or "", "status": r['status'] or "confirmed",
        "deposit_status": r.get('deposit_status'),
        "deposit_amount": r.get('deposit_amount'),
        "client_name": r.get('client_name') or "",
        "client_phone": r.get('client_phone') or "",
        "service_done_at": str(r['service_done_at']) if r.get('service_done_at') else None,
        "duration": r.get('duration_min') or 0,
        "client_confirmed": bool(r.get('client_confirmed_at')),
    }


@app.get("/api/v1/appointments")
async def v1_appointments(
    date: str = None, status: str = None,
    master_id: int = Depends(get_jwt_master_id)
):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        conditions = ["a.master_id=$1"]
        params = [master_id]
        if date:
            params.append(date)
            conditions.append(f"a.appointment_date=${len(params)}")
        if status:
            params.append(status)
            conditions.append(f"a.status=${len(params)}")
        where = " AND ".join(conditions)
        rows = await conn.fetch(f"""
            SELECT a.id, a.client_id, a.master_id, a.procedure,
                   a.appointment_date, a.time, a.price, a.notes, a.status,
                   a.deposit_status, a.deposit_amount, a.service_done_at,
                   a.duration_min, a.client_confirmed_at,
                   c.name as client_name, c.phone as client_phone
            FROM appointments a JOIN clients c ON c.id=a.client_id
            WHERE {where}
            ORDER BY a.appointment_date DESC, a.time
        """, *params)
    return [_fmt_appt(dict(r)) for r in rows]


@app.get("/api/v1/appointments/{appt_id}")
async def v1_appointment_detail(appt_id: int, master_id: int = Depends(get_jwt_master_id)):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT a.id, a.client_id, a.master_id, a.procedure,
                   a.appointment_date, a.time, a.price, a.notes, a.status,
                   a.deposit_status, a.deposit_amount, a.service_done_at,
                   a.duration_min, c.name as client_name, c.phone as client_phone
            FROM appointments a JOIN clients c ON c.id=a.client_id
            WHERE a.id=$1 AND a.master_id=$2
        """, appt_id, master_id)
    if not row:
        raise HTTPException(404, "Запись не найдена")
    return _fmt_appt(dict(row))


@app.post("/api/v1/appointments", status_code=201)
async def v1_create_appointment(body: _V1AppointmentCreate, master_id: int = Depends(get_jwt_master_id)):
    appt_id = await add_appointment(
        client_id=body.client_id, master_id=master_id,
        procedure=body.procedure, appointment_date=body.appointment_date,
        price=body.price, notes=body.notes, time=body.time,
        duration_min=body.duration_min,
    )
    
    # Task 1: Send booking confirmation to client
    try:
        client = await get_client(body.client_id)
        if client and client.get("telegram_id"):
            date_fmt = _dt.strptime(body.appointment_date, "%Y-%m-%d").strftime("%d.%m.%Y")
            await bot.send_message(
                client["telegram_id"],
                f"✅ *Запись подтверждена!*\n\n"
                f"📅 {date_fmt} в {body.time}\n"
                f"💅 {body.procedure}\n\n"
                f"До встречи!",
                parse_mode="Markdown"
            )
    except Exception:
        pass  # Don't fail if bot message fails
    
    return {"ok": True}


@app.put("/api/v1/appointments/{appt_id}")
async def v1_update_appointment(appt_id: int, body: _V1AppointmentUpdate, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        appt = await conn.fetchrow("SELECT id, master_id, appointment_date, time FROM appointments WHERE id=$1", appt_id)
        if not appt or appt["master_id"] != master_id:
            raise HTTPException(404, "Запись не найдена")
        await conn.execute(
            "UPDATE appointments SET procedure=$1, appointment_date=$2, time=$3, price=$4, status=$5 WHERE id=$6",
            body.procedure, body.appointment_date, body.time, body.price, body.status, appt_id
        )
    # Перенесли на другое время или отменили — старое окно освободилось
    if (str(appt["appointment_date"])[:10] != body.appointment_date or appt["time"] != body.time
            or body.status == "cancelled"):
        waitlist.slot_freed(master_id, appt["appointment_date"])
    wallet.touch_appointment(appt_id)
    return {"ok": True}


@app.delete("/api/v1/appointments/{appt_id}")
async def v1_cancel_appointment(appt_id: int, master_id: int = Depends(get_jwt_master_id)):
    appt = await get_appointment_with_client(appt_id)
    if not appt or appt["master_id"] != master_id:
        raise HTTPException(404, "Запись не найдена")
    await update_appointment_status(appt_id, "cancelled")
    return {"ok": True}


@app.patch("/api/v1/appointments/{appt_id}/status")
async def v1_update_status(appt_id: int, body: StatusUpdate, master_id: int = Depends(get_jwt_master_id)):
    appt = await get_appointment_with_client(appt_id)
    if not appt or appt["master_id"] != master_id:
        raise HTTPException(404, "Запись не найдена")
    valid = ("confirmed", "cancelled", "pending", "completed", "no_show")
    if body.status not in valid:
        raise HTTPException(400, "Неверный статус")
    await update_appointment_status(appt_id, body.status)
    try:
        client_tg = appt.get("client_tg_id")
        date_str = str(appt.get("appointment_date", ""))
        time_str = appt.get("time", "")
        procedure = appt.get("procedure", "")
        try:
            date_fmt = _dt.strptime(date_str, "%Y-%m-%d").strftime("%d.%m.%Y")
        except Exception:
            date_fmt = date_str
        if client_tg and body.status == "confirmed":
            await bot.send_message(
                client_tg,
                f"✅ Ваша запись подтверждена!\n\n"
                f"📅 {date_fmt} в {time_str}\n"
                f"💅 {procedure}\n\n"
                f"Ждём вас! До встречи 🌸"
            )
        elif client_tg and body.status == "cancelled":
            await bot.send_message(
                client_tg,
                f"❌ К сожалению, ваша запись отменена.\n\n"
                f"📅 {date_fmt} в {time_str}\n\n"
                f"Для новой записи перейдите по ссылке мастера."
            )
    except Exception as e:
        print(f"[NOTIFY] status change notification error: {e}")
    return {"ok": True}


@app.post("/api/v1/appointments/{appt_id}/done")
async def v1_mark_done(appt_id: int, master_id: int = Depends(get_jwt_master_id)):
    appt = await get_appointment_with_client(appt_id)
    if not appt or appt["master_id"] != master_id:
        raise HTTPException(404, "Запись не найдена")
    await update_appointment_service_done(appt_id)
    return {"ok": True}


# --- v1 schedule & slots ---

@app.get("/api/v1/schedule")
async def v1_schedule(date: str, master_id: int = Depends(get_jwt_master_id)):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, a.client_id, a.master_id, a.procedure,
                   a.appointment_date, a.time, a.price, a.notes, a.status,
                   a.deposit_status, a.deposit_amount, a.service_done_at,
                   a.duration_min,
                   c.name as client_name, c.phone as client_phone
            FROM appointments a JOIN clients c ON c.id=a.client_id
            WHERE a.master_id=$1 AND a.appointment_date=$2
            ORDER BY a.time
        """, master_id, date)
    return {"date": date, "appointments": [_fmt_appt(dict(r)) for r in rows]}


@app.get("/api/v1/slots")
async def v1_slots(date: str, master_id: int = Depends(get_jwt_master_id)):
    custom = await get_custom_slots_available(master_id, date)
    if custom is not None and await get_custom_slots_for_date(master_id, date):
        return {"slots": custom}
    master = await get_master_info(master_id)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    slots = await get_available_slots(
        master_id, date,
        master["work_start"], master["work_end"], master["slot_duration"]
    )
    return {"slots": slots}


class _CustomSlotBody(BaseModel):
    date: str
    time: str

@app.get("/api/v1/schedule/custom-slots")
async def v1_get_custom_slots(month: str, master_id: int = Depends(get_jwt_master_id)):
    slots = await get_custom_slots_for_month(master_id, month)
    return {"slots": slots}

@app.post("/api/v1/schedule/custom-slots", status_code=201)
async def v1_add_custom_slot(body: _CustomSlotBody, master_id: int = Depends(get_jwt_master_id)):
    await add_custom_slot(master_id, body.date, body.time)
    return {"ok": True}

@app.delete("/api/v1/schedule/custom-slots")
async def v1_remove_custom_slot(body: _CustomSlotBody, master_id: int = Depends(get_jwt_master_id)):
    await remove_custom_slot(master_id, body.date, body.time)
    return {"ok": True}


# --- v1 services ---

@app.get("/api/v1/services")
async def v1_services(master_id: int = Depends(get_jwt_master_id)):
    rows = await get_services(master_id)
    # Срок «пора на коррекцию»: null — по умолчанию, 0 — не напоминать
    from database import default_correction_days
    pool = await get_pool()
    async with pool.acquire() as conn:
        corr = {r["id"]: r["correction_days"] for r in await conn.fetch(
            "SELECT id, correction_days FROM services WHERE master_id=$1", master_id)}
    return {"services": [
        {"id": r[0], "name": r[1], "price_default": r[2], "duration_min": r[3], "category": r[4],
         "correction_days": corr.get(r[0]),
         "correction_days_effective": corr.get(r[0]) if corr.get(r[0]) is not None else default_correction_days(r[1])}
        for r in rows
    ]}


class _V1ServiceCorrection(BaseModel):
    days: Optional[int] = None  # null — вернуть срок по умолчанию, 0 — не напоминать


@app.patch("/api/v1/services/{svc_id}/correction")
async def v1_service_correction(svc_id: int, body: _V1ServiceCorrection, master_id: int = Depends(get_jwt_master_id)):
    if body.days is not None and not (0 <= body.days <= 365):
        raise HTTPException(400, "Срок должен быть от 0 до 365 дней")
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE services SET correction_days=$1 WHERE id=$2 AND master_id=$3", body.days, svc_id, master_id)
    if res.endswith(" 0"):
        raise HTTPException(404, "Услуга не найдена")
    return {"ok": True}


@app.post("/api/v1/services", status_code=201)
async def v1_create_service(body: _V1ServiceCreate, master_id: int = Depends(get_jwt_master_id)):
    svc_id = await add_service(master_id, body.name, body.price_default, body.duration_min, body.category)
    return {"id": svc_id, "name": body.name, "price_default": body.price_default, "duration_min": body.duration_min, "category": body.category}


@app.put("/api/v1/services/{svc_id}")
async def v1_update_service(svc_id: int, body: _V1ServiceCreate, master_id: int = Depends(get_jwt_master_id)):
    ok = await update_service(svc_id, master_id, body.name, body.price_default, body.duration_min, body.category)
    if not ok:
        raise HTTPException(404, "Услуга не найдена")
    return {"ok": True}


@app.delete("/api/v1/services/{svc_id}")
async def v1_delete_service(svc_id: int, master_id: int = Depends(get_jwt_master_id)):
    ok = await delete_service(svc_id, master_id)
    if not ok:
        raise HTTPException(404, "Услуга не найдена")
    return {"ok": True}


@app.post("/api/v1/auth/forgot-password")
async def v1_forgot_password(body: dict):
    email = body.get("email", "")
    if not email:
        return {"ok": True, "telegram_connected": False}
    master = await get_master_by_email(email)
    if not master:
        return {"ok": True, "telegram_connected": False}
    tg_id = master.get("telegram_id") or 0
    if tg_id <= 0:
        return {"ok": True, "telegram_connected": False}
    code = str(random.randint(100000, 999999))
    expires_at = _dt.utcnow() + _td(minutes=15)
    await save_password_reset_code(master["id"], code, expires_at)
    try:
        await bot.send_message(
            tg_id,
            f"🔐 Код для сброса пароля Solvo Beauty: {code}\n\nКод действителен 15 минут. Если вы не запрашивали сброс — проигнорируйте сообщение.",
            parse_mode="Markdown"
        )
    except Exception:
        pass
    return {"ok": True, "telegram_connected": True}


@app.post("/api/v1/auth/reset-password")
async def v1_reset_password(body: dict):
    email = body.get("email", "")
    code = body.get("code", "")
    new_password = body.get("new_password", "")
    if not email or not code or not new_password:
        raise HTTPException(400, "Неверный или просроченный код")
    master = await get_master_by_email(email)
    if not master:
        raise HTTPException(400, "Неверный или просроченный код")
    ok = await verify_password_reset_code(master["id"], code)
    if not ok:
        raise HTTPException(400, "Неверный или просроченный код")
    password_hash = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET password_hash=$1 WHERE id=$2",
            password_hash, master["id"]
        )
    return {"ok": True}


# ── API: Дашборд — неактивные клиенты ────────────────────────────────

@app.get("/api/dashboard/inactive")
async def dash_inactive(master_id: int = Depends(get_jwt_master_id)):
    days = await get_reminder_days_by_master(master_id)
    rows = await get_inactive_clients(master_id, days)
    return {"clients": [{"id": r[0], "name": r[1], "phone": r[2], "last_visit": str(r[3])[:10] if r[3] else None, "days_ago": r[4]} for r in rows]}


# ── API: Услуги мастера ───────────────────────────────────────────────

class ServiceCreate(BaseModel):
    name: str
    price_default: int = 0

@app.get("/api/services")
async def api_get_services(master_id: int = Depends(get_jwt_master_id)):
    rows = await get_services(master_id)
    return [{"id": r[0], "name": r[1], "price_default": r[2]} for r in rows]

@app.post("/api/services", status_code=201)
async def api_add_service(body: ServiceCreate, master_id: int = Depends(get_jwt_master_id)):
    svc_id = await add_service(master_id, body.name, body.price_default)
    return {"id": svc_id}

@app.delete("/api/services/{svc_id}")
async def api_delete_service(svc_id: int, master_id: int = Depends(get_jwt_master_id)):
    ok = await delete_service(svc_id, master_id)
    if not ok:
        raise HTTPException(status_code=404)
    return {"ok": True}


# ── API: Расширенная статистика ───────────────────────────────────────

@app.get("/api/stats/period")
async def api_stats_period(
    date_from: str, date_to: str,
    master_id: int = Depends(get_jwt_master_id)
):
    data = await get_earnings_by_period(master_id, date_from, date_to)
    by_svc = await get_earnings_by_service(master_id, date_from, date_to)
    data["by_service"] = [{"procedure": r[0], "count": r[1], "total": r[2]} for r in by_svc]
    return data

@app.get("/api/stats/by-service")
async def api_stats_by_service(master_id: int = Depends(get_jwt_master_id)):
    rows = await get_earnings_by_service(master_id)
    return [{"procedure": r[0], "count": r[1], "total": r[2]} for r in rows]

@app.get("/api/stats/by-client")
async def api_stats_by_client(master_id: int = Depends(get_jwt_master_id)):
    rows = await get_earnings_by_client(master_id)
    return [{"name": r[0], "count": r[1], "total": r[2]} for r in rows]

@app.get("/api/stats/chart")
async def api_stats_chart(days: int = 30, master_id: int = Depends(get_jwt_master_id)):
    rows = await get_earnings_by_day(master_id, days)
    return [{"date": r[0], "total": r[1]} for r in rows]


# ── Expenses API ───────────────────────────────────────────────────────

class _V1ExpenseCreate(BaseModel):
    category: str
    amount: int
    description: str = ""
    date: str = ""  # YYYY-MM-DD, defaults to today

@app.get("/api/v1/expenses")
async def v1_get_expenses(master_id: int = Depends(get_jwt_master_id)):
    rows = await get_expenses(master_id)
    return {"expenses": [
        {"id": r[0], "category": r[1], "amount": r[2],
         "description": r[3], "date": str(r[4])} for r in rows
    ]}

@app.post("/api/v1/expenses", status_code=201)
async def v1_add_expense(body: _V1ExpenseCreate, master_id: int = Depends(get_jwt_master_id)):
    from datetime import date, datetime
    try:
        if body.date:
            date_obj = datetime.strptime(body.date, "%Y-%m-%d").date()
        else:
            date_obj = date.today()
        eid = await add_expense(master_id, body.category, body.amount, body.description, date_obj)
        return {"id": eid}
    except Exception as e:
        print(f"[expense error] {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v1/expenses/{expense_id}")
async def v1_delete_expense(expense_id: int, master_id: int = Depends(get_jwt_master_id)):
    ok = await delete_expense(expense_id, master_id)
    if not ok:
        raise HTTPException(404, "Не найдено")
    return {"ok": True}


# ── Дашборд (JWT) ─────────────────────────────────────────────────────

_TEMPLATES_TEXT = {
    "correction": "🔄 Привет, {name}!\n\nПрошло около 2–3 недель после вашего визита — самое время записаться на коррекцию! Жду вас 🗓",
    "miss_you":   "💔 {name}, мы по вам скучаем!\n\nДавно не видели вас. Запишитесь на процедуру — будем рады встрече! ✨",
    "congrats":   "🎉 Привет, {name}!\n\nСпасибо, что выбираете нас. Вы — лучшие клиенты! Ждём вас снова 💅",
}

async def _template_clients(tpl_type: str, master_id: int):
    from database import get_clients_inactive_range, get_clients_with_telegram
    if tpl_type == "congrats":
        return await get_clients_with_telegram(master_id)
    if tpl_type == "correction":
        return await get_clients_inactive_range(master_id, 14, 30)
    if tpl_type == "miss_you":
        return await get_clients_inactive_range(master_id, 30, None)
    return []


@app.get("/api/me")
async def dash_me(master_id: int = Depends(get_jwt_master_id)):
    full = await get_master_full(master_id)
    if not full:
        raise HTTPException(404, "Мастер не найден")
    stats = await get_statistics(master_id)
    webapp_url = config.WEBHOOK_URL or ""
    booking_link = f"{webapp_url}/app/booking.html?master={full['telegram_id']}"
    return {**full, "stats": stats, "booking_link": booking_link}


ADMIN_IDS = [5837984455]  # IDs с доступом к админ-панели


async def require_admin(authorization: str = Header(None)) -> int:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    payload = _decode_jwt(authorization[7:])
    if not payload:
        raise HTTPException(401, "Неверный токен")
    return payload["mid"]


@app.get("/")
async def root_v2():
    return {"status": "ok", "message": "v2"}

@app.get("/api/v2/masters")
async def list_masters_v2():
    """Публичный список всех мастеров"""
    masters = await get_all_masters()
    return {"masters": masters}


@app.get("/api/dashboard/schedule")
async def dash_schedule(date: str, master_id: int = Depends(get_jwt_master_id)):
    rows = await get_master_schedule(master_id, date)
    return {
        "date": date,
        "appointments": [
            {"id": r[0], "client": r[1], "procedure": r[2], "time": r[3], "status": r[4], "phone": r[5], "notes": r[6] or "", "service_done_at": r[7]}
            for r in rows
        ],
    }


def _serialize_clients(rows):
    return [{"id": r[0], "name": r[1], "phone": r[2], "notes": r[3], "last_visit": str(r[4])[:10] if r[4] else None} for r in rows]

@app.get("/api/dashboard/clients")
async def dash_clients(
    page: int = 0,
    search: str = "",
    master_id: int = Depends(get_jwt_master_id),
):
    if search:
        rows = await search_clients(master_id, search)
        return {"clients": _serialize_clients(rows), "total": len(rows)}
    rows = await get_clients(master_id)
    total = len(rows)
    page_size = 20
    paged = rows[page * page_size: (page + 1) * page_size]
    return {"clients": _serialize_clients(paged), "total": total}

@app.get("/api/dashboard/clients/{client_id}")
async def dash_client(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client:
        raise HTTPException(status_code=404)
    history = await get_client_history(client_id)
    hist = [{"procedure": h[0], "date": str(h[1])[:10], "price": h[2], "notes": h[3]} for h in history]
    return {**client, "history": hist}


@app.put("/api/dashboard/clients/{client_id}")
async def dash_update_client(client_id: int, body: ClientUpdate, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client or client.get("master_id") != master_id:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    await update_client(client_id, master_id,
        body.name or client.get("name", ""),
        body.phone or client.get("phone", ""),
        body.notes or client.get("notes", ""),
        birthday=body.birthday or client.get("birthday"),
        source=body.source or client.get("source"),
        allergies=body.allergies or client.get("allergies"))
    if body.username is not None:
        await update_client_username(client_id, master_id, body.username)
    return {"ok": True}


@app.delete("/api/dashboard/clients/{client_id}")
async def dash_delete_client(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    client = await get_client(client_id)
    if not client or client.get("master_id") != master_id:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    await delete_client(client_id, master_id)
    return {"ok": True}


@app.post("/api/dashboard/clients", status_code=201)
async def dash_add_client(body: ClientCreate, master_id: int = Depends(get_jwt_master_id)):
    client_id = await add_client(master_id, body.name, body.phone, body.notes)
    return {"id": client_id}

@app.post("/api/dashboard/appointments", status_code=201)
async def dash_add_appointment(body: AppointmentCreate, master_id: int = Depends(get_jwt_master_id)):
    appt_id = await add_appointment(
        body.client_id, master_id, body.procedure,
        body.appointment_date, body.price, body.notes,
        time=body.time,
    )
    return {"id": appt_id}


@app.get("/api/dashboard/appointments/{appointment_id}")
async def dash_get_appointment(
    appointment_id: int,
    master_id: int = Depends(get_jwt_master_id),
):
    appt = await get_appointment_with_client(appointment_id)
    if not appt or appt["master_id"] != master_id:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    return {
        "id": appt["id"],
        "client": appt.get("client_name"),
        "phone": appt.get("client_telegram_id") or "",
        "procedure": appt.get("procedure"),
        "date": str(appt.get("appointment_date")) if appt.get("appointment_date") else "",
        "time": appt.get("time") or "",
        "status": appt.get("status") or "confirmed",
        "price": appt.get("deposit_amount") or 0,
        "notes": appt.get("notes") or "",
        "service_done_at": appt.get("service_done_at"),
    }


@app.patch("/api/dashboard/appointments/{appointment_id}/done")
async def dash_mark_done(
    appointment_id: int,
    master_id: int = Depends(get_jwt_master_id),
):
    appt = await get_appointment_with_client(appointment_id)
    if not appt or appt["master_id"] != master_id:
        raise HTTPException(status_code=404, detail="Запись не найдена")
    await update_appointment_service_done(appointment_id)
    return {"ok": True}

@app.put("/api/dashboard/settings")
async def dash_settings(body: _MasterSettings, master_id: int = Depends(get_jwt_master_id)):
    await update_master_full_settings(
        master_id, body.name, body.work_start, body.work_end,
        body.slot_duration, body.reminder_days,
    )
    return {"ok": True}


@app.put("/api/dashboard/payment")
async def dash_payment(body: _PaymentUpdate, master_id: int = Depends(get_jwt_master_id)):
    await update_master_payment(master_id, body.payment_card, body.payment_phone, body.payment_banks)
    return {"ok": True}


class _TimezoneUpdate(BaseModel):
    timezone: str

@app.put("/api/dashboard/timezone")
async def dash_timezone(body: _TimezoneUpdate, master_id: int = Depends(get_jwt_master_id)):
    await update_master_timezone(master_id, body.timezone)
    return {"ok": True}


# ── Предоплата ────────────────────────────────────────────────────────

class _DepositSettings(BaseModel):
    deposit_enabled: bool
    deposit_percent: int

@app.get("/api/dashboard/deposit")
async def dash_get_deposit(master_id: int = Depends(get_jwt_master_id)):
    from database import get_master_deposit_settings
    return await get_master_deposit_settings(master_id)

@app.put("/api/dashboard/deposit")
async def dash_update_deposit(body: _DepositSettings, master_id: int = Depends(get_jwt_master_id)):
    from database import update_master_deposit_settings
    await update_master_deposit_settings(master_id, body.deposit_enabled, body.deposit_percent)
    return {"ok": True}


# ── Шаблоны рассылки ──────────────────────────────────────────────────

@app.get("/api/dashboard/templates/count")
async def dash_template_count(type: str, master_id: int = Depends(get_jwt_master_id)):
    if type not in _TEMPLATES_TEXT:
        raise HTTPException(400, "Unknown template type")
    clients = await _template_clients(type, master_id)
    return {"count": len(clients)}


class _SendMessageBody(BaseModel):
    telegram_id: int
    text: str


@app.post("/api/dashboard/send-message")
async def dash_send_message(body: _SendMessageBody, master_id: int = Depends(get_jwt_master_id)):
    """Отправить сообщение клиенту от имени бота."""
    try:
        await bot.send_message(chat_id=body.telegram_id, text=body.text)
        return {"ok": True}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


class _SendTemplateBody(BaseModel):
    template_type: str

@app.post("/api/dashboard/templates/send")
async def dash_send_template(body: _SendTemplateBody, master_id: int = Depends(get_jwt_master_id)):
    if body.template_type not in _TEMPLATES_TEXT:
        raise HTTPException(400, "Unknown template type")
    clients = await _template_clients(body.template_type, master_id)
    text_tpl = _TEMPLATES_TEXT[body.template_type]
    sent = 0
    for client_id, name, telegram_id, *_ in clients:
        try:
            await bot.send_message(telegram_id, text_tpl.format(name=name), parse_mode="Markdown")
            sent += 1
        except Exception:
            pass
    return {"sent": sent, "total": len(clients)}


# ── Отзывы ────────────────────────────────────────────────────────────

@app.get("/api/dashboard/reviews")
async def dash_reviews(master_id: int = Depends(get_jwt_master_id)):
    from database import get_master_reviews
    rows = await get_master_reviews(master_id, 20)
    return {"reviews": [
        {"rating": r[0], "client_name": r[1], "procedure": r[2], "date": str(r[3])[:10]}
        for r in rows
    ]}


# ── dashboard.html отдаётся без кеша (Telegram кеширует WebApp агрессивно) ──
@app.get("/app/dashboard.html")
async def serve_dashboard():
    from fastapi.responses import FileResponse
    return FileResponse("webapp/dashboard.html", headers={
        "Cache-Control": "no-cache, no-store, must-revalidate",
        "Pragma": "no-cache",
        "Expires": "0",
    })

# ── Нерабочие дни ────────────────────────────────────────────────────

class _BlockedDayBody(BaseModel):
    date: str

@app.get("/api/v1/schedule/blocked-days")
async def v1_get_blocked_days(master_id: int = Depends(get_jwt_master_id)):
    days = await get_blocked_days(master_id)
    return {"blocked_days": days}

@app.post("/api/v1/schedule/blocked-days")
async def v1_add_blocked_day(body: _BlockedDayBody, master_id: int = Depends(get_jwt_master_id)):
    await add_blocked_day(master_id, body.date)
    return {"ok": True}

@app.delete("/api/v1/schedule/blocked-days/{date}")
async def v1_remove_blocked_day(date: str, master_id: int = Depends(get_jwt_master_id)):
    await remove_blocked_day(master_id, date)
    return {"ok": True}

# ── Страница онлайн-записи для клиентов ──────────────────────────────
@app.get("/book/{link}")
async def serve_booking_page(link: str):
    from fastapi.responses import FileResponse
    return FileResponse("webapp/book.html")

# ── Остальная статика (webapp) ────────────────────────────────
app.mount("/app", StaticFiles(directory="webapp", html=True), name="webapp")


# ── Admin Panel ─────────────────────────────────────────────

def _create_admin_token():
    return jwt.encode(
        {"admin": True, "exp": _dt.utcnow() + _td(hours=8)},
        _jwt_secret(), algorithm="HS256"
    )


def _verify_admin_token(authorization: str = Header(None)):
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Требуется авторизация")
    try:
        payload = jwt.decode(authorization[7:], _jwt_secret(), algorithms=["HS256"])
        if not payload.get("admin"):
            raise HTTPException(401, "Неверный токен")
    except jwt.InvalidTokenError:
        raise HTTPException(401, "Неверный или устаревший токен")


class _AdminLoginBody(BaseModel):
    password: str


@app.post("/admin/api/login")
async def admin_login(body: _AdminLoginBody):
    if body.password != ADMIN_SECRET:
        raise HTTPException(401, "Неверный пароль")
    return {"token": _create_admin_token()}


@app.get("/api/admin/masters", dependencies=[Depends(_verify_admin_token)])
async def admin_list_masters():
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT
                m.id, m.name, m.email, m.phone, m.booking_link, m.is_active,
                (SELECT COUNT(*) FROM clients c WHERE c.master_id = m.id) AS clients_count,
                (SELECT COUNT(*) FROM appointments a WHERE a.master_id = m.id) AS appointments_count
            FROM masters m
            ORDER BY m.name
            """
        )
    masters = [
        {
            "id": r["id"],
            "name": r["name"] or "Без имени",
            "email": r["email"] or "",
            "phone": r["phone"] or "",
            "booking_link": r["booking_link"] or "",
            "is_active": bool(r["is_active"]) if r["is_active"] is not None else True,
            "clients_count": r["clients_count"],
            "appointments_count": r["appointments_count"],
        }
        for r in rows
    ]
    return {"masters": masters}


@app.get("/api/admin/master/{master_id}/data", dependencies=[Depends(_verify_admin_token)])
async def admin_master_data(master_id: int):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, name, email, phone, booking_link, is_active,
                   theme, payment_card, payment_phone, payment_banks
            FROM masters WHERE id=$1
            """,
            master_id
        )
        if not row:
            raise HTTPException(404, "Мастер не найден")

        total_clients = await conn.fetchval(
            "SELECT COUNT(*) FROM clients WHERE master_id=$1", master_id
        )
        total_appointments = await conn.fetchval(
            "SELECT COUNT(*) FROM appointments WHERE master_id=$1", master_id
        )
        total_earnings = await conn.fetchval(
            "SELECT COALESCE(SUM(price),0) FROM appointments WHERE master_id=$1 AND status != 'cancelled'",
            master_id
        )
        month_earnings = await conn.fetchval(
            """
            SELECT COALESCE(SUM(price),0) FROM appointments
            WHERE master_id=$1 AND status != 'cancelled'
              AND appointment_date >= to_char(date_trunc('month', CURRENT_DATE), 'YYYY-MM-DD')
            """,
            master_id
        )
        clients_rows = await conn.fetch(
            """
            SELECT c.name, c.phone,
                   (SELECT MAX(a.appointment_date) FROM appointments a WHERE a.client_id = c.id) AS last_visit
            FROM clients c
            WHERE c.master_id=$1
            ORDER BY c.name
            """,
            master_id
        )

    master = {
        "id": row["id"],
        "name": row["name"] or "",
        "email": row["email"] or "",
        "phone": row["phone"] or "",
        "booking_link": row["booking_link"] or "",
        "is_active": bool(row["is_active"]) if row["is_active"] is not None else True,
        "theme": row["theme"] or "pink",
        "payment_card": row["payment_card"] or "",
        "payment_phone": row["payment_phone"] or "",
        "payment_banks": row["payment_banks"] or "",
    }
    stats = {
        "total_clients": total_clients,
        "total_appointments": total_appointments,
        "total_earnings": total_earnings,
        "month_earnings": month_earnings,
    }
    clients = [
        {"name": c["name"] or "-", "phone": c["phone"] or "", "last_visit": c["last_visit"] or ""}
        for c in clients_rows
    ]
    return {"master": master, "stats": stats, "clients": clients, "total_clients": total_clients}


@app.delete("/api/admin/master/{master_id}", dependencies=[Depends(_verify_admin_token)])
async def admin_delete_master(master_id: int):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id, name, email FROM masters WHERE id=$1", master_id)
        if not row:
            raise HTTPException(404, "Мастер не найден")
        try:
            async with conn.transaction():
                await conn.execute("DELETE FROM expenses WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM notifications WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM personal_notes WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM blocked_days WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM custom_slots WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM device_tokens WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM reminder_templates WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM telegram_link_tokens WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM payment_history WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM password_reset_codes WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM reviews WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM appointments WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM services WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM subscriptions WHERE master_id=$1", master_id)
                client_ids = await conn.fetch("SELECT id FROM clients WHERE master_id=$1", master_id)
                for c in client_ids:
                    await conn.execute("DELETE FROM subscriptions WHERE client_id=$1", c["id"])
                    await conn.execute("DELETE FROM appointments WHERE client_id=$1", c["id"])
                await conn.execute("DELETE FROM clients WHERE master_id=$1", master_id)
                await conn.execute("DELETE FROM masters WHERE id=$1", master_id)
        except Exception as e:
            raise HTTPException(500, f"Ошибка удаления: {e}")
    return {"ok": True, "deleted_id": master_id, "name": row["name"], "email": row["email"]}


@app.post("/api/admin/master/{master_id}/toggle-active", dependencies=[Depends(_verify_admin_token)])
async def admin_toggle_active(master_id: int):
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT is_active FROM masters WHERE id=$1", master_id)
        if not row:
            raise HTTPException(404, "Мастер не найден")
        new_state = 0 if row["is_active"] else 1
        await conn.execute("UPDATE masters SET is_active=$1 WHERE id=$2", new_state, master_id)
    return {"ok": True, "is_active": bool(new_state)}


@app.post("/api/admin/master/{master_id}/extend-trial", dependencies=[Depends(_verify_admin_token)])
async def admin_extend_trial(master_id: int, days: int = 90):
    from database import get_pool
    from datetime import datetime, timedelta
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT name, trial_end_date, paid_until FROM masters WHERE id=$1", master_id)
        if not row:
            raise HTTPException(404, "Мастер не найден")
        current_end = row["paid_until"] or row["trial_end_date"] or datetime.utcnow()
        if current_end < datetime.utcnow():
            current_end = datetime.utcnow()
        new_end = current_end + timedelta(days=days)
        await conn.execute(
            "UPDATE masters SET is_active=1, trial_end_date=$1 WHERE id=$2",
            new_end, master_id
        )
    return {"ok": True, "name": row["name"], "new_trial_end": new_end.isoformat()}


@app.post("/api/v1/device/token")
async def register_device_token_beauty(
    token: str = Form(...),
    master_id: int = Depends(get_jwt_master_id),
):
    await save_device_token(master_id, token)
    return {"ok": True}


@app.post("/api/v1/debug/push-test")
async def debug_push_test(master_id: int = Depends(get_jwt_master_id)):
    tokens = await get_device_tokens_for_master(master_id)
    if not tokens:
        return {"ok": False, "error": "no_device_tokens", "master_id": master_id}
    results = []
    for t in tokens:
        detail = await send_push_detailed(t, "Тест пуша", "Если видишь это — push работает ✅")
        detail["token"] = t[:16] + "..."
        results.append(detail)
    return {"ok": True, "master_id": master_id, "results": results}


@app.post("/api/admin/test-reminders")
async def admin_test_reminders(
    reminder_type: str = "24h",
    token: str = "",
):
    """Ручной запуск напоминаний для отладки. reminder_type: 24h | 2h"""
    if token != ADMIN_SECRET:
        raise HTTPException(403, "Forbidden")
    from scheduler import send_client_reminders_24h, send_client_reminders_2h
    if reminder_type == "24h":
        await send_client_reminders_24h(bot)
        return {"ok": True, "type": "24h", "message": "Запущено — смотри логи Render"}
    elif reminder_type == "2h":
        await send_client_reminders_2h(bot)
        return {"ok": True, "type": "2h", "message": "Запущено — смотри логи Render"}
    else:
        raise HTTPException(400, "reminder_type должен быть 24h или 2h")


@app.get("/api/admin/check-appointments")
async def admin_check_appointments(days: int = 7):
    """Показывает ближайшие записи и есть ли у клиентов Telegram. Ничего не отправляет."""
    from database import get_pool
    from datetime import date, timedelta
    date_from = date.today().isoformat()
    date_to = (date.today() + timedelta(days=days)).isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, a.appointment_date, a.time, a.procedure,
                   c.name as client_name, c.telegram_id,
                   a.reminder_24h_sent, a.reminder_2h_sent
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.appointment_date BETWEEN $1 AND $2
              AND a.status != 'cancelled'
            ORDER BY a.appointment_date, a.time
        """, date_from, date_to)
    result = []
    for r in rows:
        result.append({
            "id": r["id"],
            "date": str(r["appointment_date"]),
            "time": r["time"],
            "procedure": r["procedure"],
            "client": r["client_name"],
            "has_telegram": r["telegram_id"] is not None,
            "reminder_24h_sent": bool(r["reminder_24h_sent"]),
            "reminder_2h_sent": bool(r["reminder_2h_sent"]),
        })
    return {
        "total": len(result),
        "with_telegram": sum(1 for x in result if x["has_telegram"]),
        "appointments": result
    }


@app.get("/admin/")
@app.get("/admin")
async def serve_admin():
    from fastapi.responses import FileResponse
    html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webapp", "admin.html")
    return FileResponse(html_path)


# ── Юридические страницы ──────────────────────────────────────────────

@app.get("/oferta", response_class=HTMLResponse)
async def oferta_page():
    with open("webapp/oferta.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/privacy", response_class=HTMLResponse)
async def privacy_page():
    with open("webapp/privacy.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/contacts", response_class=HTMLResponse)
async def contacts_page():
    with open("webapp/contacts.html", "r", encoding="utf-8") as f:
        return f.read()


@app.get("/payment/success", response_class=HTMLResponse)
async def payment_success():
    with open("webapp/payment_success.html", "r", encoding="utf-8") as f:
        return f.read()


# ── Волна 2: свободные окна и рассылки клиенткам ──────────────────────

_RU_WD = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]
_RU_MON_GEN = ["января", "февраля", "марта", "апреля", "мая", "июня",
               "июля", "августа", "сентября", "октября", "ноября", "декабря"]
_BOOK_URL = "https://beauty-bot-44ou.onrender.com/book/{slug}"


async def _master_slug_tz(master_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT booking_link, COALESCE(timezone_offset, 3) AS tz, name FROM masters WHERE id=$1", master_id)
    return (row["booking_link"] or "", row["tz"], row["name"] or "") if row else ("", 3, "")


@app.get("/api/v1/free-windows")
async def v1_free_windows(date_from: str, date_to: str, duration: int = 0,
                          master_id: int = Depends(get_jwt_master_id)):
    """Свободные окна мастера по дням + готовый текст для Telegram-канала."""
    slug, tz, _ = await _master_slug_tz(master_id)
    if not slug:
        raise HTTPException(400, "Сначала задайте ссылку для записи в настройках")
    try:
        d_from = _dt.strptime(date_from, "%Y-%m-%d").date()
        d_to = _dt.strptime(date_to, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(400, "Неверный формат даты")
    if d_to < d_from or (d_to - d_from).days > 13:
        raise HTTPException(400, "Период — от 1 до 14 дней")
    from datetime import timedelta as _td
    local_now = _dt.utcnow() + _td(hours=tz)
    days = []
    d = d_from
    while d <= d_to:
        ds = d.strftime("%Y-%m-%d")
        slots = (await v1_public_slots(slug, ds, duration))["slots"]
        if d == local_now.date():  # сегодня — только время, которое ещё не прошло (+30 мин запаса)
            cutoff = (local_now + _td(minutes=30)).strftime("%H:%M")
            slots = [s for s in slots if s >= cutoff]
        if d >= local_now.date() and slots:
            days.append({"date": ds, "slots": slots})
        d += _td(days=1)
    url = _BOOK_URL.format(slug=slug)
    lines = []
    for day in days:
        dt = _dt.strptime(day["date"], "%Y-%m-%d")
        shown = day["slots"][:8]
        tail = " …" if len(day["slots"]) > 8 else ""
        lines.append(f"{_RU_WD[dt.weekday()]}, {dt.day} {_RU_MON_GEN[dt.month - 1]} — {', '.join(shown)}{tail}")
    text = ("💅 Свободные окошки\n\n" + "\n".join(lines) + f"\n\nЗаписаться: {url}") if lines else ""
    return {"days": days, "booking_url": url, "text": text}


class _BroadcastIds(BaseModel):
    client_ids: list[int]


class _BroadcastSend(BaseModel):
    client_ids: list[int]
    text: str


async def _broadcast_recipients(master_id: int, ids: list[int]):
    """Делит выбранных клиенток: получат / без Telegram / без согласия / ещё не спрашивали."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, telegram_id, marketing_consent FROM clients WHERE master_id=$1 AND id = ANY($2::int[])",
            master_id, ids)
    receive = [r for r in rows if r["telegram_id"] and r["marketing_consent"] is True]
    no_tg = [r for r in rows if not r["telegram_id"]]
    no_consent = [r for r in rows if r["telegram_id"] and r["marketing_consent"] is not True]
    not_asked = [r for r in no_consent if r["marketing_consent"] is None]
    return rows, receive, no_tg, no_consent, not_asked


@app.post("/api/v1/broadcasts/preview")
async def v1_broadcast_preview(body: _BroadcastIds, master_id: int = Depends(get_jwt_master_id)):
    rows, receive, no_tg, no_consent, not_asked = await _broadcast_recipients(master_id, body.client_ids)
    return {"selected": len(rows), "will_receive": len(receive), "no_telegram": len(no_tg),
            "no_consent": len(no_consent), "not_asked": len(not_asked)}


def _mk_keyboard(slug: str, client_id: int):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    rows = []
    if slug:
        rows.append([InlineKeyboardButton(text="💅 Записаться", url=_BOOK_URL.format(slug=slug))])
    rows.append([InlineKeyboardButton(text="Не присылать акции", callback_data=f"mk_off:{client_id}")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


async def _send_broadcast(broadcast_id: int, slug: str, recipients: list, text: str):
    delivered = 0
    for r in recipients:
        try:
            await bot.send_message(r["telegram_id"], text, reply_markup=_mk_keyboard(slug, r["id"]))
            delivered += 1
        except Exception as e:
            print(f"[BROADCAST #{broadcast_id}] client {r['id']}: {e}")
        await asyncio.sleep(0.05)  # бережём лимиты Telegram
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE broadcasts SET delivered=$1 WHERE id=$2", delivered, broadcast_id)


@app.post("/api/v1/broadcasts")
async def v1_broadcast_send(body: _BroadcastSend, master_id: int = Depends(get_jwt_master_id)):
    text = (body.text or "").strip()
    if not (1 <= len(text) <= 1500):
        raise HTTPException(400, "Текст рассылки — от 1 до 1500 символов")
    slug, tz, _ = await _master_slug_tz(master_id)
    from datetime import timedelta as _td
    local_today = (_dt.utcnow() + _td(hours=tz)).strftime("%Y-%m-%d")
    pool = await get_pool()
    async with pool.acquire() as conn:
        sent_today = await conn.fetchval(
            "SELECT COUNT(*) FROM broadcasts WHERE master_id=$1 AND to_char(created_at + make_interval(hours => $2), 'YYYY-MM-DD') = $3",
            master_id, tz, local_today)
    if sent_today:
        raise HTTPException(429, "Сегодня рассылка уже была — следующую можно отправить завтра")
    _, receive, _, _, _ = await _broadcast_recipients(master_id, body.client_ids)
    if not receive:
        raise HTTPException(400, "Никто из выбранных клиенток не согласился получать акции")
    async with pool.acquire() as conn:
        bid = await conn.fetchval(
            "INSERT INTO broadcasts (master_id, text, total) VALUES ($1, $2, $3) RETURNING id",
            master_id, text, len(receive))
    asyncio.create_task(_send_broadcast(bid, slug, [dict(r) for r in receive], text))
    return {"ok": True, "id": bid, "will_receive": len(receive)}


@app.get("/api/v1/broadcasts")
async def v1_broadcast_history(master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, created_at, text, total, delivered FROM broadcasts WHERE master_id=$1 ORDER BY created_at DESC LIMIT 20",
            master_id)
    return {"broadcasts": [{"id": r["id"], "created_at": r["created_at"].isoformat(), "text": r["text"],
                            "total": r["total"], "delivered": r["delivered"]} for r in rows]}


async def _send_consent_asks(master_name: str, recipients: list):
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
    for r in recipients:
        kb = InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(text="Да, присылайте", callback_data=f"mk_yes:{r['id']}"),
            InlineKeyboardButton(text="Нет", callback_data=f"mk_off:{r['id']}"),
        ]])
        try:
            await bot.send_message(
                r["telegram_id"],
                f"Здравствуйте! {master_name} хочет иногда присылать вам свободные окошки и акции. Получать такие сообщения?",
                reply_markup=kb)
        except Exception as e:
            print(f"[CONSENT-ASK] client {r['id']}: {e}")
        await asyncio.sleep(0.05)


@app.post("/api/v1/broadcasts/ask-consent")
async def v1_broadcast_ask_consent(master_id: int = Depends(get_jwt_master_id)):
    """Один раз спрашивает согласие у клиенток с Telegram, которых ещё не спрашивали."""
    _, _, master_name = await _master_slug_tz(master_id)
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """UPDATE clients SET marketing_asked_at = NOW()
               WHERE master_id=$1 AND telegram_id IS NOT NULL AND marketing_consent IS NULL AND marketing_asked_at IS NULL
               RETURNING id, telegram_id""", master_id)
    asyncio.create_task(_send_consent_asks(master_name or "Ваш мастер", [dict(r) for r in rows]))
    return {"ok": True, "asked": len(rows)}


# ── Импорт клиенток (текст и файлы Excel/CSV) ────────────────────────
# Раньше обработчик был только в старом api/main.py, который не запускается, —
# поэтому импорт из приложения не работал (сервер отвечал 405).

class _ImportItem(BaseModel):
    name: str
    phone: str
    notes: str = ""
    birthday: str = ""  # "MM-DD", как при записи по ссылке


class _ImportRequest(BaseModel):
    clients: list[_ImportItem]


@app.post("/api/v1/clients/import", status_code=201)
async def v1_import_clients(body: _ImportRequest, master_id: int = Depends(get_jwt_master_id)):
    if len(body.clients) > 5000:
        raise HTTPException(400, "За один раз — не больше 5000 клиенток")
    imported = skipped = 0
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = {"".join(ch for ch in (r["phone"] or "") if ch.isdigit())[-10:]
                    for r in await conn.fetch("SELECT phone FROM clients WHERE master_id=$1", master_id)}
    for c in body.clients:
        name = (c.name or "").strip()
        digits = "".join(ch for ch in c.phone if ch.isdigit())
        if len(digits) == 10:
            digits = "7" + digits
        if len(digits) == 11 and digits[0] == "8":
            digits = "7" + digits[1:]
        if not name or len(digits) != 11 or digits[-10:] in existing:
            skipped += 1
            continue
        await add_client(master_id, name[:100], "+" + digits, notes=(c.notes or "")[:500], birthday=c.birthday or "")
        existing.add(digits[-10:])
        imported += 1
    return {"imported": imported, "skipped": skipped}


_IMP_NAME = ("фио", "имя", "клиент", "name", "client", "full name", "фамилия имя")
_IMP_LAST = ("фамилия", "last name", "surname")
_IMP_FIRST = ("имя", "first name")
_IMP_PHONE = ("телефон", "тел", "phone", "мобильный", "номер", "mobile")
_IMP_BDAY = ("день рождения", "дата рождения", "birthday", "др", "birth")
_IMP_NOTES = ("комментар", "заметк", "примечан", "notes", "comment", "описание")


def _imp_cell(v) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        v = int(v)
    if hasattr(v, "strftime"):
        return v.strftime("%d.%m.%Y")
    return str(v).strip()


def _imp_bday(v: str) -> str:
    """«15.03.1995», «15.03», «1995-03-15» → «03-15»."""
    import re
    v = v.strip()
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})", v)
    if m:
        return f"{int(m.group(2)):02d}-{int(m.group(3)):02d}"
    m = re.match(r"^(\d{1,2})[./-](\d{1,2})", v)
    if m and 1 <= int(m.group(1)) <= 31 and 1 <= int(m.group(2)) <= 12:
        return f"{int(m.group(2)):02d}-{int(m.group(1)):02d}"
    return ""


def _imp_rows(filename: str, data: bytes) -> list[list[str]]:
    name = (filename or "").lower()
    if name.endswith((".xlsx", ".xlsm")):
        import io
        import openpyxl
        wb = openpyxl.load_workbook(io.BytesIO(data), read_only=True, data_only=True)
        ws = wb.worksheets[0]
        return [[_imp_cell(v) for v in row] for row in ws.iter_rows(values_only=True)]
    if name.endswith(".xls"):
        raise HTTPException(400, "Старый формат .xls не поддерживается — сохраните файл как .xlsx или .csv")
    import csv, io
    text = None
    for enc in ("utf-8-sig", "cp1251"):
        try:
            text = data.decode(enc)
            break
        except UnicodeDecodeError:
            continue
    if text is None:
        raise HTTPException(400, "Не удалось прочитать файл")
    try:
        dialect = csv.Sniffer().sniff(text[:4000], delimiters=",;\t")
    except csv.Error:
        class dialect(csv.excel):  # свой класс, чтобы не менять общий csv.excel
            delimiter = ";" if text[:4000].count(";") > text[:4000].count(",") else ","
    return [[c.strip() for c in row] for row in csv.reader(io.StringIO(text), dialect)]


def _imp_find(header: list[str], keys: tuple) -> int:
    for i, h in enumerate(header):
        hl = h.lower()
        if any(k == hl or (len(k) > 3 and k in hl) for k in keys):
            return i
    return -1


@app.post("/api/v1/clients/import-file")
async def v1_import_clients_file(file: UploadFile = File(...), master_id: int = Depends(get_jwt_master_id)):
    """Разбирает файл Excel/CSV и возвращает список клиенток для предпросмотра. В базу НЕ пишет —
    мастер смотрит предпросмотр и подтверждает обычным /clients/import."""
    data = await file.read()
    if len(data) > 5 * 1024 * 1024:
        raise HTTPException(400, "Файл больше 5 МБ")
    rows = [r for r in _imp_rows(file.filename, data) if any(c for c in r)]
    if not rows:
        raise HTTPException(400, "Файл пустой")

    header = [c.lower() for c in rows[0]]
    i_phone = _imp_find(header, _IMP_PHONE)
    has_header = i_phone >= 0
    if has_header:
        i_last = _imp_find(header, _IMP_LAST)
        i_first = _imp_find([h if j != i_last else "" for j, h in enumerate(header)], _IMP_FIRST)
        i_name = _imp_find([h if j not in (i_last, i_first) else "" for j, h in enumerate(header)], _IMP_NAME)
        i_bday = _imp_find(header, _IMP_BDAY)
        i_notes = _imp_find(header, _IMP_NOTES)
        body_rows = rows[1:]
    else:
        # Без заголовка: телефон — столбец, где больше всего «похожих на номер» значений, имя — первый текстовый
        def phone_like(v):
            d = "".join(ch for ch in v if ch.isdigit())
            return 10 <= len(d) <= 12
        cols = max(len(r) for r in rows)
        score = [sum(1 for r in rows[:50] if j < len(r) and phone_like(r[j])) for j in range(cols)]
        i_phone = max(range(cols), key=lambda j: score[j])
        if score[i_phone] == 0:
            raise HTTPException(400, "Не нашли столбец с телефонами. Добавьте заголовки «Имя» и «Телефон»")
        i_name = next((j for j in range(cols) if j != i_phone and score[j] == 0), -1)
        i_last = i_first = i_bday = i_notes = -1
        body_rows = rows

    def col(r, i):
        return r[i] if 0 <= i < len(r) else ""

    out, bad = [], 0
    for r in body_rows[:5000]:
        if i_first >= 0 or i_last >= 0:
            name = " ".join(x for x in (col(r, i_first), col(r, i_last)) if x)
            if not name:
                name = col(r, i_name)
        else:
            name = col(r, i_name)
        digits = "".join(ch for ch in col(r, i_phone) if ch.isdigit())
        if len(digits) == 10:
            digits = "7" + digits
        if len(digits) == 11 and digits[0] == "8":
            digits = "7" + digits[1:]
        if not name.strip() or len(digits) != 11:
            bad += 1
            continue
        out.append({"name": name.strip()[:100], "phone": "+" + digits,
                    "birthday": _imp_bday(col(r, i_bday)), "notes": col(r, i_notes)[:500]})
    # Отмечаем, кто уже есть в базе мастера
    pool = await get_pool()
    async with pool.acquire() as conn:
        existing = {"".join(ch for ch in (x["phone"] or "") if ch.isdigit())[-10:]
                    for x in await conn.fetch("SELECT phone FROM clients WHERE master_id=$1", master_id)}
    for c in out:
        c["exists"] = c["phone"][-10:] in existing
    return {"clients": out, "invalid": bad, "total_rows": len(body_rows)}


# ── Apple Wallet: настройки карты мастера ────────────────────────────

class _WalletSettings(BaseModel):
    color: str = "berry"
    show_price: bool = True
    show_stamps: bool = True
    rules: str = ""
    address: str = ""


@app.get("/api/v1/wallet-settings")
async def v1_wallet_settings(master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        r = await conn.fetchrow(
            """SELECT COALESCE(wallet_color,'berry') AS color, COALESCE(wallet_show_price,TRUE) AS show_price,
                      COALESCE(wallet_show_stamps,TRUE) AS show_stamps, COALESCE(wallet_rules,'') AS rules,
                      COALESCE(wallet_address,'') AS address, COALESCE(name,'') AS name,
                      COALESCE(loyalty_discount_enabled,FALSE) AS loy_on, COALESCE(loyalty_threshold,10) AS loy_n,
                      COALESCE(loyalty_discount_type,'percent') AS loy_type,
                      COALESCE(loyalty_discount_percent,10) AS loy_pct, COALESCE(loyalty_discount_rub,0) AS loy_rub
               FROM masters WHERE id=$1""", master_id)
        cards = await conn.fetchval("SELECT COUNT(*) FROM wallet_passes WHERE master_id=$1", master_id)
    gift = f"−{r['loy_pct']}%" if r["loy_type"] != "rub" else f"−{r['loy_rub']} ₽"
    return {"color": r["color"], "show_price": r["show_price"], "show_stamps": r["show_stamps"],
            "rules": r["rules"], "address": r["address"], "master_name": r["name"],
            "colors": [{"key": k, "name": v["name"],
                        "bg": "#%02X%02X%02X" % v["bg"], "fg": "#%02X%02X%02X" % v["fg"],
                        "label": "#%02X%02X%02X" % v["label"], "strip": "#%02X%02X%02X" % v["strip"],
                        "d1": "#%02X%02X%02X" % v["d1"], "d2": "#%02X%02X%02X" % v["d2"]}
                       for k, v in wallet.PALETTES.items()],
            "loyalty_enabled": r["loy_on"], "loyalty_threshold": r["loy_n"], "loyalty_gift": gift,
            "cards_issued": cards or 0, "wallet_available": wallet.enabled()}


@app.put("/api/v1/wallet-settings")
async def v1_wallet_settings_update(body: _WalletSettings, master_id: int = Depends(get_jwt_master_id)):
    if body.color not in wallet.PALETTES:
        raise HTTPException(400, "Неизвестный цвет")
    if len(body.rules) > 500 or len(body.address) > 150:
        raise HTTPException(400, "Слишком длинный текст")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            """UPDATE masters SET wallet_color=$2, wallet_show_price=$3, wallet_show_stamps=$4,
                                  wallet_rules=$5, wallet_address=$6 WHERE id=$1""",
            master_id, body.color, body.show_price, body.show_stamps, body.rules.strip(), body.address.strip())
    await wallet.touch(master_id)  # карты у клиенток обновятся сами
    return {"ok": True}


@app.get("/api/v1/clients/{client_id}/wallet-link")
async def v1_client_wallet_link(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    """Ссылка на карту Apple Wallet для постоянной клиентки — мастер отправляет её сама."""
    if not wallet.enabled():
        raise HTTPException(503, "Apple Wallet пока недоступен")
    pool = await get_pool()
    async with pool.acquire() as conn:
        ok = await conn.fetchval("SELECT 1 FROM clients WHERE id=$1 AND master_id=$2", client_id, master_id)
    if not ok:
        raise HTTPException(404, "Клиентка не найдена")
    return {"url": await wallet.pass_link(master_id, client_id)}


# ── Волна 4: лист ожидания клиенток ──────────────────────────────────

class _WaitlistJoin(BaseModel):
    client_name: str
    client_phone: str
    date: str
    service_id: int | None = None
    procedure: str = ""
    duration: int = 0
    price: int = 0
    pref: str = "any"
    marketing_consent: bool = False


@app.post("/api/v1/book/{link}/waitlist", status_code=201)
async def v1_public_waitlist_join(link: str, body: _WaitlistJoin):
    """Клиентка встаёт в лист ожидания на занятый день (страница записи)."""
    master = await get_master_by_booking_link(link)
    if not master:
        raise HTTPException(404, "Мастер не найден")
    try:
        day = _dt.strptime(body.date, "%Y-%m-%d")
    except ValueError:
        raise HTTPException(400, "Неверная дата")
    _, tz, _ = await _master_slug_tz(master["id"])
    local_today = (_dt.utcnow() + _td(hours=tz)).date()
    if day.date() < local_today or (day.date() - local_today).days > 60:
        raise HTTPException(400, "Выберите дату в ближайшие 2 месяца")
    if body.date in await get_blocked_days(master["id"]):
        raise HTTPException(409, "В этот день мастер не работает")
    name = (body.client_name or "").strip()
    digits = "".join(ch for ch in body.client_phone if ch.isdigit())
    if len(name) < 2 or len(digits) != 11:
        raise HTTPException(400, "Укажите имя и телефон")
    pref = body.pref if body.pref in waitlist.PREFS else "any"

    procedure, price, duration = body.procedure, body.price, body.duration
    if body.service_id:
        for s in await get_services(master["id"]):
            if s[0] == body.service_id:
                procedure = procedure or s[1]
                price = price or (s[2] or 0)
                duration = duration or (s[3] if len(s) > 3 else 0)
                break
    procedure = procedure or "Запись"

    # Если подходящее окно уже есть — лист ожидания не нужен, пусть запишется сразу
    lo, hi = waitlist.PREFS[pref]
    if any(lo <= waitlist._to_min(x) < hi for x in await waitlist.free_slots(link, body.date, duration)):
        raise HTTPException(409, "На этот день есть свободное время — выберите его")

    client_id = await add_client(master["id"], name, body.client_phone)
    pool = await get_pool()
    async with pool.acquire() as conn:
        if body.marketing_consent:
            await conn.execute("UPDATE clients SET marketing_consent = TRUE WHERE id = $1", client_id)
        has_tg = bool(await conn.fetchval("SELECT telegram_id FROM clients WHERE id=$1", client_id))
        exists = await conn.fetchval(
            """SELECT id FROM client_waitlist WHERE master_id=$1 AND client_id=$2 AND date=$3
               AND status IN ('waiting','offered','master_notified')""", master["id"], client_id, body.date)
        if exists:
            return {"ok": True, "already": True, "has_telegram": has_tg, "bot_username": config.BOT_USERNAME}
        await conn.execute(
            """INSERT INTO client_waitlist (master_id, client_id, date, service_id, procedure, duration_min, price, pref)
               VALUES ($1,$2,$3,$4,$5,$6,$7,$8)""",
            master["id"], client_id, body.date, body.service_id, procedure, duration or 0, price or 0, pref)
    when = waitlist.human_date(body.date)
    text = f"{name} · {when} · {waitlist.PREF_LABELS[pref]} · {procedure}"
    try:
        await create_notification(master["id"], "waitlist", "🕐 Новая клиентка в листе ожидания", text)
        await push_to_master(master["id"], "Лист ожидания", text)
    except Exception as e:
        print(f"[WAITLIST] join notify error: {e}")
    return {"ok": True, "already": False, "has_telegram": has_tg, "bot_username": config.BOT_USERNAME}


@app.get("/api/v1/waitlist-clients")
async def v1_waitlist_list(date_from: str = "", date_to: str = "", master_id: int = Depends(get_jwt_master_id)):
    """Очередь мастера: кто ждёт окна и на какой день."""
    _, tz, _ = await _master_slug_tz(master_id)
    today = (_dt.utcnow() + _td(hours=tz)).strftime("%Y-%m-%d")
    d_from = date_from or today
    d_to = date_to or "9999-12-31"
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT w.id, w.date, w.pref, w.procedure, w.status, w.offered_time, w.created_at,
                      c.id AS client_id, c.name, c.phone, c.telegram_id
               FROM client_waitlist w JOIN clients c ON c.id = w.client_id
               WHERE w.master_id=$1 AND w.date BETWEEN $2 AND $3
                 AND w.status IN ('waiting','offered','master_notified')
               ORDER BY w.date, w.created_at""", master_id, d_from, d_to)
    return {"entries": [{
        "id": r["id"], "date": r["date"], "pref": r["pref"], "pref_label": waitlist.PREF_LABELS.get(r["pref"], ""),
        "procedure": r["procedure"], "status": r["status"], "offered_time": r["offered_time"],
        "client_id": r["client_id"], "client_name": r["name"], "client_phone": r["phone"],
        "has_telegram": bool(r["telegram_id"]), "created_at": r["created_at"].isoformat(),
    } for r in rows]}


@app.delete("/api/v1/waitlist-clients/{entry_id}")
async def v1_waitlist_remove(entry_id: int, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            "UPDATE client_waitlist SET status='removed' WHERE id=$1 AND master_id=$2", entry_id, master_id)
    if res.endswith(" 0"):
        raise HTTPException(404, "Не найдено")
    return {"ok": True}


# ── Волна 4: абонементы (раньше были только в Telegram-боте) ─────────

class _PassCreate(BaseModel):
    name: str
    total: int
    price: int = 0


def _pass_json(r) -> dict:
    total, used = r["total_sessions"] or 0, r["used_sessions"] or 0
    return {"id": r["id"], "client_id": r["client_id"], "name": r["name"], "total": total, "used": used,
            "remaining": max(total - used, 0), "price": r["price"] or 0,
            "created_at": r["created_at"].isoformat() if r["created_at"] else None}


async def _own_client(conn, client_id: int, master_id: int):
    row = await conn.fetchrow("SELECT id, name, telegram_id FROM clients WHERE id=$1 AND master_id=$2", client_id, master_id)
    if not row:
        raise HTTPException(404, "Клиентка не найдена")
    return row


@app.get("/api/v1/clients/{client_id}/passes")
async def v1_client_passes(client_id: int, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_client(conn, client_id, master_id)
        rows = await conn.fetch(
            "SELECT * FROM subscriptions WHERE client_id=$1 AND master_id=$2 ORDER BY created_at DESC", client_id, master_id)
    return {"passes": [_pass_json(r) for r in rows]}


@app.post("/api/v1/clients/{client_id}/passes", status_code=201)
async def v1_client_pass_create(client_id: int, body: _PassCreate, master_id: int = Depends(get_jwt_master_id)):
    name = (body.name or "").strip()
    if not name or len(name) > 80:
        raise HTTPException(400, "Название — от 1 до 80 символов")
    if not (1 <= body.total <= 100) or not (0 <= body.price <= 1_000_000):
        raise HTTPException(400, "Сеансов — от 1 до 100")
    pool = await get_pool()
    async with pool.acquire() as conn:
        await _own_client(conn, client_id, master_id)
        row = await conn.fetchrow(
            "INSERT INTO subscriptions (master_id, client_id, name, total_sessions, price) VALUES ($1,$2,$3,$4,$5) RETURNING *",
            master_id, client_id, name, body.total, body.price)
    return _pass_json(row)


async def _change_pass(pass_id: int, master_id: int, delta: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        if delta > 0:
            row = await conn.fetchrow(
                """UPDATE subscriptions SET used_sessions = used_sessions + 1
                   WHERE id=$1 AND master_id=$2 AND used_sessions < total_sessions RETURNING *""", pass_id, master_id)
        else:
            row = await conn.fetchrow(
                """UPDATE subscriptions SET used_sessions = used_sessions - 1
                   WHERE id=$1 AND master_id=$2 AND used_sessions > 0 RETURNING *""", pass_id, master_id)
        if not row:
            exists = await conn.fetchval("SELECT 1 FROM subscriptions WHERE id=$1 AND master_id=$2", pass_id, master_id)
            if not exists:
                raise HTTPException(404, "Абонемент не найден")
            raise HTTPException(409, "Сеансы закончились" if delta > 0 else "Нечего возвращать")
        client = await conn.fetchrow("SELECT telegram_id FROM clients WHERE id=$1", row["client_id"])
    return row, (client["telegram_id"] if client else None)


@app.post("/api/v1/passes/{pass_id}/use")
async def v1_pass_use(pass_id: int, master_id: int = Depends(get_jwt_master_id)):
    """Списать сеанс. Клиентке в Telegram — сколько осталось."""
    row, tg = await _change_pass(pass_id, master_id, +1)
    p = _pass_json(row)
    if tg:
        if p["remaining"] == 0:
            tail = "Это был последний сеанс 🙌"
        elif p["remaining"] == 1:
            tail = "Остался последний сеанс."
        else:
            tail = f"Осталось {p['remaining']} из {p['total']}."
        try:
            await bot.send_message(tg, f"📦 Абонемент «{p['name']}»: списан 1 сеанс. {tail}")
        except Exception as e:
            print(f"[PASS] client notify error: {e}")
    return p


@app.post("/api/v1/passes/{pass_id}/undo")
async def v1_pass_undo(pass_id: int, master_id: int = Depends(get_jwt_master_id)):
    """Вернуть ошибочно списанный сеанс."""
    row, _ = await _change_pass(pass_id, master_id, -1)
    return _pass_json(row)


@app.delete("/api/v1/passes/{pass_id}")
async def v1_pass_delete(pass_id: int, master_id: int = Depends(get_jwt_master_id)):
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute("DELETE FROM subscriptions WHERE id=$1 AND master_id=$2", pass_id, master_id)
    if res.endswith(" 0"):
        raise HTTPException(404, "Абонемент не найден")
    return {"ok": True}


# ── ЮКасса — платёжные эндпоинты ──────────────────────────────────────

PLANS = {
    "pro_1m":  {"price": "590.00",  "days": 30,  "label": "1 месяц"},
    "pro_6m":  {"price": "2990.00", "days": 180, "label": "6 месяцев"},
    "pro_1y":  {"price": "4790.00", "days": 365, "label": "12 месяцев"},
    "pro_2y":  {"price": "8390.00", "days": 730, "label": "24 месяца"},
    "biz_1m":  {"price": "990.00", "days": 30,  "label": "Бизнес 1 месяц"},
    "biz_6m":  {"price": "4990.00", "days": 180, "label": "Бизнес 6 месяцев"},
    "biz_1y":  {"price": "7990.00","days": 365, "label": "Бизнес 12 месяцев"},
    "biz_2y":  {"price": "13990.00","days": 730, "label": "Бизнес 24 месяца"},
    # Цена первых мастеров: навсегда для первых EARLY_LIMIT оплативших
    "early_1m": {"price": "299.00",  "days": 30,  "label": "Первые мастера · 1 месяц"},
    "early_1y": {"price": "2990.00", "days": 365, "label": "Первые мастера · 12 месяцев"},
}

EARLY_LIMIT = 100


async def _early_seats_left(conn) -> int:
    taken = await conn.fetchval("SELECT COUNT(*) FROM masters WHERE early_price = TRUE")
    return max(EARLY_LIMIT - int(taken or 0), 0)


async def _check_early_allowed(master_id: int, plan_key: str):
    """Тариф первых мастеров доступен, если мастер уже его получил или ещё есть места."""
    if not plan_key.startswith("early_"):
        return
    pool = await get_pool()
    async with pool.acquire() as conn:
        is_member = await conn.fetchval("SELECT COALESCE(early_price, FALSE) FROM masters WHERE id=$1", master_id)
        if is_member:
            return
        if await _early_seats_left(conn) <= 0:
            raise HTTPException(409, "Места по цене первых мастеров закончились")


@app.get("/api/v1/early/seats")
async def early_seats():
    pool = await get_pool()
    async with pool.acquire() as conn:
        left = await _early_seats_left(conn)
    return {"limit": EARLY_LIMIT, "left": left}


class CreatePaymentRequest(BaseModel):
    plan: str = "pro_1m"


import yookassa
from yookassa import Payment, Configuration
import uuid

Configuration.account_id = config.YUKASSA_SHOP_ID
Configuration.secret_key = config.YUKASSA_SECRET_KEY


@app.post("/api/v1/payment/create")
async def create_payment(body: CreatePaymentRequest, master_id: int = Depends(get_jwt_master_id_any)):
    if not config.YUKASSA_SHOP_ID or not config.YUKASSA_SECRET_KEY:
        raise HTTPException(503, "Платёжная система не настроена")
    await _check_early_allowed(master_id, body.plan)
    plan = PLANS.get(body.plan, PLANS["pro_1m"])
    payment = Payment.create({
        "amount": {"value": plan["price"], "currency": "RUB"},
        "confirmation": {
            "type": "redirect",
            "return_url": f"{WEBHOOK_URL.rstrip('/')}/payment/success",
        },
        "capture": True,
        "description": f"Подписка Solvo Beauty — мастер #{master_id}, {plan['label']}",
        "metadata": {"master_id": str(master_id), "plan": body.plan, "days": str(plan["days"])},
    }, str(uuid.uuid4()))
    return {
        "payment_id": payment.id,
        "confirmation_url": payment.confirmation.confirmation_url,
    }


class WebPaymentRequest(BaseModel):
    email: str
    password: str
    plan: str = "pro_1m"


class _AccountWebInfo(BaseModel):
    email: str
    password: str

@app.post("/api/v1/account/web-info")
async def account_web_info(body: _AccountWebInfo):
    master = await get_master_by_email(body.email)
    if not master:
        raise HTTPException(401, "Неверный email или пароль")
    stored = master.get("password_hash") or ""
    try:
        ok = bcrypt.checkpw(body.password.encode(), stored.encode())
    except ValueError:
        ok = stored == hashlib.sha256(body.password.encode()).hexdigest()
    if not ok:
        raise HTTPException(401, "Неверный email или пароль")

    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT is_active, paid_until, trial_end_date, name FROM masters WHERE id=$1",
            master["id"]
        )
        payments = await conn.fetch(
            "SELECT plan, amount, status, paid_at FROM payment_history WHERE master_id=$1 ORDER BY paid_at DESC LIMIT 20",
            master["id"]
        )
    return {
        "name": row["name"] or "",
        "is_active": bool(row["is_active"]) if row["is_active"] is not None else False,
        "paid_until": str(row["paid_until"]) if row["paid_until"] else None,
        "trial_end_date": str(row["trial_end_date"]) if row["trial_end_date"] else None,
        "payments": [
            {
                "plan": p["plan"],
                "amount": float(p["amount"]) if p["amount"] else 0,
                "status": p["status"],
                "paid_at": str(p["paid_at"]) if p["paid_at"] else None,
            }
            for p in payments
        ]
    }

@app.post("/api/v1/payment/create-web")
async def create_payment_web(body: WebPaymentRequest):
    master = await get_master_by_email(body.email)
    if not master:
        raise HTTPException(401, "Неверный email или пароль")
    stored = master.get("password_hash") or ""
    try:
        ok = bcrypt.checkpw(body.password.encode(), stored.encode())
    except ValueError:
        ok = stored == hashlib.sha256(body.password.encode()).hexdigest()
    if not ok:
        raise HTTPException(401, "Неверный email или пароль")
    if not config.YUKASSA_SHOP_ID or not config.YUKASSA_SECRET_KEY:
        raise HTTPException(503, "Платёжная система не настроена")
    await _check_early_allowed(master["id"], body.plan)
    plan = PLANS.get(body.plan, PLANS["pro_1m"])
    payment = Payment.create({
        "amount": {"value": plan["price"], "currency": "RUB"},
        "confirmation": {
            "type": "redirect",
            "return_url": "https://solvobeauty.vercel.app/pay.html?status=success",
        },
        "capture": True,
        "description": f"Подписка Solvo Beauty — мастер #{master['id']}, {plan['label']}",
        "metadata": {"master_id": str(master["id"]), "plan": body.plan, "days": str(plan["days"])},
    }, str(uuid.uuid4()))
    return {"confirmation_url": payment.confirmation.confirmation_url}


@app.post("/api/v1/payment/webhook")
async def payment_webhook(request: Request):
    body = await request.json()
    event = body.get("event")
    obj = body.get("object", {})
    if event == "payment.succeeded":
        # Не доверяем телу запроса: перепроверяем платёж напрямую в ЮКассе
        try:
            real = Payment.find_one(obj.get("id", ""))
            if real.status != "succeeded":
                return {"status": "ignored"}
            obj = {"metadata": dict(real.metadata or {}), "amount": {"value": real.amount.value}}
        except Exception as e:
            print(f"[PAYMENT] verify error: {e}")
            raise HTTPException(400, "Платёж не подтверждён")
        meta = obj.get("metadata", {})
        master_id = int(meta.get("master_id", 0))
        days = int(meta.get("days", 30))
        plan = meta.get("plan", "pro_1m")
        if master_id:
            pool = await get_pool()
            async with pool.acquire() as conn:
                from datetime import timedelta, datetime
                # продлеваем от текущей даты окончания, если она ещё в будущем
                cur = await conn.fetchval("SELECT paid_until FROM masters WHERE id=$1", master_id)
                start = cur if cur and cur > datetime.utcnow() else datetime.utcnow()
                await conn.execute(
                    "UPDATE masters SET is_active = 1, trial_end_date = NULL, paid_until = $1 WHERE id = $2",
                    start + timedelta(days=days),
                    master_id,
                )
                if plan.startswith("early_"):
                    await conn.execute("UPDATE masters SET early_price = TRUE WHERE id=$1", master_id)
                amount = obj.get("amount", {}).get("value", "0")
                await conn.execute(
                    "INSERT INTO payment_history (master_id, plan, amount, paid_at) VALUES ($1, $2, $3, NOW())",
                    master_id, plan, amount
                )
                name = await conn.fetchval("SELECT name FROM masters WHERE id=$1", master_id)
            await send_telegram(
                f"💳 Оплата подписки от мастера {name or '#' + str(master_id)}\n"
                f"Подписка продлена на {days} дней."
            )
    return {"status": "ok"}


# ── Waitlist ────────────────────────────────────────────────────────────

class WaitlistRequest(BaseModel):
    email: str


@app.post("/api/v1/waitlist")
async def join_waitlist(body: WaitlistRequest):
    try:
        async with get_pool().acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS waitlist (
                    id SERIAL PRIMARY KEY,
                    email TEXT UNIQUE NOT NULL,
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            result = await conn.execute(
                "INSERT INTO waitlist (email) VALUES ($1) ON CONFLICT (email) DO NOTHING",
                body.email
            )
            is_new = "INSERT 0 1" in result

        if is_new:
            await send_telegram(
                f"🎉 Новая заявка на лендинге!\n📧 {body.email}"
            )

        return {"ok": True, "is_new": is_new}
    except Exception as e:
        raise HTTPException(500, str(e))


@app.get("/api/v1/waitlist/list")
async def get_waitlist_list(secret: str = ""):
    if secret != ADMIN_SECRET:
        raise HTTPException(403, "Forbidden")
    from database import get_waitlist
    items = await get_waitlist()
    return {"count": len(items), "emails": items}


# ── Личные заметки ────────────────────────────────────────────────────

class PersonalNoteCreate(BaseModel):
    date: str
    time: str
    text: str


@app.get("/api/v1/notes")
async def api_get_notes(date: str, master_id: int = Depends(get_jwt_master_id)):
    notes = await get_personal_notes(master_id, date)
    return {"notes": notes}


@app.post("/api/v1/notes")
async def api_create_note(body: PersonalNoteCreate, master_id: int = Depends(get_jwt_master_id)):
    note_id = await create_personal_note(master_id, body.date, body.time, body.text)
    return {"id": note_id, "ok": True}


@app.delete("/api/v1/notes/{note_id}")
async def api_delete_note(note_id: int, master_id: int = Depends(get_jwt_master_id)):
    deleted = await delete_personal_note(master_id, note_id)
    if not deleted:
        raise HTTPException(404, "Заметка не найдена")
    return {"ok": True}
