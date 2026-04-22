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
import jwt
import httpx
from datetime import datetime as _dt, timedelta as _td

from database import (
    init_db, get_or_create_master, get_clients, get_client,
    get_client_history, get_statistics, add_client, update_client, update_client_username,
    delete_client, add_appointment, get_inactive_clients,
    get_reminder_days, update_reminder_days,
    get_reminder_days_by_master, update_reminder_days_by_master,
    get_master_info, get_master_info_by_telegram, get_available_slots,
    get_master_schedule, update_appointment_status,
    create_login_code, verify_login_code, verify_login_code_by_code,
    get_master_full, update_master_full_settings, update_master_payment, update_master_timezone,
    search_clients,
    get_services, add_service, delete_service,
    get_earnings_by_service, get_earnings_by_client, get_earnings_by_day, get_earnings_by_period,
    get_appointment_with_client, update_appointment_service_done,
)

from scheduler import setup_scheduler
from handlers import start, clients, appointments, settings, stats, services
from handlers import booking, schedule, subscriptions, templates, reviews, deposit, fallback


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
    dp.include_router(fallback.router)  # всегда последним
    return dp


bot = Bot(token=BOT_TOKEN)
dp = build_dispatcher()

# ── Lifespan ──────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    
    # Миграции: добавить отсутствующие колонки
    from database import get_pool
    pool = await get_pool()
    async with pool.acquire() as conn:
        try:
            await conn.execute("ALTER TABLE clients ADD COLUMN username VARCHAR(50) DEFAULT ''")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE masters ADD COLUMN timezone VARCHAR(50) DEFAULT 'Europe/Moscow'")
        except Exception:
            pass
        try:
            await conn.execute("ALTER TABLE clients ADD COLUMN timezone VARCHAR(50) DEFAULT 'Europe/Moscow'")
        except Exception:
            pass
    
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


# ── Health checks (для keep-alive) ─────────────────────────────────────
@app.get("/")
async def root():
    return {"status": "ok", "message": "Beauty Book v4", "v": 4}

@app.get("/api/all-masters")
async def get_all():
    """Публичный список мастеров"""
    try:
        masters = await get_all_masters()
    except Exception as e:
        return {"error": str(e), "masters": []}
    return {"masters": masters}


@app.get("/health")
async def health():
    return {"status": "healthy"}


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


# ── JWT утилиты (определены здесь — до всех JWT-эндпоинтов) ──────────

def _jwt_secret() -> str:
    return BOT_TOKEN or "beauty_fallback_secret"

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

async def get_jwt_master_id(authorization: str = Header(None)) -> int:
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

class ClientUpdate(BaseModel):
    name: str | None = None
    phone: str | None = None
    notes: str | None = None
    username: str | None = None

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
    payment_card: str


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
        body.notes or client.get("notes", ""))
    if body.username is not None:
        await update_client_username(client_id, master_id, body.username)
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
        "id": appt["id"], "client": appt["client_name"],
        "phone": "", "procedure": appt["procedure"],
        "date": appt["appointment_date"], "time": appt["time"] or "",
        "status": "confirmed", "price": appt["deposit_amount"],
        "notes": "", "service_done_at": None,
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
    await update_master_payment(master_id, body.payment_card)
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

# ── Остальная статика (webapp) ────────────────────────────────────────
app.mount("/app", StaticFiles(directory="webapp", html=True), name="webapp")
