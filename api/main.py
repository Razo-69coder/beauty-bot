import io
import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, Header, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from dotenv import load_dotenv

from auth import validate_init_data
from database import (
    init_db,
    get_or_create_master, get_clients_page, search_clients,
    get_client, add_client, update_client, delete_client,
    get_client_history, add_appointment, get_inactive_clients,
    get_statistics, get_reminder_days, update_reminder_days,
)
from models import ClientCreate, ClientUpdate, AppointmentCreate, ReminderUpdate

load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    yield


app = FastAPI(title="Beauty Book API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

PAGE_SIZE = 10


# ─── Авторизация ─────────────────────────────────────────────────
async def get_telegram_id(
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
) -> int:
    """
    В режиме разработки передаём x-dev-telegram-id прямо в заголовке.
    В продакшене используем настоящий initData от Telegram.
    """
    if x_dev_telegram_id:
        return int(x_dev_telegram_id)

    if not x_init_data:
        raise HTTPException(status_code=401, detail="Требуется авторизация")

    bot_token = os.getenv("BOT_TOKEN")
    user = validate_init_data(x_init_data, bot_token)
    if not user:
        raise HTTPException(status_code=401, detail="Неверные данные авторизации")

    return user["id"]


async def get_master_id(
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
    x_dev_username: str = Header(None),
) -> int:
    telegram_id = await get_telegram_id(x_init_data, x_dev_telegram_id)
    name = x_dev_username or "Мастер"
    return await get_or_create_master(telegram_id, name)


# ─── Мастер ──────────────────────────────────────────────────────
@app.get("/api/master")
async def master_info(
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
):
    telegram_id = await get_telegram_id(x_init_data, x_dev_telegram_id)
    reminder_days = await get_reminder_days(telegram_id)
    return {"telegram_id": telegram_id, "reminder_days": reminder_days}


# ─── Клиенты ─────────────────────────────────────────────────────
@app.get("/api/clients")
async def clients_list(
    page: int = 0,
    search: str = "",
    master_id: int = Depends(get_master_id),
):
    if search:
        results = await search_clients(master_id, search)
        return {"clients": results, "total": len(results), "page": 0, "pages": 1}

    clients, total = await get_clients_page(master_id, page, PAGE_SIZE)
    pages = max(1, (total + PAGE_SIZE - 1) // PAGE_SIZE)
    return {"clients": clients, "total": total, "page": page, "pages": pages}


@app.post("/api/clients", status_code=201)
async def create_client(
    body: ClientCreate,
    master_id: int = Depends(get_master_id),
):
    client_id = await add_client(master_id, body.name, body.phone, body.notes)
    return {"id": client_id}


@app.get("/api/clients/{client_id}")
async def client_detail(client_id: int, master_id: int = Depends(get_master_id)):
    client = await get_client(client_id)
    if not client:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    history = await get_client_history(client_id)
    return {**client, "history": history}


@app.put("/api/clients/{client_id}")
async def edit_client(
    client_id: int,
    body: ClientUpdate,
    master_id: int = Depends(get_master_id),
):
    ok = await update_client(client_id, master_id, body.name, body.phone, body.notes)
    if not ok:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True}


@app.delete("/api/clients/{client_id}")
async def remove_client(client_id: int, master_id: int = Depends(get_master_id)):
    ok = await delete_client(client_id, master_id)
    if not ok:
        raise HTTPException(status_code=404, detail="Клиент не найден")
    return {"ok": True}


# ─── Процедуры ───────────────────────────────────────────────────
@app.post("/api/appointments", status_code=201)
async def create_appointment(
    body: AppointmentCreate,
    master_id: int = Depends(get_master_id),
):
    appt_id = await add_appointment(
        body.client_id, master_id, body.procedure,
        body.appointment_date, body.price, body.notes, body.photo_id,
    )
    return {"id": appt_id}


# ─── Неактивные клиенты ──────────────────────────────────────────
@app.get("/api/inactive")
async def inactive_clients(
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
    master_id: int = Depends(get_master_id),
):
    telegram_id = await get_telegram_id(x_init_data, x_dev_telegram_id)
    days = await get_reminder_days(telegram_id)
    clients = await get_inactive_clients(master_id, days)
    return {"clients": clients, "reminder_days": days}


# ─── Статистика ──────────────────────────────────────────────────
@app.get("/api/stats")
async def stats(master_id: int = Depends(get_master_id)):
    return await get_statistics(master_id)


# ─── Настройки ───────────────────────────────────────────────────
@app.put("/api/settings/reminder")
async def set_reminder(
    body: ReminderUpdate,
    x_init_data: str = Header(None),
    x_dev_telegram_id: str = Header(None),
):
    telegram_id = await get_telegram_id(x_init_data, x_dev_telegram_id)
    await update_reminder_days(telegram_id, body.days)
    return {"ok": True, "days": body.days}


# ─── Экспорт в Excel ─────────────────────────────────────────────
@app.get("/api/export")
async def export_excel(master_id: int = Depends(get_master_id)):
    try:
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment

        clients, _ = await get_clients_page(master_id, 0, 10000)

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Клиенты"

        headers = ["Имя", "Телефон", "Заметка", "Последний визит", "Кол-во процедур", "Выручка (₽)"]
        ws.append(headers)
        for cell in ws[1]:
            cell.font = Font(bold=True)
            cell.fill = PatternFill("solid", fgColor="FFD9D9")
            cell.alignment = Alignment(horizontal="center")

        for client in clients:
            history = await get_client_history(client["id"])
            total_price = sum(h["price"] for h in history if h["price"])
            ws.append([
                client["name"], client["phone"], client["notes"] or "",
                client["last_visit"][:10] if client["last_visit"] else "нет визитов",
                len(history), total_price,
            ])

        for col in ws.columns:
            max_len = max(len(str(cell.value or "")) for cell in col)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        return StreamingResponse(
            buf,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={"Content-Disposition": "attachment; filename=beauty_clients.xlsx"},
        )
    except ImportError:
        raise HTTPException(status_code=500, detail="Установи openpyxl: pip install openpyxl")
