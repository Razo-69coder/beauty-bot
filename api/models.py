from pydantic import BaseModel
from typing import Optional
from typing import Optional


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
    appointment_date: str  # формат YYYY-MM-DD
    price: int = 0
    notes: str = ""
    photo_id: str = ""


class ReminderUpdate(BaseModel):
    days: int


# ── Публичная запись ──────────────────────────────────────────────

class PublicBooking(BaseModel):
    master_id: int
    client_name: str
    client_phone: str
    procedure: str
    appointment_date: str
    appointment_time: str
    client_birthday: Optional[str] = None
    notes: Optional[str] = None


# ── Авторизация в веб-панели ──────────────────────────────────────

class RequestCode(BaseModel):
    telegram_id: int


class VerifyCode(BaseModel):
    telegram_id: int
    code: str


# ── Настройки мастера ─────────────────────────────────────────────

class MasterSettings(BaseModel):
    name: str
    work_start: int = 10
    work_end: int = 20
    slot_duration: int = 60
    reminder_days: int = 40


class PaymentUpdate(BaseModel):
    payment_card: str


class DashboardAppointmentCreate(BaseModel):
    client_id: int
    procedure: str
    appointment_date: str
    time: str = ""
    price: int = 0
    notes: str = ""


# ── Email/Password авторизация ───────────────────────────────────

class EmailRegisterRequest(BaseModel):
    email: str
    password: str
    name: str


class EmailLoginRequest(BaseModel):
    email: str
    password: str
