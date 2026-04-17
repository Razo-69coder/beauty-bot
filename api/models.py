from pydantic import BaseModel
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
