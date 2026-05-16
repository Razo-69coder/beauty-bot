import aiosqlite
import os
import random
import string
from datetime import datetime, timedelta, date

# Локально: ../beauty_bot.db (рядом с ботом). На Render: DATABASE_PATH или ./beauty_bot.db
DB_PATH = os.getenv(
    "DATABASE_PATH",
    os.path.join(os.path.dirname(__file__), "..", "beauty_bot.db")
)


async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                name TEXT,
                reminder_days INTEGER DEFAULT 40,
                work_start INTEGER DEFAULT 10,
                work_end INTEGER DEFAULT 20,
                slot_duration INTEGER DEFAULT 60,
                payment_card TEXT DEFAULT ''
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                phone TEXT,
                notes TEXT,
                telegram_id BIGINT,
                username TEXT DEFAULT '',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER NOT NULL,
                master_id INTEGER NOT NULL,
                procedure TEXT,
                appointment_date TEXT,
                time TEXT DEFAULT '',
                price INTEGER DEFAULT 0,
                notes TEXT,
                photo_id TEXT,
                status TEXT DEFAULT 'confirmed',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS login_codes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER NOT NULL,
                code TEXT NOT NULL,
                expires_at TEXT NOT NULL,
                used INTEGER DEFAULT 0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS blocked_days (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER NOT NULL,
                date TEXT NOT NULL,
                UNIQUE(master_id, date)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS reminder_templates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                template TEXT NOT NULL DEFAULT '',
                UNIQUE(master_id, type)
            )
        """)
        # Миграции для существующих баз
        for migration in [
            "ALTER TABLE masters ADD COLUMN reminder_days INTEGER DEFAULT 40",
            "ALTER TABLE masters ADD COLUMN work_start INTEGER DEFAULT 10",
            "ALTER TABLE masters ADD COLUMN work_end INTEGER DEFAULT 20",
            "ALTER TABLE masters ADD COLUMN slot_duration INTEGER DEFAULT 60",
            "ALTER TABLE masters ADD COLUMN payment_card TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN payment_phone TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN payment_banks TEXT DEFAULT ''",
            "ALTER TABLE appointments ADD COLUMN time TEXT DEFAULT ''",
            "ALTER TABLE appointments ADD COLUMN status TEXT DEFAULT 'confirmed'",
            "ALTER TABLE appointments ADD COLUMN service_done_at TEXT",
            "ALTER TABLE appointments ADD COLUMN review_requested_at TEXT",
            "ALTER TABLE masters ADD COLUMN email TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN password_hash TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN theme TEXT DEFAULT 'pink'",
            "ALTER TABLE masters ADD COLUMN booking_link TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN is_active INTEGER DEFAULT 1",
            "ALTER TABLE masters ADD COLUMN specialization TEXT DEFAULT ''",
            "ALTER TABLE clients ADD COLUMN source TEXT DEFAULT ''",
            "ALTER TABLE clients ADD COLUMN allergies TEXT DEFAULT ''",
            "ALTER TABLE reminder_templates ADD COLUMN enabled INTEGER DEFAULT 1",
        ]:
            try:
                await db.execute(migration)
            except Exception:
                pass
        await db.commit()


async def get_or_create_master(telegram_id: int, name: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id = ?", (telegram_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return row[0]
        await db.execute(
            "INSERT INTO masters (telegram_id, name, is_active) VALUES (?, ?, 1)", (telegram_id, name)
        )
        await db.commit()
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id = ?", (telegram_id,)
        ) as cursor:
            return (await cursor.fetchone())[0]


async def get_clients_page(master_id: int, page: int, page_size: int) -> tuple[list, int]:
    """Возвращает (список клиентов на странице, общее кол-во)"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM clients WHERE master_id=?", (master_id,)
        ) as c:
            total = (await c.fetchone())[0]

        async with db.execute("""
            SELECT c.id, c.name, c.phone, c.notes,
                   MAX(a.appointment_date) as last_visit
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = ?
            GROUP BY c.id
            ORDER BY c.name
            LIMIT ? OFFSET ?
        """, (master_id, page_size, page * page_size)) as cursor:
            rows = await cursor.fetchall()

    clients = [
        {"id": r[0], "name": r[1], "phone": r[2], "notes": r[3], "last_visit": r[4]}
        for r in rows
    ]
    return clients, total


async def search_clients(master_id: int, query: str) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT c.id, c.name, c.phone, c.notes,
                   MAX(a.appointment_date) as last_visit
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = ?
            GROUP BY c.id
            ORDER BY c.name
        """, (master_id,)) as cursor:
            all_clients = await cursor.fetchall()

    query_lower = query.lower()
    filtered = [r for r in all_clients if query_lower in r[1].lower()]
    return [
        {"id": r[0], "name": r[1], "phone": r[2], "notes": r[3], "last_visit": r[4]}
        for r in filtered
    ]


async def get_client(client_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, phone, notes, telegram_id, source, allergies FROM clients WHERE id=?", (client_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {"id": row[0], "name": row[1], "phone": row[2], "notes": row[3], "telegram_id": row[4], "source": row[5] or "", "allergies": row[6] or ""}


async def get_client_by_phone(master_telegram_id: int, phone: str) -> dict | None:
    """Получить клиента по номеру телефона"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT m.id FROM masters WHERE telegram_id=?", (master_telegram_id,)
        ) as c:
            row = await c.fetchone()
        if not row:
            return None
        master_id = row[0]
        
        async with db.execute(
            "SELECT id, name, phone, notes, telegram_id FROM clients WHERE master_id=? AND phone=?", (master_id, phone)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {"id": row[0], "name": row[1], "phone": row[2], "notes": row[3], "telegram_id": row[4]}


async def add_client(master_id: int, name: str, phone: str, notes: str = "", source: str = "", allergies: str = "") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO clients (master_id, name, phone, notes, source, allergies) VALUES (?, ?, ?, ?, ?, ?)",
            (master_id, name, phone, notes, source, allergies)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            return (await cursor.fetchone())[0]


async def update_client(client_id: int, master_id: int, name: str, phone: str, notes: str, source: str = "", allergies: str = "") -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        result = await db.execute(
            "UPDATE clients SET name=?, phone=?, notes=?, source=?, allergies=? WHERE id=? AND master_id=?",
            (name, phone, notes, source, allergies, client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0


async def delete_client(client_id: int, master_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM appointments WHERE client_id=?", (client_id,))
        result = await db.execute(
            "DELETE FROM clients WHERE id=? AND master_id=?", (client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0


async def get_client_history(client_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, procedure, appointment_date, price, notes, photo_id
            FROM appointments
            WHERE client_id=?
            ORDER BY appointment_date DESC
        """, (client_id,)) as cursor:
            rows = await cursor.fetchall()
    return [
        {"id": r[0], "procedure": r[1], "date": r[2], "price": r[3], "notes": r[4], "photo_id": r[5]}
        for r in rows
    ]


async def add_appointment(
    client_id: int, master_id: int, procedure: str,
    appointment_date: str, price: int = 0,
    notes: str = "", photo_id: str = ""
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO appointments
            (client_id, master_id, procedure, appointment_date, price, notes, photo_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (client_id, master_id, procedure, appointment_date, price, notes, photo_id))
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            return (await cursor.fetchone())[0]


async def get_inactive_clients(master_id: int, days: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT c.id, c.name, c.phone,
                   MAX(a.appointment_date) as last_visit,
                   CAST(julianday('now') - julianday(MAX(a.appointment_date)) AS INTEGER) as days_ago
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = ?
            GROUP BY c.id
            HAVING last_visit IS NOT NULL AND days_ago >= ?
            ORDER BY days_ago DESC
        """, (master_id, days)) as cursor:
            rows = await cursor.fetchall()
    return [
        {"id": r[0], "name": r[1], "phone": r[2], "last_visit": r[3], "days_ago": r[4]}
        for r in rows
    ]


async def get_statistics(master_id: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*) FROM clients WHERE master_id=?", (master_id,)
        ) as c:
            total_clients = (await c.fetchone())[0]

        async with db.execute(
            "SELECT COUNT(*), COALESCE(SUM(price), 0) FROM appointments WHERE master_id=?",
            (master_id,)
        ) as c:
            row = await c.fetchone()
            total_appointments, total_earnings = row

        async with db.execute(
            "SELECT COALESCE(SUM(price), 0) FROM appointments "
            "WHERE master_id=? AND strftime('%Y-%m', appointment_date) = strftime('%Y-%m', 'now')",
            (master_id,)
        ) as c:
            month_earnings = (await c.fetchone())[0]

        async with db.execute(
            "SELECT procedure, COUNT(*) as cnt FROM appointments "
            "WHERE master_id=? GROUP BY procedure ORDER BY cnt DESC LIMIT 5",
            (master_id,)
        ) as c:
            top_procedures = [{"procedure": r[0], "count": r[1]} for r in await c.fetchall()]

    return {
        "total_clients": total_clients,
        "total_appointments": total_appointments,
        "total_earnings": total_earnings,
        "month_earnings": month_earnings,
        "top_procedures": top_procedures,
    }


async def get_yearly_stats(master_id: int, year: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT COUNT(*), COALESCE(SUM(price), 0) FROM appointments "
            "WHERE master_id=? AND strftime('%Y', appointment_date) = ?",
            (master_id, str(year))
        ) as c:
            row = await c.fetchone()
            total_appointments, total_revenue = row

        async with db.execute(
            "SELECT procedure, COUNT(*) as cnt FROM appointments "
            "WHERE master_id=? AND strftime('%Y', appointment_date) = ? "
            "GROUP BY procedure ORDER BY cnt DESC LIMIT 5",
            (master_id, str(year))
        ) as c:
            top_services = [{"procedure": r[0], "count": r[1]} for r in await c.fetchall()]

    return {
        "total_revenue": total_revenue,
        "total_appointments": total_appointments,
        "top_services": top_services,
    }


async def get_reminder_days(telegram_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT reminder_days FROM masters WHERE telegram_id=?", (telegram_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0] if row and row[0] else 40


async def update_reminder_days(telegram_id: int, days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET reminder_days=? WHERE telegram_id=?", (days, telegram_id)
        )
        await db.commit()


# ── Настройки мастера ─────────────────────────────────────────────────

async def get_master_full(master_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, telegram_id, name, reminder_days, work_start, work_end, "
            "slot_duration, payment_card, payment_phone, payment_banks, is_active, specialization "
            "FROM masters WHERE id=?", (master_id,)
        ) as c:
            row = await c.fetchone()
    if not row:
        return None
    return {
        "id": row[0],
        "telegram_id": row[1],
        "name": row[2],
        "reminder_days": row[3] or 40,
        "work_start": row[4] or 10,
        "work_end": row[5] or 20,
        "slot_duration": row[6] or 60,
        "payment_card": row[7] or "",
        "payment_phone": row[8] or "",
        "payment_banks": row[9] or "",
        "is_active": bool(row[10]) if row[10] is not None else True,
        "specialization": row[11] or "",
    }


async def update_master_settings(master_id: int, work_start: int, work_end: int,
                                  slot_duration: int, reminder_days: int, name: str,
                                  specialization: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET work_start=?, work_end=?, slot_duration=?, reminder_days=?, name=?, specialization=? WHERE id=?",
            (work_start, work_end, slot_duration, reminder_days, name, specialization, master_id)
        )
        await db.commit()


async def update_master_payment(master_id: int, payment_card: str = "", payment_phone: str = "", payment_banks: str = ""):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET payment_card=?, payment_phone=?, payment_banks=? WHERE id=?",
            (payment_card, payment_phone, payment_banks, master_id)
        )
        await db.commit()


async def get_master_by_email(email: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, email, work_start, work_end, slot_duration, "
            "reminder_days, payment_card, payment_phone, payment_banks, "
            "theme, password_hash "
            "FROM masters WHERE email = ?", (email,)
        ) as c:
            row = await c.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1] or "", "email": row[2] or "",
        "work_start": row[3] or 9, "work_end": row[4] or 20,
        "slot_duration": row[5] or 60, "timezone": "Europe/Moscow",
        "reminder_days": row[6] or 30,
        "payment_card": row[7] or "", "payment_phone": row[8] or "",
        "payment_banks": row[9] or "", "deposit_enabled": False,
        "deposit_percent": 30, "theme": row[10] or "pink",
        "password_hash": row[11] or "",
    }


async def create_master_with_email(email: str, password_hash: str, name: str) -> int:
    import time
    telegram_id = -int(time.time())
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO masters (email, password_hash, name, telegram_id, is_active) VALUES (?, ?, ?, ?, 1)",
            (email, password_hash, name, telegram_id)
        )
        await db.commit()
        return cursor.lastrowid


# ── Публичное расписание (для страницы записи) ────────────────────────

async def get_master_public_info(telegram_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, work_start, work_end, slot_duration FROM masters WHERE telegram_id=?",
            (telegram_id,)
        ) as c:
            row = await c.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1],
        "work_start": row[2] or 10, "work_end": row[3] or 20, "slot_duration": row[4] or 60,
    }


async def get_master_by_booking_link(link: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, work_start, work_end, slot_duration, booking_link "
            "FROM masters WHERE booking_link = ?", (link,)
        ) as c:
            row = await c.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "name": row[1],
        "work_start": row[2] or 10, "work_end": row[3] or 20,
        "slot_duration": row[4] or 60, "booking_link": row[5] or "",
    }


async def update_booking_link(master_id: int, link: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET booking_link = ? WHERE id = ?", (link, master_id)
        )
        await db.commit()


async def set_master_active(master_id: int, is_active: bool) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET is_active = ? WHERE id = ?",
            (1 if is_active else 0, master_id)
        )
        await db.commit()


async def get_booking_link(master_id: int) -> str:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT booking_link FROM masters WHERE id = ?", (master_id,)
        ) as c:
            row = await c.fetchone()
    return (row[0] or "") if row else ""


async def booking_link_exists(link: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM masters WHERE booking_link = ?", (link,)
        ) as c:
            return await c.fetchone() is not None


async def get_busy_slots(master_id: int, date_str: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT time FROM appointments WHERE master_id=? AND appointment_date=? AND status != 'cancelled'",
            (master_id, date_str)
        ) as c:
            rows = await c.fetchall()
    return [r[0] for r in rows if r[0]]


async def get_available_slots(master_id: int, date_str: str,
                               work_start: int, work_end: int, slot_duration: int) -> list[str]:
    busy = await get_busy_slots(master_id, date_str)
    slots = []
    total_min = work_start * 60
    end_min = work_end * 60
    while total_min + slot_duration <= end_min:
        h, m = divmod(total_min, 60)
        t = f"{h:02d}:{m:02d}"
        if t not in busy:
            slots.append(t)
        total_min += slot_duration
    return slots


async def get_available_dates(master_id: int, work_start: int,
                                work_end: int, slot_duration: int, days: int = 60) -> list:
    today = date.today()
    blocked = await get_blocked_days(master_id)
    result = []
    for i in range(days):
        d = (today + timedelta(days=i)).isoformat()
        if d in blocked:
            continue
        slots = await get_available_slots(master_id, d, work_start, work_end, slot_duration)
        if slots:
            result.append(d)
    return result


async def public_book(master_telegram_id: int, date_str: str, time_str: str,
                       client_name: str, client_phone: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id=?", (master_telegram_id,)
        ) as c:
            row = await c.fetchone()
        if not row:
            raise ValueError("Мастер не найден")
        master_id = row[0]

        # Проверяем что слот свободен
        async with db.execute(
            "SELECT id FROM appointments WHERE master_id=? AND appointment_date=? AND time=? AND status != 'cancelled'",
            (master_id, date_str, time_str)
        ) as c:
            if await c.fetchone():
                raise ValueError("Это время уже занято")

        # Ищем или создаём клиента
        async with db.execute(
            "SELECT id, telegram_id FROM clients WHERE master_id=? AND phone=?", (master_id, client_phone)
        ) as c:
            client_row = await c.fetchone()

        if client_row:
            client_id = client_row[0]
            client_tg_id = client_row[1]
        else:
            await db.execute(
                "INSERT INTO clients (master_id, name, phone) VALUES (?, ?, ?)",
                (master_id, client_name, client_phone)
            )
            async with db.execute("SELECT last_insert_rowid()") as c:
                client_id = (await c.fetchone())[0]
            client_tg_id = None

        await db.execute(
            "INSERT INTO appointments (client_id, master_id, procedure, appointment_date, time, status) "
            "VALUES (?, ?, 'Запись онлайн', ?, ?, 'pending')",
            (client_id, master_id, date_str, time_str)
        )
        async with db.execute("SELECT last_insert_rowid()") as c:
            appt_id = (await c.fetchone())[0]
        await db.commit()
    return appt_id


# ── Расписание для дашборда ───────────────────────────────────────────

async def get_schedule(master_id: int, date_str: str) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.name, a.procedure, a.time, a.status, c.phone, a.price, a.notes, a.service_done_at
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.master_id=? AND a.appointment_date=?
            ORDER BY a.time, a.id
        """, (master_id, date_str)) as cursor:
            rows = await cursor.fetchall()
    return [
        {"id": r[0], "client": r[1], "procedure": r[2], "time": r[3],
         "status": r[4], "phone": r[5], "price": r[6], "notes": r[7] or '', "service_done_at": r[8]}
        for r in rows
    ]


async def get_appointment(master_id: int, appointment_id: int) -> dict | None:
    """Получить детали одной записи."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.name, c.phone, a.procedure, a.appointment_date, a.time,
                   a.status, a.price, a.notes, a.service_done_at
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.id=? AND a.master_id=?
        """, (appointment_id, master_id)) as cursor:
            row = await cursor.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "client": row[1], "phone": row[2], "procedure": row[3],
        "date": row[4], "time": row[5], "status": row[6], "price": row[7],
        "notes": row[8] or '', "service_done_at": row[9]
    }


async def update_appointment_status_db(master_id: int, appointment_id: int, status: str):
    """Обновить статус записи."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE appointments SET status=? WHERE id=? AND master_id=?",
            (status, appointment_id, master_id)
        )
        await db.commit()


async def mark_appointment_done_db(master_id: int, appointment_id: int):
    """Отметить услугу как оказанную. Устанавливает статус completed и планирует запрос отзыва через 2 часа."""
    from datetime import datetime, timedelta
    done_at = datetime.utcnow().isoformat()
    review_at = (datetime.utcnow() + timedelta(hours=2)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            UPDATE appointments
            SET service_done_at=?, review_requested_at=?, status='completed'
            WHERE id=? AND master_id=?
        """, (done_at, review_at, appointment_id, master_id))
        await db.commit()


# ── Авторизация в веб-панели (код из бота) ────────────────────────────

async def create_login_code(telegram_id: int) -> str:
    code = "".join(random.choices(string.digits, k=6))
    expires_at = (datetime.utcnow() + timedelta(minutes=10)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM login_codes WHERE telegram_id=?", (telegram_id,))
        await db.execute(
            "INSERT INTO login_codes (telegram_id, code, expires_at) VALUES (?, ?, ?)",
            (telegram_id, code, expires_at)
        )
        await db.commit()
    return code


async def get_all_masters() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT m.id, m.name, m.email, m.phone, m.created_at,
                   m.payment_card, m.payment_phone, m.payment_banks,
                   m.booking_link, m.theme, m.is_active,
                   COUNT(DISTINCT c.id) as clients_count,
                   COUNT(DISTINCT a.id) as appointments_count
            FROM masters m
            LEFT JOIN clients c ON c.master_id = m.id
            LEFT JOIN appointments a ON a.master_id = m.id
            GROUP BY m.id
            ORDER BY m.created_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
    return [dict(r) for r in rows]


async def verify_login_code(telegram_id: int, code: str) -> bool:
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM login_codes WHERE telegram_id=? AND code=? AND expires_at > ? AND used=0",
            (telegram_id, code, now)
        ) as c:
            row = await c.fetchone()
        if not row:
            return False
        await db.execute("UPDATE login_codes SET used=1 WHERE id=?", (row[0],))
        await db.commit()
    return True


async def get_blocked_days(master_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT date FROM blocked_days WHERE master_id=? ORDER BY date", (master_id,)
        ) as c:
            rows = await c.fetchall()
    return [r[0] for r in rows]


async def add_blocked_day(master_id: int, date_str: str) -> bool:
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR IGNORE INTO blocked_days (master_id, date) VALUES (?, ?)",
                (master_id, date_str)
            )
            await db.commit()
        return True
    except Exception:
        return False


async def remove_blocked_day(master_id: int, date_str: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        result = await db.execute(
            "DELETE FROM blocked_days WHERE master_id=? AND date=?",
            (master_id, date_str)
        )
        await db.commit()
        return result.rowcount > 0


async def import_clients_batch(master_id: int, clients: list) -> dict:
    imported = 0
    skipped = 0
    async with aiosqlite.connect(DB_PATH) as db:
        for c in clients:
            if not c.phone.strip():
                skipped += 1
                continue
            async with db.execute(
                "SELECT id FROM clients WHERE master_id=? AND phone=?", (master_id, c.phone.strip())
            ) as cur:
                existing = await cur.fetchone()
            if existing:
                skipped += 1
                continue
            await db.execute(
                "INSERT INTO clients (master_id, name, phone, notes) VALUES (?, ?, ?, ?)",
                (master_id, c.name, c.phone.strip(), c.notes)
            )
            imported += 1
        await db.commit()
    return {"imported": imported, "skipped": skipped}


async def get_reminder_template(master_id: int, template_type: str) -> str | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT template FROM reminder_templates WHERE master_id=? AND type=?",
            (master_id, template_type)
        ) as c:
            row = await c.fetchone()
    return row[0] if row else None


async def get_reminder_template_with_enabled(master_id: int, template_type: str) -> tuple[str | None, bool]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT template, enabled FROM reminder_templates WHERE master_id=? AND type=?",
            (master_id, template_type)
        ) as c:
            row = await c.fetchone()
    if row:
        return row[0], bool(row[1]) if row[1] is not None else True
    return None, True


async def upsert_reminder_template(master_id: int, template_type: str, template: str, enabled: bool = True):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO reminder_templates (master_id, type, template, enabled) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(master_id, type) DO UPDATE SET template=excluded.template, enabled=excluded.enabled",
            (master_id, template_type, template, 1 if enabled else 0)
        )
        await db.commit()


async def get_all_reminder_templates(master_id: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT type, template, enabled FROM reminder_templates WHERE master_id=?",
            (master_id,)
        ) as c:
            rows = await c.fetchall()
    return [{"type": r[0], "template": r[1], "enabled": bool(r[2]) if r[2] is not None else True} for r in rows]
