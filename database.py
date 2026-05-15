import asyncpg
import pytz
from config import DATABASE_URL
from datetime import datetime, timedelta

_pool: asyncpg.Pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        import ssl as ssl_lib
        import urllib.parse
        raw = DATABASE_URL or ''
        base_url = raw.split('?')[0]
        parsed = urllib.parse.urlparse(base_url)
        ssl_ctx = ssl_lib.create_default_context()
        ssl_ctx.check_hostname = False
        ssl_ctx.verify_mode = ssl_lib.CERT_NONE
        _pool = await asyncpg.create_pool(
            host=parsed.hostname,
            port=parsed.port or 5432,
            user=urllib.parse.unquote(parsed.username or ''),
            password=urllib.parse.unquote(parsed.password or ''),
            database=parsed.path.lstrip('/'),
            ssl=ssl_ctx
        )
    return _pool


async def init_db():
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS masters (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT UNIQUE NOT NULL,
                name TEXT,
                reminder_days INTEGER DEFAULT 40,
                work_start INTEGER DEFAULT 10,
                work_end INTEGER DEFAULT 20,
                slot_duration INTEGER DEFAULT 60,
                timezone TEXT DEFAULT 'Europe/Moscow',
                created_at TIMESTAMP DEFAULT NOW(),
                email TEXT DEFAULT '',
                password_hash TEXT DEFAULT '',
                theme TEXT DEFAULT 'pink'
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id SERIAL PRIMARY KEY,
                master_id INTEGER REFERENCES masters(id),
                name TEXT,
                phone TEXT,
                notes TEXT DEFAULT '',
                telegram_id BIGINT,
                username TEXT DEFAULT '',
                timezone TEXT DEFAULT 'Europe/Moscow',
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            ALTER TABLE clients ADD COLUMN IF NOT EXISTS telegram_id BIGINT
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id SERIAL PRIMARY KEY,
                client_id INTEGER REFERENCES clients(id),
                master_id INTEGER REFERENCES masters(id),
                procedure TEXT,
                appointment_date TEXT,
                time TEXT DEFAULT '',
                price INTEGER DEFAULT 0,
                notes TEXT DEFAULT '',
                photo_id TEXT DEFAULT '',
                status TEXT DEFAULT 'confirmed',
                reminder_24h_sent INTEGER DEFAULT 0,
                reminder_2h_sent INTEGER DEFAULT 0,
                correction_reminder_sent INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id SERIAL PRIMARY KEY,
                master_id INTEGER REFERENCES masters(id),
                client_id INTEGER REFERENCES clients(id),
                name TEXT,
                total_sessions INTEGER,
                used_sessions INTEGER DEFAULT 0,
                price INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS login_codes (
                id SERIAL PRIMARY KEY,
                telegram_id BIGINT NOT NULL,
                code TEXT NOT NULL,
                expires_at TIMESTAMP NOT NULL,
                used INTEGER DEFAULT 0
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reviews (
                id SERIAL PRIMARY KEY,
                appointment_id INTEGER REFERENCES appointments(id),
                client_id INTEGER REFERENCES clients(id),
                master_id INTEGER REFERENCES masters(id),
                rating INTEGER NOT NULL,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS services (
                id SERIAL PRIMARY KEY,
                master_id INTEGER REFERENCES masters(id),
                name TEXT NOT NULL,
                price_default INTEGER DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS expenses (
                id SERIAL PRIMARY KEY,
                master_id INTEGER NOT NULL,
                category VARCHAR(100) NOT NULL,
                amount INTEGER NOT NULL,
                description TEXT DEFAULT '',
                date DATE NOT NULL DEFAULT CURRENT_DATE,
                created_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Миграции для существующих баз
        for sql in [
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS payment_card TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS payment_phone TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS payment_banks TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS deposit_enabled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS deposit_percent INTEGER DEFAULT 30",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS client_type TEXT DEFAULT 'new'",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS review_sent INTEGER DEFAULT 0",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS deposit_status TEXT DEFAULT 'not_required'",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS deposit_amount INTEGER DEFAULT 0",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS theme TEXT DEFAULT 'pink'",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS payment_reminder_enabled BOOLEAN DEFAULT TRUE",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS service_done_at TIMESTAMP",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS review_requested_at TIMESTAMP",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS email TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS password_hash TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS booking_link TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS loyalty_threshold INTEGER DEFAULT 10",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS birthday_discount_enabled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS birthday_discount_percent INTEGER DEFAULT 10",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS loyalty_discount_enabled BOOLEAN DEFAULT FALSE",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS loyalty_discount_percent INTEGER DEFAULT 10",
            "ALTER TABLE clients ADD COLUMN IF NOT EXISTS birthday TEXT",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS phone TEXT DEFAULT ''",
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS is_active INTEGER DEFAULT 1",
        ]:
            try:
                await conn.execute(sql)
            except Exception:
                pass
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS blocked_days (
                id SERIAL PRIMARY KEY,
                master_id INTEGER REFERENCES masters(id) ON DELETE CASCADE,
                date TEXT NOT NULL,
                UNIQUE(master_id, date)
            )
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS telegram_link_tokens (
                id SERIAL PRIMARY KEY,
                master_id INTEGER NOT NULL,
                token TEXT NOT NULL UNIQUE,
                expires_at TIMESTAMPTZ NOT NULL
            )
        """)


# ── Мастера ───────────────────────────────────────────────────────────

async def get_or_create_master(telegram_id: int, name: str) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM masters WHERE telegram_id=$1", telegram_id)
        if row:
            return row['id']
        row = await conn.fetchrow(
            "INSERT INTO masters (telegram_id, name) VALUES ($1, $2) RETURNING id",
            telegram_id, name
        )
        return row['id']


async def get_master_info(master_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, telegram_id, name, work_start, work_end, slot_duration FROM masters WHERE id=$1",
            master_id
        )
    if not row:
        return None
    return {
        "id": row['id'], "telegram_id": row['telegram_id'], "name": row['name'],
        "work_start": row['work_start'] or 10,
        "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60,
    }


async def get_master_info_by_telegram(telegram_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, telegram_id, name, work_start, work_end, slot_duration FROM masters WHERE telegram_id=$1",
            telegram_id
        )
    if not row:
        return None
    return {
        "id": row['id'], "telegram_id": row['telegram_id'], "name": row['name'],
        "work_start": row['work_start'] or 10,
        "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60,
    }


async def get_master_by_email(email: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, email, work_start, work_end, slot_duration, "
            "reminder_days, payment_card, payment_phone, payment_banks, "
            "theme, password_hash "
            "FROM masters WHERE email=$1",
            email
        )
    if not row:
        return None
    return {
        "id": row['id'], "name": row['name'] or "", "email": row['email'] or "",
        "work_start": row['work_start'] or 9, "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60, "timezone": "Europe/Moscow",
        "reminder_days": row['reminder_days'] or 30,
        "payment_card": row['payment_card'] or "", "payment_phone": row['payment_phone'] or "",
        "payment_banks": row['payment_banks'] or "", "deposit_enabled": False,
        "deposit_percent": 30, "theme": row['theme'] or "pink",
        "password_hash": row['password_hash'] or "",
    }


async def create_master_with_email(email: str, password_hash: str, name: str, phone: str = "") -> int:
    import time
    telegram_id = -int(time.time())
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO masters (email, password_hash, name, telegram_id, is_active, phone) VALUES ($1, $2, $3, $4, 1, $5) RETURNING id",
            email, password_hash, name, telegram_id, phone
        )
    return row['id'] if row else 0


async def get_master_by_booking_link(link: str) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, work_start, work_end, slot_duration, booking_link, telegram_id "
            "FROM masters WHERE booking_link=$1",
            link
        )
    if not row:
        return None
    return {
        "id": row['id'], "name": row['name'] or "",
        "work_start": row['work_start'] or 9, "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60, "booking_link": row['booking_link'] or "",
        "telegram_id": row['telegram_id'],
    }


async def update_booking_link(master_id: int, link: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET booking_link=$1 WHERE id=$2",
            link, master_id
        )
    return True


async def get_master_booking_link(master_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT booking_link FROM masters WHERE id=$1",
            master_id
        )
    return row['booking_link'] if row else ""


async def is_booking_linkTaken(link: str, exclude_master_id: int = 0) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM masters WHERE booking_link=$1 AND id != $2",
            link, exclude_master_id
        )
    return row is not None


async def update_master_work_hours(master_id: int, work_start: int, work_end: int, slot_duration: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET work_start=$1, work_end=$2, slot_duration=$3 WHERE id=$4",
            work_start, work_end, slot_duration, master_id
        )


async def get_all_masters() -> list:
    try:
        pool = await get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("SELECT id, telegram_id, name FROM masters ORDER BY name")
        return [{"id": r['id'], "telegram_id": r['telegram_id'], "name": r['name'] or "Без имени"} for r in rows]
    except Exception as e:
        print(f"get_all_masters error: {e}")
        return []


async def get_reminder_days(telegram_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT reminder_days FROM masters WHERE telegram_id=$1", telegram_id)
    return val or 40


async def get_reminder_days_by_master(master_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT reminder_days FROM masters WHERE id=$1", master_id)
    return val or 40


async def update_reminder_days(telegram_id: int, days: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE masters SET reminder_days=$1 WHERE telegram_id=$2", days, telegram_id)


async def update_reminder_days_by_master(master_id: int, days: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE masters SET reminder_days=$1 WHERE id=$2", days, master_id)


# ── Клиенты ───────────────────────────────────────────────────────────

async def add_client(master_id: int, name: str, phone: str, notes: str = "", birthday: str = "") -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        # Нормализуем телефон — только цифры
        digits = ''.join(filter(str.isdigit, phone or ''))
        if digits.startswith('8') and len(digits) == 11:
            digits = '7' + digits[1:]

        # Проверяем дубль
        existing = await conn.fetchrow(
            "SELECT id FROM clients WHERE master_id=$1 AND regexp_replace(phone, '[^0-9]', '', 'g') = $2",
            master_id, digits
        )
        if existing:
            return existing['id']  # возвращаем существующего клиента

        row = await conn.fetchrow(
            "INSERT INTO clients (master_id, name, phone, notes, birthday) VALUES ($1,$2,$3,$4,$5) RETURNING id",
            master_id, name, phone, notes, birthday if birthday else None
        )
    return row['id']


async def add_client_with_telegram(master_id: int, name: str, phone: str, telegram_id: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO clients (master_id, name, phone, notes, telegram_id) VALUES ($1,$2,$3,'',$4) RETURNING id",
            master_id, name, phone, telegram_id
        )
    return row['id']


async def get_client_by_telegram(master_id: int, telegram_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, phone FROM clients WHERE master_id=$1 AND telegram_id=$2",
            master_id, telegram_id
        )
    if not row:
        return None
    return {"id": row['id'], "name": row['name'], "phone": row['phone']}


async def get_clients(master_id: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.phone, c.notes,
                   MAX(a.appointment_date) as last_visit
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = $1
            GROUP BY c.id, c.name, c.phone, c.notes
            ORDER BY c.name
        """, master_id)
    return [(r['id'], r['name'], r['phone'], r['notes'], r['last_visit']) for r in rows]


async def get_client(client_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, phone, notes, telegram_id, username, master_id FROM clients WHERE id=$1", client_id
        )
    if not row:
        return None
    return {"id": row['id'], "name": row['name'], "phone": row['phone'], "notes": row['notes'], "telegram_id": row['telegram_id'], "username": row['username'] or '', "master_id": row['master_id']}


async def get_appointment_by_id(appointment_id: int) -> dict | None:
    """Получить запись по ID"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, client_id, master_id, procedure, appointment_date, time, status FROM appointments WHERE id=$1", appointment_id
        )
    if not row:
        return None
    return dict(row)


async def assign_client_telegram(client_id: int, telegram_id: int) -> None:
    """Привязать telegram_id к клиенту"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE clients SET telegram_id=$1 WHERE id=$2", telegram_id, client_id
        )


async def update_client(client_id: int, master_id: int, name: str, phone: str, notes: str,
                        username: str = "", birthday: str | None = None,
                        source: str | None = None, allergies: str | None = None) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE clients SET name=$1, phone=$2, notes=$3, username=$4, "
            "birthday=$5, source=$6, allergies=$7 WHERE id=$8 AND master_id=$9",
            name, phone, notes, username, birthday, source, allergies, client_id, master_id
        )
    return result != "UPDATE 0"


async def update_client_username(client_id: int, master_id: int, username: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE clients SET username=$1 WHERE id=$2 AND master_id=$3",
            username, client_id, master_id
        )
    return result != "UPDATE 0"


async def delete_client(client_id: int, master_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM appointments WHERE client_id=$1", client_id)
        await conn.execute("DELETE FROM subscriptions WHERE client_id=$1", client_id)
        result = await conn.execute(
            "DELETE FROM clients WHERE id=$1 AND master_id=$2", client_id, master_id
        )
    return result != "DELETE 0"


async def search_clients(master_id: int, query: str) -> list:
    all_clients = await get_clients(master_id)
    q = query.lower()
    return [c for c in all_clients if q in c[1].lower()]


async def get_inactive_clients(master_id: int, days: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.phone,
                   MAX(a.appointment_date) as last_visit,
                   (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER as days_ago
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = $1
            GROUP BY c.id, c.name, c.phone
            HAVING MAX(a.appointment_date) IS NOT NULL
               AND (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER >= $2
            ORDER BY days_ago DESC
        """, master_id, days)
    return [(r['id'], r['name'], r['phone'], r['last_visit'], r['days_ago']) for r in rows]


# ── Записи ────────────────────────────────────────────────────────────

async def add_appointment(
    client_id: int, master_id: int, procedure: str,
    appointment_date: str, price: int = 0,
    notes: str = "", photo_id: str = "",
    time: str = "", status: str = "confirmed"
) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            INSERT INTO appointments
            (client_id, master_id, procedure, appointment_date, price, notes, photo_id, time, status)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9) RETURNING id
        """, client_id, master_id, procedure, appointment_date, price, notes, photo_id, time, status)
    return row['id']


async def get_client_history(client_id: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT procedure, appointment_date, price, notes, photo_id
            FROM appointments
            WHERE client_id=$1
            ORDER BY appointment_date DESC
            LIMIT 10
        """, client_id)
    return [(r['procedure'], r['appointment_date'], r['price'], r['notes'], r['photo_id']) for r in rows]


async def update_appointment_status(appointment_id: int, status: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE appointments SET status=$1 WHERE id=$2", status, appointment_id
        )


async def update_appointment_service_done(appointment_id: int):
    """Отмечает услугу как оказанную. Устанавливает статус completed и планирует запрос отзыва через 2 часа."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE appointments
            SET service_done_at = NOW(),
                review_requested_at = NOW() + INTERVAL '2 hours',
                status = 'completed'
            WHERE id = $1
        """, appointment_id)


async def get_appointments_for_review_request(target_time: str) -> list:
    """Получает записи, где пора просить отзыв (service_done_at есть, review_requested_at <= now, review ещё не запрошен)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, a.client_id, a.master_id, a.procedure,
                   c.name as client_name, c.telegram_id as client_telegram_id
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.service_done_at IS NOT NULL
              AND a.review_requested_at IS NOT NULL
              AND a.review_requested_at <= $1::timestamp
              AND a.review_sent = 0
            ORDER BY a.review_requested_at
        """, target_time)
        return [dict(row) for row in rows]


async def get_appointment_client_telegram(appointment_id: int) -> tuple:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT c.telegram_id, a.appointment_date, a.time
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.id=$1
        """, appointment_id)
    if not row:
        return None, None, None
    return row['telegram_id'], row['appointment_date'], row['time']


# ── Слоты и расписание ────────────────────────────────────────────────

async def get_busy_slots(master_id: int, date: str) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT time FROM appointments WHERE master_id=$1 AND appointment_date=$2 AND status != 'cancelled'",
            master_id, date
        )
    return [r['time'] for r in rows if r['time']]


async def get_available_slots(
    master_id: int, date: str,
    work_start: int, work_end: int, slot_duration: int
) -> list[str]:
    busy = await get_busy_slots(master_id, date)
    slots = []
    total_minutes = work_start * 60
    end_minutes = work_end * 60

    while total_minutes + slot_duration <= end_minutes:
        h = total_minutes // 60
        m = total_minutes % 60
        time_str = f"{h:02d}:{m:02d}"
        if time_str not in busy:
            slots.append(time_str)
        total_minutes += slot_duration

    return slots


async def get_master_schedule(master_id: int, date: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.name, a.procedure, a.time, a.status, c.phone, a.notes, a.service_done_at
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.master_id=$1 AND a.appointment_date=$2
            ORDER BY a.time, a.id
        """, master_id, date)
    return [(r['id'], r['name'], r['procedure'], r['time'], r['status'], r['phone'], r['notes'], r['service_done_at']) for r in rows]


# ── Напоминания клиентам ──────────────────────────────────────────────

async def get_appointments_for_reminder_24h(target_date: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date=$1
              AND a.status != 'cancelled'
              AND a.reminder_24h_sent = 0
              AND c.telegram_id IS NOT NULL
        """, target_date)
    return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


async def get_appointments_for_reminder_2h(target_date: str, target_time_from: str, target_time_to: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date=$1
              AND a.time BETWEEN $2 AND $3
              AND a.status != 'cancelled'
              AND a.reminder_2h_sent = 0
              AND c.telegram_id IS NOT NULL
        """, target_date, target_time_from, target_time_to)
    return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


async def mark_reminder_sent(appointment_id: int, reminder_type: str):
    col = "reminder_24h_sent" if reminder_type == "24h" else "reminder_2h_sent"
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(f"UPDATE appointments SET {col}=1 WHERE id=$1", appointment_id)


async def get_appointments_for_correction_reminder(three_weeks_ago: str) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.name, m.name, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date=$1
              AND a.status = 'confirmed'
              AND a.correction_reminder_sent = 0
              AND c.telegram_id IS NOT NULL
        """, three_weeks_ago)
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


async def mark_correction_reminder_sent(appointment_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE appointments SET correction_reminder_sent=1 WHERE id=$1", appointment_id
        )


# ── Абонементы ────────────────────────────────────────────────────────

async def add_subscription(master_id: int, client_id: int, name: str, total: int, price: int) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO subscriptions (master_id, client_id, name, total_sessions, price) VALUES ($1,$2,$3,$4,$5) RETURNING id",
            master_id, client_id, name, total, price
        )
    return row['id']


async def get_client_subscriptions(client_id: int, master_id: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, name, total_sessions, used_sessions, price
            FROM subscriptions
            WHERE client_id=$1 AND master_id=$2
            ORDER BY created_at DESC
        """, client_id, master_id)
    return [(r['id'], r['name'], r['total_sessions'], r['used_sessions'], r['price']) for r in rows]


async def use_subscription_session(sub_id: int, master_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT used_sessions, total_sessions FROM subscriptions WHERE id=$1 AND master_id=$2",
            sub_id, master_id
        )
        if not row or row['used_sessions'] >= row['total_sessions']:
            return False
        await conn.execute(
            "UPDATE subscriptions SET used_sessions=used_sessions+1 WHERE id=$1", sub_id
        )
    return True


# ── Статистика ────────────────────────────────────────────────────────

async def get_statistics(master_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        total_clients = await conn.fetchval(
            "SELECT COUNT(*) FROM clients WHERE master_id=$1", master_id
        )
        row = await conn.fetchrow(
            "SELECT COUNT(*), COALESCE(SUM(price), 0) FROM appointments WHERE master_id=$1", master_id
        )
        total_appointments, total_earnings = row[0], row[1]

        month_earnings = await conn.fetchval(
            "SELECT COALESCE(SUM(price), 0) FROM appointments "
            "WHERE master_id=$1 AND TO_CHAR(appointment_date::date, 'YYYY-MM') = TO_CHAR(CURRENT_DATE, 'YYYY-MM')",
            master_id
        )
        top_rows = await conn.fetch(
            "SELECT procedure, COUNT(*) as cnt FROM appointments "
            "WHERE master_id=$1 GROUP BY procedure ORDER BY cnt DESC LIMIT 3",
            master_id
        )
        top_procedures = [(r['procedure'], r['cnt']) for r in top_rows]

    return {
        "total_clients": total_clients,
        "total_appointments": total_appointments,
        "total_earnings": total_earnings,
        "month_earnings": month_earnings,
        "top_procedures": [{"procedure": p[0], "count": p[1]} for p in top_procedures],
    }


# ── Веб-панель: авторизация (код из бота) ─────────────────────────────

async def create_login_code(telegram_id: int) -> str:
    import random, string
    from datetime import datetime, timedelta
    code = "".join(random.choices(string.digits, k=6))
    expires_at = datetime.utcnow() + timedelta(minutes=10)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM login_codes WHERE telegram_id=$1", telegram_id)
        await conn.execute(
            "INSERT INTO login_codes (telegram_id, code, expires_at) VALUES ($1,$2,$3)",
            telegram_id, code, expires_at
        )
    return code


async def verify_login_code(telegram_id: int, code: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id FROM login_codes WHERE telegram_id=$1 AND code=$2 AND expires_at > NOW() AND used=0",
            telegram_id, code
        )
        if not row:
            return False
        await conn.execute("UPDATE login_codes SET used=1 WHERE id=$1", row['id'])
    return True


async def verify_login_code_by_code(code: str) -> int | None:
    """Проверяет код без telegram_id, возвращает telegram_id мастера или None."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, telegram_id FROM login_codes WHERE code=$1 AND expires_at > NOW() AND used=0",
            code
        )
        if not row:
            return None
        await conn.execute("UPDATE login_codes SET used=1 WHERE id=$1", row['id'])
    return row['telegram_id']


# ── Веб-панель: настройки мастера ─────────────────────────────────────

async def get_master_full(master_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, telegram_id, name, reminder_days, work_start, work_end, slot_duration, "
            "COALESCE(payment_card,'') as payment_card, COALESCE(payment_phone,'') as payment_phone, COALESCE(payment_banks,'') as payment_banks, "
            "COALESCE(timezone,'Europe/Moscow') as timezone, "
            "loyalty_threshold, birthday_discount_enabled, birthday_discount_percent, "
            "loyalty_discount_enabled, loyalty_discount_percent "
            "FROM masters WHERE id=$1",
            master_id
        )
    if not row:
        return None
    return {
        "id": row['id'], "telegram_id": row['telegram_id'], "name": row['name'],
        "reminder_days": row['reminder_days'] or 40,
        "work_start": row['work_start'] or 10, "work_end": row['work_end'] or 20,
        "slot_duration": row['slot_duration'] or 60,
        "payment_card": row['payment_card'],
        "payment_phone": row['payment_phone'],
        "payment_banks": row['payment_banks'],
        "timezone": row['timezone'],
        "loyalty_threshold": row['loyalty_threshold'] or 10,
        "birthday_discount_enabled": row['birthday_discount_enabled'] or False,
        "birthday_discount_percent": row['birthday_discount_percent'] or 10,
        "loyalty_discount_enabled": row['loyalty_discount_enabled'] or False,
        "loyalty_discount_percent": row['loyalty_discount_percent'] or 10,
    }


async def update_master_full_settings(master_id: int, name: str, work_start: int,
                                       work_end: int, slot_duration: int, reminder_days: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET name=$1, work_start=$2, work_end=$3, slot_duration=$4, reminder_days=$5 WHERE id=$6",
            name, work_start, work_end, slot_duration, reminder_days, master_id
        )


async def update_master_payment(master_id: int, payment_card: str, payment_phone: str = "", payment_banks: str = ""):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET payment_card=$1, payment_phone=$2, payment_banks=$3 WHERE id=$4",
            payment_card, payment_phone, payment_banks, master_id
        )


async def update_master_timezone(master_id: int, timezone: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET timezone=$1 WHERE id=$2", timezone, master_id
        )


async def update_master_loyalty_settings(master_id: int, loyalty_enabled: bool, loyalty_threshold: int,
                                          loyalty_discount_percent: int, birthday_enabled: bool,
                                          birthday_discount_percent: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET loyalty_discount_enabled=$1, loyalty_threshold=$2, "
            "loyalty_discount_percent=$3, birthday_discount_enabled=$4, birthday_discount_percent=$5 "
            "WHERE id=$6",
            loyalty_enabled, loyalty_threshold, loyalty_discount_percent,
            birthday_enabled, birthday_discount_percent, master_id
        )


# ── Отзывы ────────────────────────────────────────────────────────────

async def get_appointments_for_review(target_date: str, time_from: str, time_to: str) -> list:
    """Записи, завершившиеся ~2 часа назад — просим оценку."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.id as client_id, a.master_id,
                   c.name, m.name as master_name, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date = $1
              AND a.time BETWEEN $2 AND $3
              AND a.status = 'confirmed'
              AND a.review_sent = 0
              AND c.telegram_id IS NOT NULL
        """, target_date, time_from, time_to)
    return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


async def mark_review_sent(appointment_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE appointments SET review_sent=1 WHERE id=$1", appointment_id)


async def save_review(appointment_id: int, client_id: int, master_id: int, rating: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reviews (appointment_id, client_id, master_id, rating) VALUES ($1,$2,$3,$4)",
            appointment_id, client_id, master_id, rating
        )


async def get_master_reviews(master_id: int, limit: int = 20) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT r.rating, c.name, a.procedure, a.appointment_date, r.created_at
            FROM reviews r
            JOIN clients c ON c.id = r.client_id
            JOIN appointments a ON a.id = r.appointment_id
            WHERE r.master_id = $1
            ORDER BY r.created_at DESC
            LIMIT $2
        """, master_id, limit)
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


# ── Шаблоны сообщений ─────────────────────────────────────────────────

async def get_clients_with_telegram(master_id: int) -> list:
    """Клиенты с telegram_id (могут получать сообщения)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT c.id, c.name, c.telegram_id,
                   MAX(a.appointment_date) as last_visit,
                   (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER as days_ago
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = $1 AND c.telegram_id IS NOT NULL
            GROUP BY c.id, c.name, c.telegram_id
            ORDER BY c.name
        """, master_id)
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


# ── Предоплата ────────────────────────────────────────────────────────

async def get_master_deposit_settings(master_id: int) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT deposit_enabled, deposit_percent, COALESCE(payment_card,'') as payment_card "
            "FROM masters WHERE id=$1",
            master_id
        )
    if not row:
        return {"deposit_enabled": False, "deposit_percent": 30, "payment_card": ""}
    return {
        "deposit_enabled": bool(row["deposit_enabled"]),
        "deposit_percent": row["deposit_percent"] or 30,
        "payment_card": row["payment_card"],
    }


async def update_master_deposit_settings(master_id: int, enabled: bool, percent: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET deposit_enabled=$1, deposit_percent=$2 WHERE id=$3",
            enabled, percent, master_id
        )


async def get_client_type(client_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval("SELECT client_type FROM clients WHERE id=$1", client_id)
    return val or "new"


async def mark_client_regular(client_id: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("UPDATE clients SET client_type='regular' WHERE id=$1", client_id)


async def update_appointment_deposit(appointment_id: int, deposit_status: str, deposit_amount: int = 0):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE appointments SET deposit_status=$1, deposit_amount=$2 WHERE id=$3",
            deposit_status, deposit_amount, appointment_id
        )


async def get_appointment_with_client(appointment_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT a.id, a.master_id, a.client_id, a.procedure,
                   a.appointment_date, a.time, a.deposit_status, a.deposit_amount,
                   a.status, a.notes, a.service_done_at,
                   c.name as client_name, c.telegram_id as client_tg_id,
                   m.telegram_id as master_tg_id, m.name as master_name
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.id=$1
        """, appointment_id)
    if not row:
        return None
    return dict(row)


# ── Шаблоны сообщений ─────────────────────────────────────────────────

async def get_clients_inactive_range(master_id: int, min_days: int, max_days: int | None) -> list:
    """Неактивные клиенты с telegram_id в диапазоне дней."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if max_days is None:
            rows = await conn.fetch("""
                SELECT c.id, c.name, c.telegram_id, MAX(a.appointment_date) as last_visit,
                       (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER as days_ago
                FROM clients c
                LEFT JOIN appointments a ON a.client_id = c.id
                WHERE c.master_id = $1 AND c.telegram_id IS NOT NULL
                GROUP BY c.id, c.name, c.telegram_id
                HAVING MAX(a.appointment_date) IS NOT NULL
                   AND (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER >= $2
                ORDER BY days_ago DESC
            """, master_id, min_days)
        else:
            rows = await conn.fetch("""
                SELECT c.id, c.name, c.telegram_id, MAX(a.appointment_date) as last_visit,
                       (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER as days_ago
                FROM clients c
                LEFT JOIN appointments a ON a.client_id = c.id
                WHERE c.master_id = $1 AND c.telegram_id IS NOT NULL
                GROUP BY c.id, c.name, c.telegram_id
                HAVING MAX(a.appointment_date) IS NOT NULL
                   AND (CURRENT_DATE - MAX(a.appointment_date::date))::INTEGER BETWEEN $2 AND $3
                ORDER BY days_ago DESC
            """, master_id, min_days, max_days)
    return [(r[0], r[1], r[2], r[3], r[4]) for r in rows]


# ── Клиентские записи ───────────────────────────────────────────

async def get_client_pending_appointments(telegram_id: int) -> list:
    """Получить все записи, ожидающие подтверждения, для клиента с данным telegram_id"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, a.procedure, a.appointment_date, a.time, m.name as master_name
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE c.telegram_id = $1 AND a.status = 'pending'
            ORDER BY a.appointment_date, a.time
        """, telegram_id)
    return [dict(r) for r in rows]


# ── Тема оформления ───────────────────────────────────────────────────

async def get_master_theme(telegram_id: int) -> str:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT theme FROM masters WHERE telegram_id=$1", telegram_id
        )
    return (row['theme'] or 'pink') if row else 'pink'


async def set_master_theme(telegram_id: int, theme: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET theme=$1 WHERE telegram_id=$2", theme, telegram_id
        )


# ── Напоминание об оплате ─────────────────────────────────────────────

async def get_payment_reminder_enabled(telegram_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        val = await conn.fetchval(
            "SELECT payment_reminder_enabled FROM masters WHERE telegram_id=$1", telegram_id
        )
    return bool(val) if val is not None else True


# ── Услуги мастера ────────────────────────────────────────────────────

async def add_service(master_id: int, name: str, price_default: int = 0) -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO services (master_id, name, price_default) VALUES ($1,$2,$3) RETURNING id",
            master_id, name, price_default
        )
    return row['id']


async def get_services(master_id: int) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, name, price_default FROM services WHERE master_id=$1 ORDER BY created_at",
            master_id
        )
    return [(r['id'], r['name'], r['price_default']) for r in rows]


async def get_service(service_id: int, master_id: int) -> dict | None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, name, price_default FROM services WHERE id=$1 AND master_id=$2",
            service_id, master_id
        )
    if not row:
        return None
    return {"id": row['id'], "name": row['name'], "price_default": row['price_default']}


async def delete_service(service_id: int, master_id: int) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM services WHERE id=$1 AND master_id=$2", service_id, master_id
        )
    return result != "DELETE 0"


# ── Расширенная статистика ────────────────────────────────────────────

async def get_earnings_by_period(master_id: int, date_from: str, date_to: str) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT COUNT(*) as cnt, COALESCE(SUM(price), 0) as total
            FROM appointments
            WHERE master_id=$1
              AND appointment_date BETWEEN $2 AND $3
              AND status != 'cancelled'
        """, master_id, date_from, date_to)
    return {"total_appointments": row['cnt'], "total_earnings": row['total']}


async def get_earnings_by_service(master_id: int, date_from: str = None, date_to: str = None) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if date_from and date_to:
            rows = await conn.fetch("""
                SELECT procedure, COUNT(*) as cnt, COALESCE(SUM(price), 0) as total
                FROM appointments
                WHERE master_id=$1 AND status != 'cancelled'
                  AND appointment_date BETWEEN $2 AND $3
                GROUP BY procedure ORDER BY total DESC
            """, master_id, date_from, date_to)
        else:
            rows = await conn.fetch("""
                SELECT procedure, COUNT(*) as cnt, COALESCE(SUM(price), 0) as total
                FROM appointments
                WHERE master_id=$1 AND status != 'cancelled'
                GROUP BY procedure ORDER BY total DESC
            """, master_id)
    return [(r['procedure'], r['cnt'], r['total']) for r in rows]


async def get_earnings_by_client(master_id: int, date_from: str = None, date_to: str = None) -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        if date_from and date_to:
            rows = await conn.fetch("""
                SELECT c.name, COUNT(a.id) as cnt, COALESCE(SUM(a.price), 0) as total
                FROM appointments a
                JOIN clients c ON c.id = a.client_id
                WHERE a.master_id=$1 AND a.status != 'cancelled'
                  AND a.appointment_date BETWEEN $2 AND $3
                GROUP BY c.id, c.name ORDER BY total DESC LIMIT 20
            """, master_id, date_from, date_to)
        else:
            rows = await conn.fetch("""
                SELECT c.name, COUNT(a.id) as cnt, COALESCE(SUM(a.price), 0) as total
                FROM appointments a
                JOIN clients c ON c.id = a.client_id
                WHERE a.master_id=$1 AND a.status != 'cancelled'
                GROUP BY c.id, c.name ORDER BY total DESC LIMIT 20
            """, master_id)
    return [(r['name'], r['cnt'], r['total']) for r in rows]


async def get_earnings_by_day(master_id: int, days: int = 30) -> list:
    from datetime import date, timedelta
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT appointment_date::date as day, COALESCE(SUM(price), 0) as total
            FROM appointments
            WHERE master_id=$1
              AND status != 'cancelled'
              AND appointment_date >= $2
            GROUP BY day ORDER BY day
        """, master_id, cutoff)
    return [(str(r['day']), int(r['total'])) for r in rows]


async def set_payment_reminder_enabled(telegram_id: int, enabled: bool) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET payment_reminder_enabled=$1 WHERE telegram_id=$2", enabled, telegram_id
        )


async def get_appointments_pending_deposit_24h(target_date: str) -> list:
    """Записи завтра с невнесённой предоплатой, у мастеров с включённым напоминанием."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, m.deposit_percent
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date = $1
              AND a.deposit_status = 'pending_payment'
              AND a.status != 'cancelled'
              AND c.telegram_id IS NOT NULL
              AND COALESCE(m.payment_reminder_enabled, TRUE) = TRUE
        """, target_date)
    return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


async def get_appointments_pending_deposit_2h() -> list:
    """Записи с невнесённой предоплатой, созданные 2-3 часа назад."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, m.deposit_percent
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.created_at >= NOW() - INTERVAL '3 hours'
              AND a.created_at < NOW() - INTERVAL '2 hours'
              AND a.deposit_status = 'pending_payment'
              AND a.status != 'cancelled'
              AND c.telegram_id IS NOT NULL
              AND COALESCE(m.payment_reminder_enabled, TRUE) = TRUE
        """)
    return [(r[0], r[1], r[2], r[3], r[4], r[5], r[6]) for r in rows]


# ── Часовые пояса ───────────────────────────────────────────────────────

def get_local_time(utc_time: datetime, timezone: str) -> datetime:
    """Конвертирует UTC время в локальное."""
    if not timezone:
        timezone = 'Europe/Moscow'
    try:
        tz = pytz.timezone(timezone)
        if utc_time.tzinfo is None:
            utc_time = pytz.utc.localize(utc_time)
        return utc_time.astimezone(tz)
    except Exception:
        return utc_time


def to_utc(local_time: datetime, timezone: str) -> datetime:
    """Конвертирует локальное время в UTC."""
    if not timezone:
        timezone = 'Europe/Moscow'
    try:
        tz = pytz.timezone(timezone)
        if local_time.tzinfo is None:
            local_time = tz.localize(local_time)
        return local_time.astimezone(pytz.utc)
    except Exception:
        return local_time


def format_local_time(utc_time: datetime, timezone: str, fmt: str = "%H:%M") -> str:
    """Форматирует UTC время в локальное строкой."""
    local = get_local_time(utc_time, timezone)
    return local.strftime(fmt)


def get_client_timezone(client_id: int) -> str:
    """Получает часовой пояс клиента."""
    # Пока возвращаем дефолт — потом можно получить из БД
    return 'Europe/Moscow'


# ── Расходы ─────────────────────────────────────────────────────────────

async def get_expenses(master_id: int) -> list:
    """Получить все расходы мастера"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, category, amount, description, date FROM expenses "
            "WHERE master_id=$1 ORDER BY date DESC, created_at DESC",
            master_id
        )
    return [(r['id'], r['category'], r['amount'], r['description'], r['date']) for r in rows]


async def add_expense(master_id: int, category: str, amount: int, description: str, date: str) -> int:
    """Добавить новый расход"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO expenses (master_id, category, amount, description, date) "
            "VALUES ($1, $2, $3, $4, $5) RETURNING id",
            master_id, category, amount, description, date
        )
    return row['id']


async def delete_expense(expense_id: int, master_id: int) -> bool:
    """Удалить расход"""
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "DELETE FROM expenses WHERE id=$1 AND master_id=$2",
            expense_id, master_id
        )
    return result != "DELETE 0"


# ── Нерабочие дни ────────────────────────────────────────────────────

async def get_blocked_days(master_id: int) -> list[str]:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT date FROM blocked_days WHERE master_id=$1 ORDER BY date",
            master_id
        )
    return [r['date'] for r in rows]


async def add_blocked_day(master_id: int, date: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO blocked_days (master_id, date) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            master_id, date
        )


async def remove_blocked_day(master_id: int, date: str) -> None:
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "DELETE FROM blocked_days WHERE master_id=$1 AND date=$2",
            master_id, date
        )
