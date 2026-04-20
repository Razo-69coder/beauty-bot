import asyncpg
from config import DATABASE_URL
from datetime import datetime, timedelta

_pool: asyncpg.Pool = None


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL)
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
                created_at TIMESTAMP DEFAULT NOW()
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
                created_at TIMESTAMP DEFAULT NOW()
            )
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
        # Миграции для существующих баз
        for sql in [
            "ALTER TABLE masters ADD COLUMN IF NOT EXISTS payment_card TEXT DEFAULT ''",
            "ALTER TABLE appointments ADD COLUMN IF NOT EXISTS review_sent INTEGER DEFAULT 0",
        ]:
            try:
                await conn.execute(sql)
            except Exception:
                pass


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


async def update_master_work_hours(master_id: int, work_start: int, work_end: int, slot_duration: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET work_start=$1, work_end=$2, slot_duration=$3 WHERE id=$4",
            work_start, work_end, slot_duration, master_id
        )


async def get_all_masters() -> list:
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, telegram_id FROM masters")
    return [(r['id'], r['telegram_id']) for r in rows]


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

async def add_client(master_id: int, name: str, phone: str, notes: str = "") -> int:
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO clients (master_id, name, phone, notes) VALUES ($1,$2,$3,$4) RETURNING id",
            master_id, name, phone, notes
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
            "SELECT id, name, phone, notes FROM clients WHERE id=$1", client_id
        )
    if not row:
        return None
    return {"id": row['id'], "name": row['name'], "phone": row['phone'], "notes": row['notes']}


async def update_client(client_id: int, master_id: int, name: str, phone: str, notes: str) -> bool:
    pool = await get_pool()
    async with pool.acquire() as conn:
        result = await conn.execute(
            "UPDATE clients SET name=$1, phone=$2, notes=$3 WHERE id=$4 AND master_id=$5",
            name, phone, notes, client_id, master_id
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
            SELECT a.id, c.name, a.procedure, a.time, a.status, c.phone
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.master_id=$1 AND a.appointment_date=$2
            ORDER BY a.time, a.id
        """, master_id, date)
    return [(r['id'], r['name'], r['procedure'], r['time'], r['status'], r['phone']) for r in rows]


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
            "COALESCE(payment_card,'') as payment_card "
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
    }


async def update_master_full_settings(master_id: int, name: str, work_start: int,
                                       work_end: int, slot_duration: int, reminder_days: int):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET name=$1, work_start=$2, work_end=$3, slot_duration=$4, reminder_days=$5 WHERE id=$6",
            name, work_start, work_end, slot_duration, reminder_days, master_id
        )


async def update_master_payment(master_id: int, payment_card: str):
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE masters SET payment_card=$1 WHERE id=$2", payment_card, master_id
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
