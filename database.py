import aiosqlite
from config import DB_PATH
from datetime import datetime, timedelta


async def init_db():
    """Создаёт таблицы и выполняет миграции при старте"""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE,
                name TEXT,
                reminder_days INTEGER DEFAULT 40,
                work_start INTEGER DEFAULT 10,
                work_end INTEGER DEFAULT 20,
                slot_duration INTEGER DEFAULT 60,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER,
                name TEXT,
                phone TEXT,
                notes TEXT,
                telegram_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (master_id) REFERENCES masters(id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                master_id INTEGER,
                procedure TEXT,
                appointment_date TEXT,
                time TEXT DEFAULT '',
                price INTEGER,
                notes TEXT,
                photo_id TEXT,
                status TEXT DEFAULT 'confirmed',
                reminder_24h_sent INTEGER DEFAULT 0,
                reminder_2h_sent INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER,
                client_id INTEGER,
                name TEXT,
                total_sessions INTEGER,
                used_sessions INTEGER DEFAULT 0,
                price INTEGER DEFAULT 0,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (master_id) REFERENCES masters(id),
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)
        await db.commit()

        # Миграции для существующих баз
        migrations = [
            "ALTER TABLE masters ADD COLUMN reminder_days INTEGER DEFAULT 40",
            "ALTER TABLE masters ADD COLUMN work_start INTEGER DEFAULT 10",
            "ALTER TABLE masters ADD COLUMN work_end INTEGER DEFAULT 20",
            "ALTER TABLE masters ADD COLUMN slot_duration INTEGER DEFAULT 60",
            "ALTER TABLE clients ADD COLUMN telegram_id INTEGER",
            "ALTER TABLE appointments ADD COLUMN time TEXT DEFAULT ''",
            "ALTER TABLE appointments ADD COLUMN status TEXT DEFAULT 'confirmed'",
            "ALTER TABLE appointments ADD COLUMN reminder_24h_sent INTEGER DEFAULT 0",
            "ALTER TABLE appointments ADD COLUMN reminder_2h_sent INTEGER DEFAULT 0",
            "ALTER TABLE appointments ADD COLUMN correction_reminder_sent INTEGER DEFAULT 0",
        ]
        for sql in migrations:
            try:
                await db.execute(sql)
                await db.commit()
            except Exception:
                pass


# ── Мастера ───────────────────────────────────────────────────────────

async def get_or_create_master(telegram_id: int, name: str) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id = ?", (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
            if row:
                return row[0]
        await db.execute(
            "INSERT INTO masters (telegram_id, name) VALUES (?, ?)", (telegram_id, name)
        )
        await db.commit()
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id = ?", (telegram_id,)
        ) as cur:
            return (await cur.fetchone())[0]


async def get_master_info(master_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, telegram_id, name, work_start, work_end, slot_duration FROM masters WHERE id=?",
            (master_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "telegram_id": row[1], "name": row[2],
        "work_start": row[3] or 10, "work_end": row[4] or 20, "slot_duration": row[5] or 60,
    }


async def get_master_info_by_telegram(telegram_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, telegram_id, name, work_start, work_end, slot_duration FROM masters WHERE telegram_id=?",
            (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {
        "id": row[0], "telegram_id": row[1], "name": row[2],
        "work_start": row[3] or 10, "work_end": row[4] or 20, "slot_duration": row[5] or 60,
    }


async def update_master_work_hours(master_id: int, work_start: int, work_end: int, slot_duration: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET work_start=?, work_end=?, slot_duration=? WHERE id=?",
            (work_start, work_end, slot_duration, master_id)
        )
        await db.commit()


async def get_all_masters() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, telegram_id FROM masters") as cur:
            return await cur.fetchall()


async def get_reminder_days(telegram_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT reminder_days FROM masters WHERE telegram_id=?", (telegram_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] else 40


async def get_reminder_days_by_master(master_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT reminder_days FROM masters WHERE id=?", (master_id,)
        ) as cur:
            row = await cur.fetchone()
            return row[0] if row and row[0] else 40


async def update_reminder_days(telegram_id: int, days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET reminder_days=? WHERE telegram_id=?", (days, telegram_id)
        )
        await db.commit()


async def update_reminder_days_by_master(master_id: int, days: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE masters SET reminder_days=? WHERE id=?", (days, master_id)
        )
        await db.commit()


# ── Клиенты ───────────────────────────────────────────────────────────

async def add_client(master_id: int, name: str, phone: str, notes: str = "") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO clients (master_id, name, phone, notes) VALUES (?, ?, ?, ?)",
            (master_id, name, phone, notes)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            return (await cur.fetchone())[0]


async def add_client_with_telegram(master_id: int, name: str, phone: str, telegram_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO clients (master_id, name, phone, notes, telegram_id) VALUES (?, ?, ?, '', ?)",
            (master_id, name, phone, telegram_id)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            return (await cur.fetchone())[0]


async def get_client_by_telegram(master_id: int, telegram_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, phone FROM clients WHERE master_id=? AND telegram_id=?",
            (master_id, telegram_id)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "name": row[1], "phone": row[2]}


async def get_clients(master_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT c.id, c.name, c.phone, c.notes,
                   MAX(a.appointment_date) as last_visit
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = ?
            GROUP BY c.id
            ORDER BY c.name
        """, (master_id,)) as cur:
            return await cur.fetchall()


async def get_client(client_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, phone, notes FROM clients WHERE id = ?", (client_id,)
        ) as cur:
            row = await cur.fetchone()
    if not row:
        return None
    return {"id": row[0], "name": row[1], "phone": row[2], "notes": row[3]}


async def update_client(client_id: int, master_id: int, name: str, phone: str, notes: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        result = await db.execute(
            "UPDATE clients SET name=?, phone=?, notes=? WHERE id=? AND master_id=?",
            (name, phone, notes, client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0


async def delete_client(client_id: int, master_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM appointments WHERE client_id = ?", (client_id,))
        result = await db.execute(
            "DELETE FROM clients WHERE id = ? AND master_id = ?", (client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0


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
        """, (master_id,)) as cur:
            all_clients = await cur.fetchall()
    q = query.lower()
    return [c for c in all_clients if q in c[1].lower()]


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
        """, (master_id, days)) as cur:
            return await cur.fetchall()


# ── Записи ────────────────────────────────────────────────────────────

async def add_appointment(
    client_id: int, master_id: int, procedure: str,
    appointment_date: str, price: int = 0,
    notes: str = "", photo_id: str = "",
    time: str = "", status: str = "confirmed"
) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO appointments
            (client_id, master_id, procedure, appointment_date, price, notes, photo_id, time, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (client_id, master_id, procedure, appointment_date, price, notes, photo_id, time, status))
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            return (await cur.fetchone())[0]


async def get_client_history(client_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT procedure, appointment_date, price, notes, photo_id
            FROM appointments
            WHERE client_id = ?
            ORDER BY appointment_date DESC
            LIMIT 10
        """, (client_id,)) as cur:
            return await cur.fetchall()


async def update_appointment_status(appointment_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE appointments SET status=? WHERE id=?", (status, appointment_id)
        )
        await db.commit()


async def get_appointment_client_telegram(appointment_id: int) -> tuple:
    """Возвращает (client_telegram_id, date, time) для уведомления клиента"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT c.telegram_id, a.appointment_date, a.time
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.id = ?
        """, (appointment_id,)) as cur:
            row = await cur.fetchone()
    if not row:
        return None, None, None
    return row[0], row[1], row[2]


# ── Слоты и расписание ────────────────────────────────────────────────

async def get_busy_slots(master_id: int, date: str) -> list[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT time FROM appointments WHERE master_id=? AND appointment_date=? AND status != 'cancelled'",
            (master_id, date)
        ) as cur:
            rows = await cur.fetchall()
    return [r[0] for r in rows if r[0]]


async def get_available_slots(
    master_id: int, date: str,
    work_start: int, work_end: int, slot_duration: int
) -> list[str]:
    """Вычисляет свободные временные слоты на день"""
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
    """Расписание мастера на конкретный день"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.name, a.procedure, a.time, a.status, c.phone
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            WHERE a.master_id=? AND a.appointment_date=?
            ORDER BY a.time, a.id
        """, (master_id, date)) as cur:
            return await cur.fetchall()


# ── Напоминания клиентам ──────────────────────────────────────────────

async def get_appointments_for_reminder_24h(target_date: str) -> list:
    """Записи на завтра, которым ещё не отправили 24h-напоминание"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date = ?
              AND a.status != 'cancelled'
              AND a.reminder_24h_sent = 0
              AND c.telegram_id IS NOT NULL
        """, (target_date,)) as cur:
            return await cur.fetchall()


async def get_appointments_for_reminder_2h(target_date: str, target_time_from: str, target_time_to: str) -> list:
    """Записи через ~2 часа, которым ещё не отправили 2h-напоминание"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.telegram_id, c.name, m.telegram_id,
                   a.appointment_date, a.time, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date = ?
              AND a.time BETWEEN ? AND ?
              AND a.status != 'cancelled'
              AND a.reminder_2h_sent = 0
              AND c.telegram_id IS NOT NULL
        """, (target_date, target_time_from, target_time_to)) as cur:
            return await cur.fetchall()


async def mark_reminder_sent(appointment_id: int, reminder_type: str):
    """reminder_type: '24h' или '2h'"""
    col = "reminder_24h_sent" if reminder_type == "24h" else "reminder_2h_sent"
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"UPDATE appointments SET {col}=1 WHERE id=?", (appointment_id,))
        await db.commit()


async def get_appointments_for_correction_reminder(three_weeks_ago: str) -> list:
    """Визиты ровно 3 недели назад, статус confirmed, без отправленного напоминания"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT a.id, c.telegram_id, c.name, m.name, a.procedure
            FROM appointments a
            JOIN clients c ON c.id = a.client_id
            JOIN masters m ON m.id = a.master_id
            WHERE a.appointment_date = ?
              AND a.status = 'confirmed'
              AND a.correction_reminder_sent = 0
              AND c.telegram_id IS NOT NULL
        """, (three_weeks_ago,)) as cur:
            return await cur.fetchall()


async def mark_correction_reminder_sent(appointment_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE appointments SET correction_reminder_sent=1 WHERE id=?", (appointment_id,)
        )
        await db.commit()


# ── Абонементы ────────────────────────────────────────────────────────

async def add_subscription(master_id: int, client_id: int, name: str, total: int, price: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subscriptions (master_id, client_id, name, total_sessions, price) VALUES (?,?,?,?,?)",
            (master_id, client_id, name, total, price)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cur:
            return (await cur.fetchone())[0]


async def get_client_subscriptions(client_id: int, master_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT id, name, total_sessions, used_sessions, price
            FROM subscriptions
            WHERE client_id=? AND master_id=?
            ORDER BY created_at DESC
        """, (client_id, master_id)) as cur:
            return await cur.fetchall()


async def use_subscription_session(sub_id: int, master_id: int) -> bool:
    """Списывает один сеанс. Возвращает False если сеансы исчерпаны."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT used_sessions, total_sessions FROM subscriptions WHERE id=? AND master_id=?",
            (sub_id, master_id)
        ) as cur:
            row = await cur.fetchone()
        if not row or row[0] >= row[1]:
            return False
        await db.execute(
            "UPDATE subscriptions SET used_sessions=used_sessions+1 WHERE id=?", (sub_id,)
        )
        await db.commit()
        return True


# ── Статистика ────────────────────────────────────────────────────────

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
            "WHERE master_id=? GROUP BY procedure ORDER BY cnt DESC LIMIT 3",
            (master_id,)
        ) as c:
            top_procedures = await c.fetchall()

    return {
        "total_clients": total_clients,
        "total_appointments": total_appointments,
        "total_earnings": total_earnings,
        "month_earnings": month_earnings,
        "top_procedures": top_procedures,
    }
