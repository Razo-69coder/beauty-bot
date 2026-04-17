import aiosqlite
from config import DB_PATH
from datetime import datetime


async def init_db():
    """Создаёт таблицы при первом запуске"""
    async with aiosqlite.connect(DB_PATH) as db:
        # Мастера (каждый пользователь бота — мастер)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS masters (
                id INTEGER PRIMARY KEY,
                telegram_id INTEGER UNIQUE,
                name TEXT,
                reminder_days INTEGER DEFAULT 40,
                created_at TEXT DEFAULT (datetime('now'))
            )
        """)
        # Добавляем колонку reminder_days если база уже существует
        try:
            await db.execute("ALTER TABLE masters ADD COLUMN reminder_days INTEGER DEFAULT 40")
            await db.commit()
        except Exception:
            pass
        # Клиенты мастера
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER,
                name TEXT,
                phone TEXT,
                notes TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (master_id) REFERENCES masters(id)
            )
        """)
        # Записи (процедуры)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS appointments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                client_id INTEGER,
                master_id INTEGER,
                procedure TEXT,
                appointment_date TEXT,
                price INTEGER,
                notes TEXT,
                photo_id TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (client_id) REFERENCES clients(id)
            )
        """)
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
            "INSERT INTO masters (telegram_id, name) VALUES (?, ?)",
            (telegram_id, name)
        )
        await db.commit()
        async with db.execute(
            "SELECT id FROM masters WHERE telegram_id = ?", (telegram_id,)
        ) as cursor:
            row = await cursor.fetchone()
            return row[0]


async def add_client(master_id: int, name: str, phone: str, notes: str = "") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO clients (master_id, name, phone, notes) VALUES (?, ?, ?, ?)",
            (master_id, name, phone, notes)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            row = await cursor.fetchone()
            return row[0]


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
        """, (master_id,)) as cursor:
            return await cursor.fetchall()


async def get_client(client_id: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT id, name, phone, notes FROM clients WHERE id = ?",
            (client_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {"id": row[0], "name": row[1], "phone": row[2], "notes": row[3]}


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
            row = await cursor.fetchone()
            return row[0]


async def get_client_history(client_id: int) -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT procedure, appointment_date, price, notes, photo_id
            FROM appointments
            WHERE client_id = ?
            ORDER BY appointment_date DESC
            LIMIT 10
        """, (client_id,)) as cursor:
            return await cursor.fetchall()


async def get_inactive_clients(master_id: int, days: int) -> list:
    """Возвращает клиентов, которые не приходили больше N дней"""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT c.id, c.name, c.phone,
                   MAX(a.appointment_date) as last_visit,
                   CAST(julianday('now') - julianday(MAX(a.appointment_date)) AS INTEGER) as days_ago
            FROM clients c
            LEFT JOIN appointments a ON a.client_id = c.id
            WHERE c.master_id = ?
            GROUP BY c.id
            HAVING last_visit IS NOT NULL
               AND days_ago >= ?
            ORDER BY days_ago DESC
        """, (master_id, days)) as cursor:
            return await cursor.fetchall()


async def search_clients(master_id: int, query: str) -> list:
    """Ищет клиентов по имени (частичное совпадение, без учёта регистра, включая кириллицу)"""
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
    return [c for c in all_clients if query_lower in c[1].lower()]


async def get_all_masters() -> list:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, telegram_id FROM masters") as cursor:
            return await cursor.fetchall()


async def update_client(client_id: int, master_id: int, name: str, phone: str, notes: str) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        result = await db.execute(
            "UPDATE clients SET name=?, phone=?, notes=? WHERE id=? AND master_id=?",
            (name, phone, notes, client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0


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


async def delete_client(client_id: int, master_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "DELETE FROM appointments WHERE client_id = ?", (client_id,)
        )
        result = await db.execute(
            "DELETE FROM clients WHERE id = ? AND master_id = ?",
            (client_id, master_id)
        )
        await db.commit()
        return result.rowcount > 0
