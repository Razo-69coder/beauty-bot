import aiosqlite
import os

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
                reminder_days INTEGER DEFAULT 40
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS clients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                master_id INTEGER NOT NULL,
                name TEXT NOT NULL,
                phone TEXT,
                notes TEXT,
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
                price INTEGER DEFAULT 0,
                notes TEXT,
                photo_id TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        try:
            await db.execute(
                "ALTER TABLE masters ADD COLUMN reminder_days INTEGER DEFAULT 40"
            )
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
            "INSERT INTO masters (telegram_id, name) VALUES (?, ?)", (telegram_id, name)
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
            "SELECT id, name, phone, notes FROM clients WHERE id=?", (client_id,)
        ) as cursor:
            row = await cursor.fetchone()
            if not row:
                return None
            return {"id": row[0], "name": row[1], "phone": row[2], "notes": row[3]}


async def add_client(master_id: int, name: str, phone: str, notes: str = "") -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO clients (master_id, name, phone, notes) VALUES (?, ?, ?, ?)",
            (master_id, name, phone, notes)
        )
        await db.commit()
        async with db.execute("SELECT last_insert_rowid()") as cursor:
            return (await cursor.fetchone())[0]


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
