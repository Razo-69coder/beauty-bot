"""Apple Wallet: одна карта на пару «мастер — клиентка».

Карта живёт в Wallet постоянно и обновляется сама:
- есть ближайшая запись → дата, время, услуга, адрес, статус;
- записи нет → «Записаться снова» (или «Пора на коррекцию»);
- штампы за визиты — из существующей программы лояльности мастера (если включена).
Обновление: когда запись меняется, сервер шлёт пустой пуш на устройства с картой (APNs,
сертификат Pass Type ID), Wallet сам скачивает свежую версию через веб-сервис ниже.
"""
import base64
import hashlib
import io
import json
import os
import secrets
import tempfile
import zipfile
from datetime import datetime as _dt, timedelta

from fastapi import APIRouter, Header, HTTPException, Request, Response

from database import get_pool

router = APIRouter()

PASS_TYPE_ID = os.getenv("WALLET_PASS_TYPE_ID", "pass.com.solvobeauty.card")
TEAM_ID = "53C33F97M2"
PUBLIC_BASE = os.getenv("PUBLIC_BASE_URL", "https://beauty-bot-44ou.onrender.com")
_WWDR_PATH = os.path.join(os.path.dirname(__file__), "wallet_assets", "AppleWWDRCAG4.cer")

# Цвета карты — те же 6, что в макете и в приложении
PALETTES = {
    "berry":    {"name": "Ягода",   "bg": (42, 14, 31),   "fg": (255, 241, 246), "label": (255, 122, 174), "strip": (74, 22, 52),   "d1": (255, 62, 134),  "d2": (194, 27, 255)},
    "nude":     {"name": "Нюд",     "bg": (239, 228, 216), "fg": (43, 29, 20),    "label": (122, 78, 49),   "strip": (217, 195, 172), "d1": (185, 138, 100), "d2": (247, 239, 230)},
    "emerald":  {"name": "Изумруд", "bg": (15, 42, 38),   "fg": (234, 246, 242), "label": (127, 209, 185), "strip": (23, 66, 59),   "d1": (46, 139, 118),  "d2": (127, 209, 185)},
    "graphite": {"name": "Графит",  "bg": (22, 22, 22),   "fg": (245, 245, 245), "label": (212, 180, 131), "strip": (42, 42, 42),   "d1": (212, 180, 131), "d2": (90, 90, 90)},
    "lavender": {"name": "Лаванда", "bg": (233, 227, 246), "fg": (35, 26, 58),    "label": (91, 63, 148),   "strip": (207, 194, 236), "d1": (156, 132, 214), "d2": (245, 241, 252)},
    "powder":   {"name": "Пудра",   "bg": (248, 227, 230), "fg": (58, 24, 32),    "label": (163, 56, 79),   "strip": (239, 197, 204), "d1": (217, 120, 139), "d2": (255, 244, 246)},
}
_RU_MON = ["янв", "фев", "мар", "апр", "мая", "июн", "июл", "авг", "сен", "окт", "ноя", "дек"]
_STATUS = {"confirmed": "Подтверждена", "pending": "Ждёт ответа"}


def enabled() -> bool:
    return bool(os.getenv("WALLET_CERT_B64") and os.getenv("WALLET_KEY_B64") and os.path.exists(_WWDR_PATH))


def _rgb(c) -> str:
    return f"rgb({c[0]},{c[1]},{c[2]})"


# ── Таблицы ────────────────────────────────────────────────────────────

MIGRATIONS = [
    """CREATE TABLE IF NOT EXISTS wallet_passes (
        id SERIAL PRIMARY KEY, master_id INTEGER, client_id INTEGER,
        serial TEXT UNIQUE, auth_token TEXT, link_token TEXT UNIQUE,
        updated_at TIMESTAMP DEFAULT NOW(), created_at TIMESTAMP DEFAULT NOW())""",
    "CREATE UNIQUE INDEX IF NOT EXISTS idx_wallet_passes_mc ON wallet_passes (master_id, client_id)",
    """CREATE TABLE IF NOT EXISTS wallet_registrations (
        device_id TEXT, push_token TEXT, serial TEXT, created_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (device_id, serial))""",
    "ALTER TABLE masters ADD COLUMN IF NOT EXISTS wallet_color TEXT DEFAULT 'berry'",
    "ALTER TABLE masters ADD COLUMN IF NOT EXISTS wallet_show_price BOOLEAN DEFAULT TRUE",
    "ALTER TABLE masters ADD COLUMN IF NOT EXISTS wallet_show_stamps BOOLEAN DEFAULT TRUE",
    "ALTER TABLE masters ADD COLUMN IF NOT EXISTS wallet_rules TEXT DEFAULT ''",
    "ALTER TABLE masters ADD COLUMN IF NOT EXISTS wallet_address TEXT DEFAULT ''",
]


async def pass_link(master_id: int, client_id: int) -> str:
    """Ссылка «Добавить в Apple Wallet» для клиентки (создаёт карту при первом обращении)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT link_token FROM wallet_passes WHERE master_id=$1 AND client_id=$2", master_id, client_id)
        if not row:
            row = await conn.fetchrow(
                """INSERT INTO wallet_passes (master_id, client_id, serial, auth_token, link_token)
                   VALUES ($1,$2,$3,$4,$5)
                   ON CONFLICT (master_id, client_id) DO UPDATE SET master_id=EXCLUDED.master_id
                   RETURNING link_token""",
                master_id, client_id, f"sb-{master_id}-{client_id}-{secrets.token_hex(4)}",
                secrets.token_urlsafe(24), secrets.token_urlsafe(16))
    return f"{PUBLIC_BASE}/wallet/pass/{row['link_token']}"


# ── Данные для карты ───────────────────────────────────────────────────

async def _pass_data(p) -> dict:
    pool = await get_pool()
    async with pool.acquire() as conn:
        m = await conn.fetchrow(
            """SELECT id, name, booking_link, COALESCE(phone,'') AS phone, COALESCE(timezone_offset,3) AS tz,
                      COALESCE(wallet_color,'berry') AS color, COALESCE(wallet_show_price,TRUE) AS show_price,
                      COALESCE(wallet_show_stamps,TRUE) AS show_stamps, COALESCE(wallet_rules,'') AS rules,
                      COALESCE(wallet_address,'') AS address,
                      COALESCE(loyalty_discount_enabled,FALSE) AS loy_on, COALESCE(loyalty_threshold,10) AS loy_n,
                      COALESCE(loyalty_discount_type,'percent') AS loy_type,
                      COALESCE(loyalty_discount_percent,10) AS loy_pct, COALESCE(loyalty_discount_rub,0) AS loy_rub
               FROM masters WHERE id=$1""", p["master_id"])
        c = await conn.fetchrow("SELECT id, name FROM clients WHERE id=$1", p["client_id"])
        local_now = _dt.utcnow() + timedelta(hours=m["tz"])
        today = local_now.strftime("%Y-%m-%d")
        nxt = await conn.fetchrow(
            """SELECT appointment_date, time, procedure, price, status, COALESCE(duration_min,0) AS dur
               FROM appointments WHERE master_id=$1 AND client_id=$2
                 AND status NOT IN ('cancelled','no_show','completed') AND appointment_date >= $3
               ORDER BY appointment_date, time LIMIT 1""", m["id"], c["id"], today)
        last = await conn.fetchrow(
            """SELECT appointment_date, procedure FROM appointments
               WHERE master_id=$1 AND client_id=$2 AND status='completed'
               ORDER BY appointment_date DESC, time DESC LIMIT 1""", m["id"], c["id"])
        visits = await conn.fetchval(
            "SELECT COUNT(*) FROM appointments WHERE master_id=$1 AND client_id=$2 AND status='completed'",
            m["id"], c["id"])
    return {"m": m, "c": c, "next": nxt, "last": last, "visits": visits or 0, "local_now": local_now}


def _fmt_date(d: str) -> str:
    dt = _dt.strptime(str(d)[:10], "%Y-%m-%d")
    return f"{dt.day} {_RU_MON[dt.month - 1]}"


def _pass_json(p, d: dict) -> dict:
    m, c, nxt, last = d["m"], d["c"], d["next"], d["last"]
    pal = PALETTES.get(m["color"], PALETTES["berry"])
    slug = m["booking_link"] or ""
    book_url = f"{PUBLIC_BASE}/book/{slug}" if slug else PUBLIC_BASE
    my_url = f"https://solvobeauty.vercel.app/my/{slug}" if slug else book_url

    header, primary, secondary, auxiliary = [], [], [], []
    relevant = None
    if nxt:
        header.append({"key": "date", "label": "ДАТА", "value": _fmt_date(nxt["appointment_date"]),
                       "changeMessage": "Запись перенесена: %@"})
        primary.append({"key": "time", "label": "ВРЕМЯ", "value": nxt["time"], "changeMessage": "Новое время записи: %@"})
        secondary.append({"key": "service", "label": "УСЛУГА", "value": nxt["procedure"] or "Запись"})
        if m["address"]:
            secondary.append({"key": "address", "label": "АДРЕС", "value": m["address"]})
        if m["show_price"] and (nxt["price"] or 0) > 0:
            auxiliary.append({"key": "price", "label": "ЦЕНА", "value": f"{nxt['price']:,} ₽".replace(",", " ")})
        auxiliary.append({"key": "status", "label": "СТАТУС", "value": _STATUS.get(nxt["status"], "Записана")})
        tz = m["tz"]
        sign = "+" if tz >= 0 else "-"
        relevant = f"{str(nxt['appointment_date'])[:10]}T{nxt['time']}:00{sign}{abs(tz):02d}:00"
    else:
        header.append({"key": "date", "label": "СЛЕДУЮЩИЙ ВИЗИТ", "value": "—"})
        weeks = None
        if last:
            days = (d["local_now"].date() - _dt.strptime(str(last["appointment_date"])[:10], "%Y-%m-%d").date()).days
            weeks = days // 7 if days >= 21 else None
        if weeks:
            primary.append({"key": "cta", "label": "ПОРА НА КОРРЕКЦИЮ", "value": f"Прошло {weeks} нед."})
        else:
            primary.append({"key": "cta", "label": "ЖДЁМ ВАС СНОВА", "value": "Записаться снова"})
        if last:
            secondary.append({"key": "last", "label": "ПОСЛЕДНИЙ ВИЗИТ", "value": _fmt_date(last["appointment_date"])})
            secondary.append({"key": "service", "label": "УСЛУГА", "value": last["procedure"] or ""})
        auxiliary.append({"key": "hint", "label": "ЗАПИСЬ", "value": "Нажмите ⓘ → «Записаться»"})

    if m["loy_on"] and m["show_stamps"]:
        n = max(int(m["loy_n"] or 10), 2)
        v = d["visits"]
        gift = f"−{m['loy_pct']}%" if m["loy_type"] != "rub" else f"−{m['loy_rub']} ₽"
        if v > 0 and v % n == 0:
            label, dots = f"ПОДАРОК: {gift} НА СЛЕДУЮЩИЙ ВИЗИТ", "●" * n
        else:
            k = v % n
            label, dots = f"ВИЗИТЫ {k} / {n} · ПОДАРОК {gift}", "●" * k + "○" * (n - k)
        auxiliary.append({"key": "stamps", "label": label, "value": dots, "changeMessage": "Визиты: %@"})

    back = [
        {"key": "manage", "label": "Перенести или отменить запись", "value": my_url,
         "attributedValue": f"<a href='{my_url}'>Открыть</a>"},
        {"key": "book", "label": "Записаться снова", "value": book_url,
         "attributedValue": f"<a href='{book_url}'>Выбрать время</a>"},
    ]
    if m["address"]:
        back.append({"key": "addr", "label": "Адрес", "value": m["address"]})
    if m["phone"]:
        back.append({"key": "phone", "label": "Телефон мастера", "value": m["phone"]})
    if m["rules"]:
        back.append({"key": "rules", "label": "Правила мастера", "value": m["rules"]})
    back.append({"key": "made", "label": "", "value": "Запись через Solvo Beauty"})

    pj = {
        "formatVersion": 1,
        "passTypeIdentifier": PASS_TYPE_ID,
        "teamIdentifier": TEAM_ID,
        "serialNumber": p["serial"],
        "authenticationToken": p["auth_token"],
        "webServiceURL": f"{PUBLIC_BASE}/wallet",
        "organizationName": m["name"] or "Solvo Beauty",
        "description": f"Запись к мастеру {m['name'] or ''}".strip(),
        "logoText": m["name"] or "Solvo Beauty",
        "backgroundColor": _rgb(pal["bg"]),
        "foregroundColor": _rgb(pal["fg"]),
        "labelColor": _rgb(pal["label"]),
        "storeCard": {
            "headerFields": header, "primaryFields": primary, "secondaryFields": secondary,
            "auxiliaryFields": auxiliary, "backFields": back,
        },
        "sharingProhibited": True,
    }
    if relevant:
        pj["relevantDate"] = relevant
    return pj


# ── Картинки (узор в цвете мастера, без фото) ──────────────────────────

def _images(color: str, initial: str) -> dict:
    from PIL import Image, ImageDraw
    pal = PALETTES.get(color, PALETTES["berry"])
    out = {}

    def png(img) -> bytes:
        buf = io.BytesIO()
        img.save(buf, "PNG")
        return buf.getvalue()

    for scale, suffix in ((1, ""), (2, "@2x"), (3, "@3x")):
        w, h = 375 * scale, 144 * scale
        img = Image.new("RGB", (w, h), pal["strip"])
        dr = ImageDraw.Draw(img)
        r1 = 105 * scale
        dr.ellipse((w - r1 * 1.4, -r1 * 0.6, w - r1 * 1.4 + 2 * r1, -r1 * 0.6 + 2 * r1), fill=pal["d1"])
        r2 = 55 * scale
        cx, cy = w - 175 * scale, 100 * scale
        dr.ellipse((cx - r2, cy - r2, cx + r2, cy + r2), fill=pal["d2"])
        out[f"strip{suffix}.png"] = png(img)
        # Логотип/иконка — круг акцентного цвета
        for name, size in (("logo", 50), ("icon", 29)):
            s = size * scale
            im = Image.new("RGBA", (s, s), (0, 0, 0, 0))
            ImageDraw.Draw(im).ellipse((0, 0, s - 1, s - 1), fill=pal["label"] + (255,))
            out[f"{name}{suffix}.png"] = png(im)
    return out


# ── Подпись и упаковка .pkpass ─────────────────────────────────────────

def _sign(manifest: bytes) -> bytes:
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.serialization import pkcs7
    cert = x509.load_pem_x509_certificate(base64.b64decode(os.environ["WALLET_CERT_B64"]))
    key = serialization.load_pem_private_key(base64.b64decode(os.environ["WALLET_KEY_B64"]), None)
    wwdr = x509.load_der_x509_certificate(open(_WWDR_PATH, "rb").read())
    return (pkcs7.PKCS7SignatureBuilder().set_data(manifest)
            .add_signer(cert, key, hashes.SHA256()).add_certificate(wwdr)
            .sign(serialization.Encoding.DER, [pkcs7.PKCS7Options.DetachedSignature, pkcs7.PKCS7Options.Binary]))


async def build_pkpass(p) -> bytes:
    d = await _pass_data(p)
    files = {"pass.json": json.dumps(_pass_json(p, d), ensure_ascii=False).encode("utf-8")}
    files.update(_images(d["m"]["color"], (d["m"]["name"] or "S")[:1]))
    manifest = json.dumps({n: hashlib.sha1(b).hexdigest() for n, b in files.items()}).encode("utf-8")
    files["manifest.json"] = manifest
    files["signature"] = _sign(manifest)
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for n, b in files.items():
            z.writestr(n, b)
    return buf.getvalue()


def _pkpass_response(data: bytes, updated_at) -> Response:
    headers = {"Content-Disposition": 'attachment; filename="SolvoBeauty.pkpass"', "Cache-Control": "no-store"}
    if updated_at:
        headers["Last-Modified"] = updated_at.strftime("%a, %d %b %Y %H:%M:%S GMT")
    return Response(content=data, media_type="application/vnd.apple.pkpass", headers=headers)


# ── Скачивание карты клиенткой ─────────────────────────────────────────

@router.get("/wallet/pass/{link_token}")
async def wallet_download(link_token: str):
    if not enabled():
        raise HTTPException(503, "Apple Wallet пока недоступен")
    pool = await get_pool()
    async with pool.acquire() as conn:
        p = await conn.fetchrow("SELECT * FROM wallet_passes WHERE link_token=$1", link_token)
    if not p:
        raise HTTPException(404, "Карта не найдена")
    return _pkpass_response(await build_pkpass(p), p["updated_at"])


# ── Веб-сервис PassKit (Wallet сам ходит сюда за обновлениями) ─────────

async def _auth_pass(serial: str, authorization: str | None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        p = await conn.fetchrow("SELECT * FROM wallet_passes WHERE serial=$1", serial)
    if not p or authorization != f"ApplePass {p['auth_token']}":
        raise HTTPException(401)
    return p


@router.post("/wallet/v1/devices/{device_id}/registrations/{pass_type}/{serial}")
async def wallet_register(device_id: str, pass_type: str, serial: str, request: Request,
                          authorization: str | None = Header(None)):
    await _auth_pass(serial, authorization)
    body = await request.json()
    pool = await get_pool()
    async with pool.acquire() as conn:
        res = await conn.execute(
            """INSERT INTO wallet_registrations (device_id, push_token, serial) VALUES ($1,$2,$3)
               ON CONFLICT (device_id, serial) DO UPDATE SET push_token=EXCLUDED.push_token""",
            device_id, body.get("pushToken", ""), serial)
    return Response(status_code=201 if res.endswith(" 1") else 200)


@router.delete("/wallet/v1/devices/{device_id}/registrations/{pass_type}/{serial}")
async def wallet_unregister(device_id: str, pass_type: str, serial: str, authorization: str | None = Header(None)):
    await _auth_pass(serial, authorization)
    pool = await get_pool()
    async with pool.acquire() as conn:
        await conn.execute("DELETE FROM wallet_registrations WHERE device_id=$1 AND serial=$2", device_id, serial)
    return Response(status_code=200)


@router.get("/wallet/v1/devices/{device_id}/registrations/{pass_type}")
async def wallet_updated_serials(device_id: str, pass_type: str, passesUpdatedSince: str | None = None):
    pool = await get_pool()
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """SELECT p.serial, p.updated_at FROM wallet_registrations r JOIN wallet_passes p ON p.serial = r.serial
               WHERE r.device_id=$1""", device_id)
    since = float(passesUpdatedSince) if passesUpdatedSince and passesUpdatedSince.replace(".", "", 1).isdigit() else 0
    fresh = [r for r in rows if r["updated_at"] and r["updated_at"].timestamp() > since]
    if not fresh:
        return Response(status_code=204)
    last = max(r["updated_at"].timestamp() for r in fresh)
    return {"serialNumbers": [r["serial"] for r in fresh], "lastUpdated": str(int(last))}


@router.get("/wallet/v1/passes/{pass_type}/{serial}")
async def wallet_latest(pass_type: str, serial: str, authorization: str | None = Header(None)):
    p = await _auth_pass(serial, authorization)
    return _pkpass_response(await build_pkpass(p), p["updated_at"])


@router.post("/wallet/v1/log")
async def wallet_log(request: Request):
    try:
        print(f"[WALLET LOG] {(await request.json()).get('logs')}")
    except Exception:
        pass
    return Response(status_code=200)


# ── Обновление карт ────────────────────────────────────────────────────

async def _push(tokens: list):
    if not tokens or not enabled():
        return
    import httpx
    cert_pem = base64.b64decode(os.environ["WALLET_CERT_B64"])
    key_pem = base64.b64decode(os.environ["WALLET_KEY_B64"])
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as cf, \
            tempfile.NamedTemporaryFile(delete=False, suffix=".pem") as kf:
        cf.write(cert_pem)
        kf.write(key_pem)
    try:
        async with httpx.AsyncClient(http2=True, cert=(cf.name, kf.name), timeout=10) as client:
            for t in tokens:
                try:
                    r = await client.post(f"https://api.push.apple.com/3/device/{t}", json={},
                                          headers={"apns-topic": PASS_TYPE_ID, "apns-push-type": "background"})
                    if r.status_code != 200:
                        print(f"[WALLET PUSH] {r.status_code} {r.text}")
                except Exception as e:
                    print(f"[WALLET PUSH] error: {e}")
    finally:
        for f in (cf.name, kf.name):
            try:
                os.unlink(f)
            except OSError:
                pass


async def touch(master_id: int, client_id: int | None = None):
    """Данные карты изменились — помечаем и просим Wallet обновиться."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        if client_id is None:
            rows = await conn.fetch(
                """UPDATE wallet_passes SET updated_at=NOW() WHERE master_id=$1 RETURNING serial""", master_id)
        else:
            rows = await conn.fetch(
                """UPDATE wallet_passes SET updated_at=NOW() WHERE master_id=$1 AND client_id=$2 RETURNING serial""",
                master_id, client_id)
        serials = [r["serial"] for r in rows]
        tokens = []
        if serials:
            tokens = [r["push_token"] for r in await conn.fetch(
                "SELECT push_token FROM wallet_registrations WHERE serial = ANY($1::text[])", serials) if r["push_token"]]
    await _push(tokens)


def touch_appointment(appointment_id: int):
    """Запись изменилась (создана, перенесена, отменена, выполнена) — обновляем карту клиентки в фоне."""
    import asyncio

    async def _run():
        try:
            pool = await get_pool()
            async with pool.acquire() as conn:
                row = await conn.fetchrow("SELECT master_id, client_id FROM appointments WHERE id=$1", appointment_id)
            if row:
                await touch(row["master_id"], row["client_id"])
        except Exception as e:
            print(f"[WALLET] touch error: {e}")
    try:
        asyncio.get_running_loop().create_task(_run())
    except RuntimeError:
        pass
