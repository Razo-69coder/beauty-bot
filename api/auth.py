import hmac
import hashlib
import json
from urllib.parse import unquote


def validate_init_data(init_data: str, bot_token: str) -> dict | None:
    """Проверяет подпись Telegram initData, возвращает данные пользователя или None"""
    try:
        vals = dict(x.split("=", 1) for x in init_data.split("&"))
        received_hash = vals.pop("hash", None)
        if not received_hash:
            return None

        data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(vals.items()))

        secret_key = hmac.new(b"WebAppData", bot_token.encode(), hashlib.sha256).digest()
        computed_hash = hmac.new(secret_key, data_check_string.encode(), hashlib.sha256).hexdigest()

        if not hmac.compare_digest(computed_hash, received_hash):
            return None

        return json.loads(unquote(vals.get("user", "{}")))
    except Exception:
        return None
