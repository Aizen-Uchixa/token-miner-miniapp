import hashlib
import hmac
import json
from urllib.parse import parse_qsl


def _parse_init_data(init_data: str) -> tuple[dict[str, str], str, str]:
    pairs = dict(parse_qsl(init_data, keep_blank_values=True))
    provided_hash = pairs.pop("hash", "")
    data_check_string = "\n".join(f"{k}={v}" for k, v in sorted(pairs.items()))
    return pairs, provided_hash, data_check_string


def verify_init_data(init_data: str, bot_token: str) -> dict:
    if not init_data:
        raise ValueError("Missing initData")
    if not bot_token:
        raise ValueError("Missing bot token")

    parsed, provided_hash, data_check_string = _parse_init_data(init_data)
    if not provided_hash:
        raise ValueError("Missing hash in initData")

    secret_key = hmac.new(b"WebAppData", bot_token.encode("utf-8"), hashlib.sha256).digest()
    expected_hash = hmac.new(secret_key, data_check_string.encode("utf-8"), hashlib.sha256).hexdigest()

    if not hmac.compare_digest(expected_hash, provided_hash):
        raise ValueError("Invalid initData signature")

    user_raw = parsed.get("user")
    if not user_raw:
        raise ValueError("Missing user in initData")

    try:
        user = json.loads(user_raw)
    except json.JSONDecodeError as exc:
        raise ValueError("Invalid user payload in initData") from exc

    if "id" not in user:
        raise ValueError("Missing user id in initData")

    return user
