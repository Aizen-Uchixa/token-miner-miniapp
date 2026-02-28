import json
import os
import time
from pathlib import Path
from typing import Any

import aiosqlite

from models import MeResponse

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_DB_PATH = PROJECT_ROOT / "game.db"
DB_PATH = Path(os.getenv("BACKEND_DB_PATH", str(DEFAULT_DB_PATH))).resolve()


def _default_player(username: str) -> dict[str, Any]:
    return {
        "name": username or "Player",
        "coins": 500,
        "gems": 50,
        "prestige_points": 0,
        "active_mine": "coal",
        "company": {"name": "My Company", "level": 1, "xp": 0},
        "last_offline_check": time.time(),
    }


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("PRAGMA journal_mode=WAL;")
        await db.execute(
            """
            CREATE TABLE IF NOT EXISTS players (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                data TEXT
            )
            """
        )
        await db.commit()


async def get_or_create_player(user_id: int, username: str) -> MeResponse:
    await init_db()

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT username, data FROM players WHERE user_id = ?", (user_id,)) as cursor:
            row = await cursor.fetchone()

        if row is None:
            payload = _default_player(username)
            await db.execute(
                "INSERT INTO players (user_id, username, data) VALUES (?, ?, ?)",
                (user_id, username, json.dumps(payload, ensure_ascii=False)),
            )
            await db.commit()
            db_username = username
            data = payload
        else:
            db_username, raw = row
            try:
                data = json.loads(raw or "{}")
            except json.JSONDecodeError:
                data = _default_player(username)

    company = data.get("company", {}) if isinstance(data.get("company"), dict) else {}

    return MeResponse(
        user_id=user_id,
        username=(db_username or username or data.get("name") or "Player"),
        coins=int(data.get("coins", 0)),
        gems=int(data.get("gems", 0)),
        prestige_points=int(data.get("prestige_points", 0)),
        active_mine=str(data.get("active_mine", "coal")),
        company={
            "name": str(company.get("name", "My Company")),
            "level": int(company.get("level", 1)),
        },
    )
