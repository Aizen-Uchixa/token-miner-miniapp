import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware

from auth import verify_init_data
from db import get_or_create_player, init_db

load_dotenv()
load_dotenv(dotenv_path=Path(__file__).resolve().parent.parent / ".env")

BOT_TOKEN = (
    os.getenv("TELEGRAM_TOKEN")
    or os.getenv("BOT_TOKEN")
    or os.getenv("TG_TOKEN")
    or os.getenv("TOKEN")
    or ""
).strip()

app = FastAPI(title="Tycoon Mini App Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup() -> None:
    await init_db()


@app.get("/health")
async def health() -> dict:
    return {"ok": True}


@app.get("/me")
async def me(x_tg_initdata: str = Header(default="", alias="X-TG-INITDATA")) -> dict:
    if not BOT_TOKEN:
        raise HTTPException(status_code=500, detail="Backend BOT_TOKEN is missing")

    try:
        user = verify_init_data(x_tg_initdata, BOT_TOKEN)
    except ValueError as exc:
        raise HTTPException(status_code=401, detail=str(exc)) from exc

    username = user.get("username") or user.get("first_name") or "Player"
    profile = await get_or_create_player(int(user["id"]), username)
    return profile.to_dict()
