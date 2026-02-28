# Backend (FastAPI) - Telegram Mini App

## What it does
- Verifies Telegram Mini App `initData` signature.
- Reads player data from existing `game.db` (`players` table, JSON payload).
- Creates player row if it does not exist.
- Exposes:
  - `GET /health`
  - `GET /me` (requires header `X-TG-INITDATA`)

## .env
Backend uses root project `.env`.
Required:
- `TELEGRAM_TOKEN` (or `BOT_TOKEN` / `TG_TOKEN` / `TOKEN`)
Optional:
- `BACKEND_DB_PATH` (default: `../game.db`)

## Run local
```bash
cd backend
pip install -r requirements.txt
uvicorn app:app --reload --port 8000
```

## Test quickly
```bash
curl http://127.0.0.1:8000/health
```

`/me` must be called from Telegram Mini App with valid `X-TG-INITDATA`.
