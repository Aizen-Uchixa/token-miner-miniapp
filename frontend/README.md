# Frontend (React + Vite) - Telegram Mini App

## Features
- Uses Telegram WebApp SDK when opened from Telegram.
- Reads `initData` and calls backend `GET /me` with `X-TG-INITDATA` header.
- Shows basic profile: username, coins, gems, prestige.
- Tabs: Home / Mine / Managers.
- Canvas animation with placeholder shapes (miner/elevator/courier).
- Dev mode fallback: if Telegram SDK is missing, paste initData manually.

## .env.local (example)
Create `frontend/.env.local`:
```env
VITE_API_URL=http://localhost:8000
```

## Run local
```bash
cd frontend
npm install
npm run dev
```

Then open `http://localhost:5173`.
For real Mini App behavior, open inside Telegram via bot button.
