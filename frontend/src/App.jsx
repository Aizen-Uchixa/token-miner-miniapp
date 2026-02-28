import { useEffect, useMemo, useState } from "react";
import { fetchMe } from "./api";
import Tabs from "./components/Tabs";
import CanvasScene from "./components/CanvasScene";

function getTelegramInitData() {
  return window.Telegram?.WebApp?.initData || "";
}

export default function App() {
  const [tab, setTab] = useState("home");
  const [profile, setProfile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [manualInitData, setManualInitData] = useState("");
  const [devMode, setDevMode] = useState(false);

  const telegramInitData = useMemo(() => getTelegramInitData(), []);

  useEffect(() => {
    const tg = window.Telegram?.WebApp;
    if (tg) {
      tg.ready();
      tg.expand();
      setDevMode(false);
      return;
    }
    setDevMode(true);
  }, []);

  useEffect(() => {
    if (devMode) return;
    if (!telegramInitData) {
      setError("Telegram initData is empty. Open app from Telegram bot.");
      return;
    }
    void loadProfile(telegramInitData);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [devMode, telegramInitData]);

  async function loadProfile(initData) {
    setLoading(true);
    setError("");
    try {
      const me = await fetchMe(initData);
      setProfile(me);
    } catch (e) {
      setError(e.message || "Failed to load profile");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main style={styles.page}>
      <h1 style={styles.title}>Tycoon Mini App</h1>

      {devMode && (
        <section style={styles.card}>
          <p style={styles.warn}>
            Telegram SDK not found. Dev mode enabled.
            Paste initData from Telegram to test /me.
          </p>
          <textarea
            style={styles.textarea}
            value={manualInitData}
            onChange={(e) => setManualInitData(e.target.value)}
            placeholder="Paste Telegram initData here"
          />
          <button style={styles.button} onClick={() => loadProfile(manualInitData)}>Load Profile</button>
        </section>
      )}

      {loading && <p style={styles.muted}>Loading...</p>}
      {error && <p style={styles.error}>{error}</p>}

      {profile && (
        <section style={styles.card}>
          <div style={styles.row}><span>User</span><b>{profile.username}</b></div>
          <div style={styles.row}><span>Coins</span><b>{profile.coins}</b></div>
          <div style={styles.row}><span>Gems</span><b>{profile.gems}</b></div>
          <div style={styles.row}><span>Prestige</span><b>{profile.prestige_points}</b></div>
          <div style={styles.row}><span>Mine</span><b>{profile.active_mine}</b></div>
        </section>
      )}

      <Tabs active={tab} onChange={setTab} />

      <section style={styles.card}>
        {tab === "home" && <p>Home dashboard: summary and quick actions will appear here.</p>}
        {tab === "mine" && <p>Mine tab: production controls and storage states can be shown here.</p>}
        {tab === "managers" && <p>Managers tab: assignments and abilities can be shown here.</p>}
      </section>

      <section style={styles.card}>
        <CanvasScene />
      </section>
    </main>
  );
}

const styles = {
  page: {
    maxWidth: 420,
    margin: "0 auto",
    minHeight: "100vh",
    background: "linear-gradient(180deg,#0a1320,#0e2034)",
    color: "#e6f2ff",
    padding: 12,
    fontFamily: "Segoe UI, Tahoma, sans-serif",
  },
  title: {
    margin: "6px 0 12px",
    fontSize: 20,
  },
  card: {
    background: "rgba(255,255,255,0.06)",
    border: "1px solid rgba(255,255,255,0.12)",
    borderRadius: 12,
    padding: 10,
    marginBottom: 10,
  },
  row: {
    display: "flex",
    justifyContent: "space-between",
    margin: "6px 0",
  },
  button: {
    border: "1px solid #4e88c4",
    background: "#2e78c7",
    color: "white",
    borderRadius: 10,
    padding: "8px 10px",
    cursor: "pointer",
  },
  textarea: {
    width: "100%",
    minHeight: 70,
    marginBottom: 8,
    borderRadius: 8,
    border: "1px solid #365a82",
    background: "#0f2236",
    color: "#d7ecff",
    padding: 8,
  },
  muted: {
    color: "#9bb6d1",
  },
  warn: {
    color: "#ffd286",
    marginTop: 0,
  },
  error: {
    color: "#ff9f9f",
  },
};
