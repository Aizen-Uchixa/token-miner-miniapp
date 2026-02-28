const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000";

export async function fetchMe(initData) {
  const response = await fetch(`${API_URL}/me`, {
    headers: {
      "X-TG-INITDATA": initData || "",
    },
  });

  if (!response.ok) {
    let message = `HTTP ${response.status}`;
    try {
      const payload = await response.json();
      message = payload.detail || message;
    } catch {
      // ignore
    }
    throw new Error(message);
  }

  return response.json();
}
