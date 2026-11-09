let csrfToken = "";

async function request(path, options = {}) {
  const headers = new Headers(options.headers || {});
  headers.set("Accept", "application/json");
  if (csrfToken && options.method && options.method !== "GET") {
    headers.set("X-Client-CSRF-Token", csrfToken);
  }
  const response = await fetch(path, { ...options, headers, credentials: "same-origin" });
  const contentType = response.headers.get("content-type") || "";
  if (!contentType.includes("application/json")) {
    throw new Error(`Unexpected response content type: ${contentType}`);
  }
  const envelope = await response.json();
  if (!response.ok || !envelope.ok) {
    throw new Error(envelope.error?.message || `HTTP ${response.status}`);
  }
  return envelope.data;
}

export const API = {
  async getSession() {
    const data = await request("/api/v1/session");
    csrfToken = data.csrf_token || csrfToken;
    return data;
  },
  async login(username, password) {
    return request("/api/v1/auth/login", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  },
  async logout() { return request("/api/v1/auth/logout", { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  async peers() { return request("/api/v1/peers"); },
  async refreshPeers() { return request("/api/v1/peers/refresh", { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  async broadcast(clientMsgID, content) {
    return request("/api/v1/messages/broadcast", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ client_msg_id: clientMsgID, content }) });
  },
  async unicast(clientMsgID, recipient, content) {
    return request("/api/v1/messages/unicast", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ client_msg_id: clientMsgID, recipient, content }) });
  },
  async sendFile(recipient, caption, file, onProgress) {
    const body = new FormData();
    body.set("recipient", recipient);
    body.set("caption", caption || "");
    body.set("file", file, file.name);
    // fetch does not expose upload progress; SSE remains the source of truth.
    if (onProgress) onProgress(0);
    const result = await request("/api/v1/files/send", { method: "POST", body });
    if (onProgress) onProgress(100);
    return result;
  },
  async cancelFile(id) { return request(`/api/v1/files/${encodeURIComponent(id)}/cancel`, { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  async retryFile(id) { return request(`/api/v1/files/${encodeURIComponent(id)}/retry`, { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  downloadURL(id) { return `/api/v1/files/${encodeURIComponent(id)}/download`; },
};
