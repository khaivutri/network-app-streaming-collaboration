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
    return new Promise((resolve, reject) => {
      const xhr = new XMLHttpRequest();
      xhr.open("POST", "/api/v1/files/send");
      xhr.withCredentials = true;
      xhr.setRequestHeader("Accept", "application/json");
      if (csrfToken) xhr.setRequestHeader("X-Client-CSRF-Token", csrfToken);
      xhr.upload.onprogress = (event) => {
        if (event.lengthComputable && onProgress) onProgress(Math.min(100, event.loaded / event.total * 100), event.loaded, event.total);
      };
      xhr.onerror = () => reject(new Error("Không thể kết nối để nạp tệp lên agent."));
      xhr.onabort = () => reject(new Error("Đã huỷ nạp tệp lên agent."));
      xhr.onload = () => {
        const contentType = xhr.getResponseHeader("content-type") || "";
        if (!contentType.includes("application/json")) { reject(new Error(`Unexpected response content type: ${contentType}`)); return; }
        let envelope;
        try { envelope = JSON.parse(xhr.responseText); } catch { reject(new Error("Phản hồi tải tệp không hợp lệ.")); return; }
        if (xhr.status < 200 || xhr.status >= 300 || !envelope.ok) { reject(new Error(envelope.error?.message || `HTTP ${xhr.status}`)); return; }
        if (onProgress) onProgress(100, file.size, file.size);
        resolve(envelope.data);
      };
      xhr.send(body);
    });
  },
  async cancelFile(id) { return request(`/api/v1/files/${encodeURIComponent(id)}/cancel`, { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  async retryFile(id) { return request(`/api/v1/files/${encodeURIComponent(id)}/retry`, { method: "POST", body: "{}", headers: { "Content-Type": "application/json" } }); },
  downloadURL(id) { return `/api/v1/files/${encodeURIComponent(id)}/download`; },
};
