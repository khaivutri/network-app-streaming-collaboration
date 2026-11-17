import { API } from "../api-client.js";
import { confirmModal } from "../components/modal.js";
import { renderFileCard } from "../components/file-card.js";
import { showToast } from "../components/toast.js";

export function renderWorkspace(root, snapshot, { onOpenPeer, onChanged, onRefresh, uiState }) {
  root.replaceChildren();
  const layout = document.createElement("div");
  layout.className = "workspace-layout";
  const topbar = document.createElement("header");
  topbar.className = "app-topbar";
  const brand = document.createElement("strong");
  brand.textContent = "Hệ thống Cộng tác Nội bộ";
  const userActions = document.createElement("div");
  userActions.className = "user-actions";
  const greeting = document.createElement("span");
  greeting.textContent = `Xin chào, ${snapshot.current_user?.username || ""}`;
  userActions.append(greeting);
  topbar.append(brand, userActions);
  const sidebar = document.createElement("aside");
  sidebar.className = "sidebar";
  const peersHeading = document.createElement("h2");
  peersHeading.textContent = `THÀNH VIÊN (${snapshot.online_peers.length})`;
  sidebar.append(peersHeading);
  snapshot.online_peers.forEach((peer) => {
    const button = document.createElement("button");
    button.className = "peer-row";
    button.setAttribute("aria-current", "false");
    const dot = document.createElement("span");
    dot.className = "peer-status";
    const label = document.createElement("span");
    label.textContent = `${peer.display_name || peer.username}${peer.username === snapshot.current_user?.username ? " (Bạn)" : ""}`;
    button.append(dot, label);
    button.addEventListener("click", () => onOpenPeer(peer.username));
    sidebar.append(button);
  });
  const commonChat = document.createElement("button");
  commonChat.className = "common-chat-link";
  commonChat.setAttribute("aria-current", "page");
  commonChat.textContent = "Trò chuyện chung";
  sidebar.append(commonChat);
  const main = document.createElement("section");
  main.className = "workspace-main chat-panel";
  const header = document.createElement("header");
  header.className = "chat-header";
  const title = document.createElement("h1");
  title.textContent = "# Trò chuyện Chung";
  const onlineCount = document.createElement("span");
  onlineCount.className = "online-count";
  onlineCount.textContent = `${snapshot.online_peers.filter((peer) => peer.status === "ONLINE").length} người đang online`;
  const logout = document.createElement("button");
  logout.className = "button-secondary";
  logout.textContent = "Đăng xuất";
  logout.addEventListener("click", async () => { if (await confirmModal("Xác nhận đăng xuất", "Bạn có chắc muốn đăng xuất?")) { await API.logout(); await onRefresh(); } });
  userActions.append(logout);
  header.append(title, onlineCount);
  let connectionBanner = null;
  if (snapshot.connection_state !== "CONNECTED") {
    const connection = document.createElement("div");
    connection.className = "connection-banner";
    connection.setAttribute("role", "status");
    connection.textContent = snapshot.connection_state === "P2P_ONLY" ? "Máy chủ tạm ngưng · Đang kết nối trực tiếp P2P (Nhắn tin & Truyền file vẫn hoạt động)" : snapshot.connection_state === "RECONNECTING" ? "Mất kết nối. Một số tính năng không khả dụng. · Đang kết nối lại…" : "Đã ngắt kết nối. Vui lòng đăng nhập lại.";
    connectionBanner = connection;
  }
  const messages = document.createElement("div");
  messages.className = "message-list";
  if (!snapshot.broadcast_messages.length) messages.append(emptyState());
  snapshot.broadcast_messages.forEach((message) => messages.append(messageBubble(message, message.sender === snapshot.current_user?.username, snapshot.delivery_status[message.client_msg_id])));
  snapshot.file_transfers.filter((file) => file.peer === "").forEach((file) => messages.append(renderFileCard(file, {
    outgoing: file.direction === "UPLOAD",
    onCancel: async (id) => { await API.cancelFile(id); await onRefresh(); },
    onRetry: async (id) => { await API.retryFile(id); await onRefresh(); },
  })));
  const composerDock = document.createElement("section");
  composerDock.className = "composer-dock";
  const form = document.createElement("form");
  form.className = "composer";
  const input = document.createElement("textarea");
  input.rows = 1;
  input.placeholder = "Viết tin nhắn...";
  input.value = uiState.broadcastDraft;
  input.addEventListener("input", () => { uiState.broadcastDraft = input.value; });
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); form.requestSubmit(); }
  });
  input.setAttribute("aria-label", "Tin nhắn. Enter để gửi, Shift+Enter để xuống dòng");
  const send = document.createElement("button");
  send.className = "button-primary";
  send.textContent = "Gửi";
  send.type = "submit";
  form.append(input, send);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const content = input.value.trim();
    if (!content) return;
    const id = crypto.randomUUID();
    input.value = "";
    uiState.broadcastDraft = "";
    send.disabled = true;
    try { await API.broadcast(id, content); }
    catch (error) { input.value = content; uiState.broadcastDraft = content; showToast(error.message, "error"); }
    finally { send.disabled = false; input.focus(); }
  });
  const hint = document.createElement("p");
  hint.className = "composer-hint";
  hint.textContent = "Enter để gửi · Shift+Enter để xuống dòng";
  composerDock.append(form, hint);
  main.append(header, messages, composerDock);
  layout.append(topbar);
  if (connectionBanner) layout.append(connectionBanner);
  layout.append(sidebar, main);
  root.append(layout);
}

function messageBubble(message, outgoing, deliveryStatus) {
  const bubble = document.createElement("article");
  bubble.className = `message-bubble ${outgoing ? "outgoing" : "incoming"}`;
  const sender = document.createElement("strong");
  sender.textContent = message.sender_display || message.sender || "Bạn";
  const content = document.createElement("p");
  content.textContent = message.content || "";
  const time = document.createElement("small");
  const messageTime = message.server_time || message.timestamp;
  time.textContent = messageTime ? new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit" }).format(new Date(messageTime)) : "";
  bubble.append(sender, content, time);
  if (outgoing && deliveryStatus === "FAILED") {
    const failed = document.createElement("span");
    failed.className = "message-failed";
    failed.textContent = "Gửi thất bại · Thử gửi lại bằng cách soạn tin nhắn mới";
    bubble.append(failed);
  }
  return bubble;
}

function emptyState() {
  const empty = document.createElement("section");
  empty.className = "empty-state";
  const heading = document.createElement("h2");
  heading.textContent = "Chào mừng đến với Trò chuyện Chung";
  const copy = document.createElement("p");
  copy.textContent = "Tin nhắn mới trong phòng sẽ xuất hiện ở đây.";
  empty.append(heading, copy);
  return empty;
}
