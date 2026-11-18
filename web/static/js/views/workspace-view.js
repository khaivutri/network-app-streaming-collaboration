import { API } from "../api-client.js";
import { confirmModal } from "../components/modal.js";
import { renderFileCard, updateFileCard } from "../components/file-card.js";
import { showToast } from "../components/toast.js";

export function renderWorkspace(root, snapshot, { onOpenPeer, onChanged, onRefresh, uiState }) {
  if (root.dataset.viewKey === "workspace" && root.__viewRefs) { patchWorkspace(root.__viewRefs, snapshot); return; }
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
  root.dataset.viewKey = "workspace";
  root.__viewRefs = { root, sidebar, onlineCount, messages, connectionBanner, onOpenPeer, onRefresh };
}

function patchWorkspace(refs, snapshot) {
  const beforeBottom = refs.messages.scrollHeight - refs.messages.scrollTop - refs.messages.clientHeight <= 64;
  refs.onlineCount.textContent = `${snapshot.online_peers.filter((peer) => peer.status === "ONLINE").length} người đang online`;
  const peersHeading = refs.sidebar.querySelector("h2"); peersHeading.textContent = `THÀNH VIÊN (${snapshot.online_peers.length})`;
  refs.sidebar.querySelectorAll(".peer-row").forEach((node) => node.remove());
  const common = refs.sidebar.querySelector(".common-chat-link");
  snapshot.online_peers.forEach((peer) => {
    const button = document.createElement("button"); button.className = "peer-row"; button.setAttribute("aria-current", "false");
    const dot = document.createElement("span"); dot.className = "peer-status"; const label = document.createElement("span"); label.textContent = `${peer.display_name || peer.username}${peer.username === snapshot.current_user?.username ? " (Bạn)" : ""}`;
    button.append(dot, label); button.addEventListener("click", () => refs.onOpenPeer?.(peer.username)); refs.sidebar.insertBefore(button, common);
  });
  const message = snapshot.connection_state === "P2P_ONLY" ? "Máy chủ tạm ngưng · Đang kết nối trực tiếp P2P (Nhắn tin & Truyền file vẫn hoạt động)" : snapshot.connection_state === "RECONNECTING" ? "Mất kết nối. Một số tính năng không khả dụng. · Đang kết nối lại…" : "Đã ngắt kết nối. Vui lòng đăng nhập lại.";
  if (snapshot.connection_state !== "CONNECTED") {
    if (!refs.connectionBanner) { refs.connectionBanner = document.createElement("div"); refs.connectionBanner.className = "connection-banner"; refs.connectionBanner.setAttribute("role", "status"); refs.root.querySelector(".workspace-layout").insertBefore(refs.connectionBanner, refs.sidebar); }
    refs.connectionBanner.textContent = message;
  } else if (refs.connectionBanner) { refs.connectionBanner.remove(); refs.connectionBanner = null; }
  patchWorkspaceTimeline(refs.messages, snapshot, beforeBottom);
}

function patchWorkspaceTimeline(container, snapshot, beforeBottom) {
  const timeline = [...snapshot.broadcast_messages.map((item, i) => ({ kind: "message", item, at: Number(item.server_time || item.sent_at || 0), order: i })), ...snapshot.file_transfers.filter((file) => file.peer === "").map((item, i) => ({ kind: "file", item, at: Number(item.created_at || item.updated_at || 0), order: i }))].sort((a, b) => a.at - b.at || a.order - b.order);
  if (!timeline.length) { if (!container.querySelector(".empty-state")) container.append(emptyState()); return; }
  container.querySelector(".empty-state")?.remove();
  const nodes = timeline.map(({ kind, item }) => {
    const node = [...container.children].find((child) => kind === "file" ? child.dataset.transferId === item.transfer_id : child.dataset.clientMsgId === item.client_msg_id || (item.msg_id && child.dataset.msgId === item.msg_id));
    if (kind === "file") {
      const callbacks = { outgoing: item.direction === "UPLOAD", onCancel: async (id) => { await API.cancelFile(id); await refs.onRefresh?.(); }, onRetry: async (id) => { await API.retryFile(id); await refs.onRefresh?.(); } };
      return node ? updateFileCard(node, item, callbacks) : renderFileCard(item, callbacks);
    }
    const outgoing = item.sender === snapshot.current_user?.username;
    if (node) updateWorkspaceBubble(node, item, outgoing, snapshot.delivery_status[item.client_msg_id]);
    return node || messageBubble(item, outgoing, snapshot.delivery_status[item.client_msg_id]);
  });
  nodes.forEach((node, i) => { if (container.children[i] !== node) container.insertBefore(node, container.children[i] || null); });
  while (container.children.length > nodes.length) container.lastElementChild.remove();
  if (beforeBottom) container.scrollTop = container.scrollHeight;
}

function updateWorkspaceBubble(bubble, message, outgoing, status) {
  bubble.dataset.clientMsgId = message.client_msg_id || ""; bubble.dataset.msgId = message.msg_id || ""; bubble.className = `message-bubble ${outgoing ? "outgoing" : "incoming"}`;
  bubble.children[0].textContent = message.sender_display || message.sender || "Bạn"; bubble.children[1].textContent = message.content || "";
  const time = message.server_time || message.timestamp; bubble.children[2].textContent = time ? new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit" }).format(new Date(time)) : "";
  let failed = bubble.querySelector(".message-failed");
  if (outgoing && status === "FAILED") { if (!failed) { failed = document.createElement("span"); failed.className = "message-failed"; bubble.append(failed); } failed.textContent = "Gửi thất bại · Thử gửi lại bằng cách soạn tin nhắn mới"; } else failed?.remove();
}

function messageBubble(message, outgoing, deliveryStatus) {
  const bubble = document.createElement("article");
  bubble.dataset.clientMsgId = message.client_msg_id || ""; bubble.dataset.msgId = message.msg_id || "";
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
