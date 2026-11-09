import { API } from "../api-client.js";
import { confirmModal } from "../components/modal.js";
import { renderFileCard } from "../components/file-card.js";
import { showToast } from "../components/toast.js";

export function renderDirectChat(root, snapshot, selectedPeer, { onBack, onChanged, onOpenPeer, onFileTransfer, onOptimisticMessage, uiState }) {
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
  const logout = document.createElement("button");
  logout.className = "button-secondary";
  logout.textContent = "Đăng xuất";
  logout.addEventListener("click", async () => {
    if (await confirmModal("Xác nhận đăng xuất", "Bạn có chắc muốn đăng xuất?")) {
      await API.logout();
      await onChanged();
    }
  });
  userActions.append(greeting, logout);
  topbar.append(brand, userActions);

  const sidebar = document.createElement("aside");
  sidebar.className = "sidebar";
  const rosterHeading = document.createElement("h2");
  rosterHeading.textContent = `THÀNH VIÊN (${snapshot.online_peers.length})`;
  const roster = document.createElement("nav");
  roster.setAttribute("aria-label", "Danh sách thành viên");
  snapshot.online_peers.forEach((member) => {
    const button = document.createElement("button");
    button.className = "peer-row";
    button.setAttribute("aria-current", member.username === selectedPeer ? "page" : "false");
    button.disabled = member.username === snapshot.current_user?.username;
    const dot = document.createElement("span");
    dot.className = "peer-status";
    const name = document.createElement("span");
    name.textContent = `${member.display_name || member.username}${member.username === snapshot.current_user?.username ? " (Bạn)" : ""}`;
    button.append(dot, name);
    button.addEventListener("click", () => onOpenPeer?.(member.username));
    roster.append(button);
  });
  const commonChat = document.createElement("button");
  commonChat.className = "common-chat-link";
  commonChat.textContent = "Trò chuyện chung";
  commonChat.addEventListener("click", onBack);
  sidebar.append(rosterHeading, roster, commonChat);

  const main = document.createElement("main");
  main.className = "workspace-main direct-main";
  const currentPeer = snapshot.online_peers.find((item) => item.username === selectedPeer);
  const online = currentPeer?.status === "ONLINE";
  const header = document.createElement("header");
  header.className = "chat-header direct-header";
  const back = document.createElement("button");
  back.className = "icon-button";
  back.setAttribute("aria-label", "Quay lại trò chuyện chung");
  back.textContent = "←";
  back.addEventListener("click", onBack);
  const avatar = document.createElement("span");
  avatar.className = "avatar";
  avatar.textContent = (currentPeer?.display_name || currentPeer?.username || "?").slice(0, 1).toUpperCase();
  const peerIdentity = document.createElement("div");
  peerIdentity.className = "peer-identity";
  const title = document.createElement("h1");
  title.textContent = currentPeer?.display_name || currentPeer?.username || "Thành viên";
  const status = document.createElement("span");
  status.className = online ? "peer-online" : "peer-offline";
  status.textContent = online ? "● Đang hoạt động" : "Ngoại tuyến";
  peerIdentity.append(title, status);
  header.append(back, avatar, peerIdentity);

  const messages = document.createElement("div");
  messages.className = "message-list";
  messages.setAttribute("aria-live", "polite");
  const peerMessages = snapshot.direct_messages[currentPeer?.username] || [];
  const timeline = [
    ...peerMessages.map((message, index) => ({ kind: "message", item: message, at: Number(message.server_time || message.sent_at || 0), order: index })),
    ...snapshot.file_transfers.filter((file) => file.peer === currentPeer?.username).map((file, index) => ({ kind: "file", item: file, at: Number(file.created_at || file.updated_at || 0), order: index })),
  ].sort((a, b) => a.at - b.at || a.order - b.order);
  if (!timeline.length) messages.append(emptyNotice("Chưa có tin nhắn. Hãy bắt đầu cuộc trò chuyện."));
  timeline.forEach(({ kind, item }) => {
    if (kind === "message") messages.append(messageBubble(item, item.sender === snapshot.current_user?.username));
    else messages.append(renderFileCard(item, {
      outgoing: item.direction === "UPLOAD",
      onCancel: async (id) => { if (await confirmModal("Huỷ truyền tệp?", "Tệp chưa hoàn tất sẽ bị xoá khỏi thiết bị người nhận.", "Huỷ truyền")) { await API.cancelFile(id); await onChanged(); } },
      onRetry: async (id) => { await API.retryFile(id); await onChanged(); },
    }));
  });

  const composerDock = document.createElement("section");
  composerDock.className = "composer-dock";
  const selectedFile = { value: uiState.selectedFiles.get(selectedPeer) || null };
  const fileInput = document.createElement("input");
  fileInput.className = "file-picker";
  fileInput.type = "file";
  fileInput.setAttribute("aria-label", "Chọn tệp đính kèm");
  const preview = document.createElement("div");
  preview.className = "file-preview";
  preview.hidden = true;
  const renderPreview = () => {
    preview.replaceChildren();
    if (!selectedFile.value) { preview.hidden = true; return; }
    const fileLabel = document.createElement("span");
    fileLabel.textContent = `${selectedFile.value.name} · ${formatBytes(selectedFile.value.size)}`;
    const remove = document.createElement("button");
    remove.type = "button";
    remove.className = "icon-button";
    remove.textContent = "×";
    remove.setAttribute("aria-label", "Bỏ chọn tệp");
    remove.addEventListener("click", () => { fileInput.value = ""; selectedFile.value = null; uiState.selectedFiles.delete(selectedPeer); preview.hidden = true; });
    preview.append(fileLabel, remove);
    preview.hidden = false;
  };
  fileInput.addEventListener("change", () => {
    selectedFile.value = fileInput.files?.[0] || null;
    if (selectedFile.value) uiState.selectedFiles.set(selectedPeer, selectedFile.value);
    renderPreview();
  });
  const form = document.createElement("form");
  form.className = "composer";
  const attach = document.createElement("button");
  attach.type = "button";
  attach.className = "icon-button attach-button";
  attach.textContent = "＋";
  attach.title = "Đính kèm tệp";
  attach.disabled = !online || snapshot.connection_state !== "CONNECTED";
  attach.addEventListener("click", () => fileInput.click());
  const input = document.createElement("textarea");
  input.rows = 1;
  input.autocomplete = "off";
  input.placeholder = online ? "Nhập tin nhắn..." : "Người nhận đang ngoại tuyến";
  input.disabled = !online || snapshot.connection_state !== "CONNECTED";
  input.value = uiState.directDrafts.get(selectedPeer) || "";
  input.addEventListener("input", () => uiState.directDrafts.set(selectedPeer, input.value));
  input.addEventListener("keydown", (event) => {
    if (event.key === "Enter" && !event.shiftKey && !event.isComposing) { event.preventDefault(); form.requestSubmit(); }
  });
  input.setAttribute("aria-label", "Tin nhắn. Enter để gửi, Shift+Enter để xuống dòng");
  const send = document.createElement("button");
  send.className = "button-primary";
  send.textContent = "Gửi";
  send.type = "submit";
  send.disabled = input.disabled;
  form.append(attach, input, send);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const content = input.value.trim();
    const file = selectedFile.value;
    if (!content && !file) return;
    input.value = "";
    uiState.directDrafts.set(selectedPeer, "");
    const messageID = makeMessageID();
    if (content) onOptimisticMessage?.("UNICAST", currentPeer.username, { client_msg_id: messageID, sender: snapshot.current_user?.username, content, server_time: Date.now() });
    send.disabled = true;
    let messageSent = false;
    try {
      if (content) {
        await API.unicast(messageID, currentPeer.username, content);
        messageSent = true;
      }
      if (file) {
        const result = await API.sendFile(currentPeer.username, "", file);
        onFileTransfer?.({ ...result, direction: "UPLOAD", peer: currentPeer.username, file_size: file.size, created_at: Date.now(), state: result.state || "INIT", transferred_bytes: 0, percentage: 0 });
      }
      fileInput.value = "";
      selectedFile.value = null;
      uiState.selectedFiles.delete(selectedPeer);
      preview.hidden = true;
    } catch (error) {
      if (!messageSent) {
        input.value = content;
        uiState.directDrafts.set(selectedPeer, content);
      }
      showToast(error.message, "error");
    }
    finally { send.disabled = !online || snapshot.connection_state !== "CONNECTED"; }
  });
  if (selectedFile.value) renderPreview();
  const hint = document.createElement("p");
  hint.className = "composer-hint";
  hint.textContent = "Enter để gửi · Shift+Enter để xuống dòng";
  composerDock.append(preview, form, hint, fileInput);
  main.append(header, messages, composerDock);
  layout.append(topbar, sidebar, main);
  root.append(layout);
}

function messageBubble(message, outgoing) {
  const bubble = document.createElement("article");
  bubble.className = `message-bubble ${outgoing ? "outgoing" : "incoming"}`;
  const sender = document.createElement("strong");
  sender.textContent = message.sender_display || message.sender || "Bạn";
  const content = document.createElement("p");
  content.textContent = message.content || "";
  const time = document.createElement("small");
  time.textContent = `${formatTime(message.server_time || message.timestamp)}${message.optimistic ? " · Đang gửi" : ""}`;
  bubble.append(sender, content, time);
  return bubble;
}

function emptyNotice(text) {
  const notice = document.createElement("p");
  notice.className = "empty-state";
  notice.textContent = text;
  return notice;
}

function makeMessageID() {
  return globalThis.crypto?.randomUUID?.() || `msg-${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

function formatTime(value) {
  if (!value) return "";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? "" : new Intl.DateTimeFormat("vi-VN", { hour: "2-digit", minute: "2-digit" }).format(date);
}

function formatBytes(value) {
  if (!Number.isFinite(value) || value < 0) return "0 B";
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB"];
  let size = value / 1024;
  let index = 0;
  while (size >= 1024 && index < units.length - 1) { size /= 1024; index += 1; }
  return `${size.toFixed(1)} ${units[index]}`;
}
