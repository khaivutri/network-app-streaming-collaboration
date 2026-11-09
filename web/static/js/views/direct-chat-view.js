import { API } from "../api-client.js";
import { confirmModal } from "../components/modal.js";
import { renderFileCard } from "../components/file-card.js";
import { showToast } from "../components/toast.js";

export function renderDirectChat(root, snapshot, selectedPeer, { onBack, onChanged, onOpenPeer, uiState }) {
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
  if (!peerMessages.length) messages.append(emptyNotice("Chưa có tin nhắn. Hãy bắt đầu cuộc trò chuyện."));
  peerMessages.forEach((message) => messages.append(messageBubble(message, message.sender === snapshot.current_user?.username)));
  snapshot.file_transfers.filter((file) => file.peer === currentPeer?.username).forEach((file) => {
    messages.append(renderFileCard(file, {
      outgoing: file.direction === "UPLOAD",
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
  const input = document.createElement("input");
  input.type = "text";
  input.autocomplete = "off";
  input.placeholder = online ? "Nhập tin nhắn..." : "Người nhận đang ngoại tuyến";
  input.disabled = !online || snapshot.connection_state !== "CONNECTED";
  input.value = uiState.directDrafts.get(selectedPeer) || "";
  input.addEventListener("input", () => uiState.directDrafts.set(selectedPeer, input.value));
  const send = document.createElement("button");
  send.className = "button-primary";
  send.textContent = "Gửi";
  send.disabled = input.disabled;
  form.append(attach, input, send);
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const content = input.value.trim();
    const file = selectedFile.value;
    if (!content && !file) return;
    send.disabled = true;
    try {
      if (file) await API.sendFile(currentPeer.username, content, file);
      if (content) await API.unicast(makeMessageID(), currentPeer.username, content);
      input.value = "";
      uiState.directDrafts.set(selectedPeer, "");
      fileInput.value = "";
      selectedFile.value = null;
      uiState.selectedFiles.delete(selectedPeer);
      preview.hidden = true;
      await onChanged();
    } catch (error) { showToast(error.message, "error"); }
    finally { send.disabled = !online || snapshot.connection_state !== "CONNECTED"; }
  });
  if (selectedFile.value) renderPreview();
  composerDock.append(preview, form, fileInput);
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
  time.textContent = formatTime(message.timestamp);
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
