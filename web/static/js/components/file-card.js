import { API } from "../api-client.js";

export function renderFileCard(file, { onCancel, onRetry, outgoing = file.direction === "UPLOAD" } = {}) {
  const state = String(file.state || "INIT").toUpperCase();
  const card = document.createElement("article");
  card.className = `file-card ${outgoing ? "outgoing" : "incoming"} file-${state.toLowerCase()}`;
  const metadata = document.createElement("div");
  metadata.className = "file-metadata";
  const icon = document.createElement("span");
  icon.className = "file-icon";
  icon.setAttribute("aria-hidden", "true");
  icon.textContent = "▤";
  const details = document.createElement("div");
  details.className = "file-details";
  const title = document.createElement("strong");
  title.textContent = file.file_name || "Tệp";
  const size = document.createElement("small");
  size.textContent = formatBytes(Number(file.file_size || 0));
  details.append(title, size);
  metadata.append(icon, details);
  card.append(metadata);

  const percentage = Math.max(0, Math.min(100, Number(file.percentage || 0)));
  if (["INIT", "TRANSFERRING"].includes(state)) {
    const progress = document.createElement("progress");
    progress.max = 100;
    progress.value = percentage;
    progress.setAttribute("aria-label", `Đang truyền tệp: ${percentage}%`);
    const progressLabel = document.createElement("div");
    progressLabel.className = "file-progress-label";
    progressLabel.textContent = `${percentage}% · ${formatBytes(Number(file.transferred_bytes || 0))}`;
    card.append(progress, progressLabel);
  }

  const status = document.createElement("span");
  status.className = "file-status";
  status.textContent = ({
    INIT: "Đang chuẩn bị…",
    TRANSFERRING: "Đang truyền tệp…",
    COMPLETED: "Đã truyền thành công",
    FAILED: file.error_message || "Gián đoạn đường truyền",
    ERROR: file.error_message || "Gián đoạn đường truyền",
    REJECTED: file.error_message || "Không thể nhận tệp",
    CANCELLED: "Đã huỷ truyền tệp",
  })[state] || state;
  card.append(status);

  const actions = document.createElement("div");
  actions.className = "file-actions";
  if (["INIT", "TRANSFERRING"].includes(state) && onCancel) {
    actions.append(actionButton("Huỷ", "button-secondary", () => onCancel(file.transfer_id)));
  }
  if (["FAILED", "ERROR", "REJECTED"].includes(state) && onRetry) {
    actions.append(actionButton("Thử lại", "button-link", () => onRetry(file.transfer_id)));
  }
  if (state === "COMPLETED" && file.direction === "DOWNLOAD" && file.transfer_id) {
    const download = document.createElement("a");
    download.className = "button-secondary download-link";
    download.href = API.downloadURL(file.transfer_id);
    download.textContent = "Tải xuống";
    actions.append(download);
  }
  if (actions.childElementCount) card.append(actions);
  return card;
}

function actionButton(label, className, action) {
  const button = document.createElement("button");
  button.type = "button";
  button.className = className;
  button.textContent = label;
  button.addEventListener("click", action);
  return button;
}

function formatBytes(value) {
  if (!Number.isFinite(value) || value < 0) return "0 B";
  if (value < 1024) return `${value} B`;
  const units = ["KB", "MB", "GB"];
  let size = value / 1024;
  let unit = 0;
  while (size >= 1024 && unit < units.length - 1) { size /= 1024; unit += 1; }
  return `${size.toFixed(1)} ${units[unit]}`;
}
