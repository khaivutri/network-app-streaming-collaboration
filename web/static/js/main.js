import { API } from "./api-client.js";
import { createStore } from "./store.js";
import { connectEvents } from "./sse-listener.js";
import { renderLogin } from "./views/login-view.js";
import { renderWorkspace } from "./views/workspace-view.js";
import { renderDirectChat } from "./views/direct-chat-view.js";
import { showToast } from "./components/toast.js";

const appRoot = document.querySelector("#app");
const browserStore = createStore();
let stopEvents = null;
let selectedPeer = null;
let unsubscribeRender = null;
let transferSyncTimer = null;
const uiState = { broadcastDraft: "", directDrafts: new Map(), selectedFiles: new Map() };

function renderUI(snapshot) {
  if (!snapshot.authenticated) {
    selectedPeer = null;
    renderLogin(appRoot, { onLoggedIn: bootstrap });
  } else if (selectedPeer) {
    renderDirectChat(appRoot, snapshot, selectedPeer, {
      onBack: () => { selectedPeer = null; renderUI(browserStore.snapshot()); },
      onChanged: bootstrap,
      onOpenPeer: (peer) => { selectedPeer = peer; renderUI(browserStore.snapshot()); },
      onFileTransfer: (transfer) => browserStore.upsertFileTransfer(transfer),
      onOptimisticMessage: (scope, peer, message) => browserStore.addOptimisticMessage(scope, peer, message),
      uiState,
    });
  } else {
    renderWorkspace(appRoot, snapshot, {
      onOpenPeer: (peer) => { selectedPeer = peer; renderUI(browserStore.snapshot()); },
      onChanged: renderUI,
      onRefresh: bootstrap,
      onFileTransfer: (transfer) => browserStore.upsertFileTransfer(transfer),
      onOptimisticMessage: (scope, peer, message) => browserStore.addOptimisticMessage(scope, peer, message),
      uiState,
    });
  }
}

async function bootstrap() {
  try {
    const session = await API.getSession();
    browserStore.replaceSession(session);
    if (!unsubscribeRender) unsubscribeRender = browserStore.subscribe(renderUI);
    if (!transferSyncTimer) transferSyncTimer = setInterval(async () => {
      const current = browserStore.snapshot();
      if (!current.authenticated || !current.file_transfers.some((file) => ["INIT", "TRANSFERRING"].includes(file.state))) return;
      try { browserStore.replaceSession(await API.getSession()); }
      catch (error) { console.error("Transfer resync failed", error); }
    }, 2000);
    if (stopEvents) {
      stopEvents();
      stopEvents = null;
    }
    if (session.authenticated) {
      let warned = false;
      stopEvents = connectEvents(
        (name, data) => { browserStore.applyEvent(name, data); },
        () => { if (!warned) showToast("Mất kết nối sự kiện, đang thử lại...", "error"); warned = true; },
        async () => {
          try { browserStore.replaceSession(await API.getSession()); warned = false; }
          catch (error) { console.error("SSE resync failed", error); }
        },
      );
    }
    renderUI(browserStore.snapshot());
  } catch (error) {
    renderBootstrapError(error);
    console.error(error);
  }
}

function renderBootstrapError(error) {
  const panel = document.createElement("section");
  panel.className = "bootstrap-error panel";
  panel.setAttribute("role", "alert");
  const title = document.createElement("h1");
  title.textContent = "Chưa thể tải giao diện";
  const message = document.createElement("p");
  message.textContent = "Không thể kết nối với Local Client Agent. Hãy thử tải lại dữ liệu.";
  const code = document.createElement("code");
  code.textContent = "UI_BOOTSTRAP_FAILED";
  const retry = document.createElement("button");
  retry.className = "button-primary";
  retry.textContent = "Thử lại";
  retry.addEventListener("click", bootstrap);
  panel.append(title, message, code, retry);
  appRoot.replaceChildren(panel);
  if (error) console.error("UI_BOOTSTRAP_FAILED", error);
}

bootstrap();
