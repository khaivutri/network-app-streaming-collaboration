const emptySession = () => ({
  authenticated: false,
  connection_state: "DISCONNECTED",
  current_user: null,
  online_peers: [],
  broadcast_messages: [],
  direct_messages: {},
  delivery_status: {},
  file_transfers: [],
});

export function normalizeSession(value = {}) {
  const session = value && typeof value === "object" ? value : {};
  return {
    ...emptySession(),
    ...session,
    online_peers: Array.isArray(session.online_peers) ? session.online_peers : [],
    broadcast_messages: Array.isArray(session.broadcast_messages) ? session.broadcast_messages : [],
    direct_messages: session.direct_messages && typeof session.direct_messages === "object" && !Array.isArray(session.direct_messages) ? session.direct_messages : {},
    delivery_status: session.delivery_status && typeof session.delivery_status === "object" && !Array.isArray(session.delivery_status) ? session.delivery_status : {},
    file_transfers: Array.isArray(session.file_transfers) ? session.file_transfers : [],
  };
}

function clone(value) {
  return typeof structuredClone === "function" ? structuredClone(value) : JSON.parse(JSON.stringify(value));
}

export function createStore() {
  let session = emptySession();
  const listeners = new Set();
  return {
    replaceSession(next) {
      session = clone(normalizeSession({ ...session, ...next }));
      listeners.forEach((listener) => {
        try { listener(this.snapshot()); } catch (error) { console.error("UI render failed", error); }
      });
    },
    applyEvent(name, data) {
      if (name === "presence.updated" && data.peers) session.online_peers = data.peers;
      if (name === "presence.updated" && data.peer) {
        const peers = session.online_peers.filter((peer) => peer.username !== data.peer.username);
        if (data.event === "JOIN") peers.push(data.peer);
        session.online_peers = peers;
      }
      if (name === "chat.message_received") {
        if (data.scope === "UNICAST" || data.recipient) {
          const peer = data.sender === session.current_user?.username ? data.recipient : data.sender;
          session.direct_messages[peer] ||= [];
          upsertMessage(session.direct_messages[peer], data);
        } else upsertMessage(session.broadcast_messages, data);
      }
      if (name === "chat.delivery_status") session.delivery_status[data.client_msg_id] = data.status;
      if (name === "file.progress" || name === "file.completed" || name === "file.failed") {
        const index = session.file_transfers.findIndex((item) => item.transfer_id === data.transfer_id);
        if (index >= 0) session.file_transfers[index] = mergeTransfer(session.file_transfers[index], data);
        else session.file_transfers.push(data);
      }
      if (name === "connection.status") session.connection_state = data.state;
      listeners.forEach((listener) => listener(this.snapshot()));
    },
    upsertFileTransfer(transfer) {
      if (!transfer?.transfer_id) return;
      const index = session.file_transfers.findIndex((item) => item.transfer_id === transfer.transfer_id);
      if (index >= 0) session.file_transfers[index] = mergeTransfer(session.file_transfers[index], transfer);
      else session.file_transfers.push(clone(transfer));
      listeners.forEach((listener) => listener(this.snapshot()));
    },
    addOptimisticMessage(scope, peer, message) {
      const list = scope === "UNICAST" ? (session.direct_messages[peer] ||= []) : session.broadcast_messages;
      upsertMessage(list, { ...message, scope, optimistic: true });
      listeners.forEach((listener) => listener(this.snapshot()));
    },
    subscribe(listener) { listeners.add(listener); return () => listeners.delete(listener); },
    snapshot() { return clone(session); },
  };
}

function mergeTransfer(previous = {}, next = {}) {
  const merged = { ...previous };
  for (const [key, value] of Object.entries(next)) {
    if (key === "created_at" && Number(previous.created_at) > 0) continue;
    if (key === "state" && ["COMPLETED", "FAILED", "ERROR", "REJECTED", "CANCELLED"].includes(previous.state) && ["INIT", "TRANSFERRING"].includes(value)) continue;
    if ((key === "percentage" || key === "transferred_bytes") && Number(value) < Number(previous[key] || 0)) continue;
    if (value !== undefined && value !== null && value !== "") merged[key] = value;
  }
  return merged;
}

function upsertMessage(messages, message) {
  const index = messages.findIndex((item) => (message.msg_id && item.msg_id === message.msg_id) || (message.client_msg_id && item.client_msg_id === message.client_msg_id));
  if (index >= 0) messages[index] = { ...messages[index], ...message, optimistic: false };
  else messages.push(message);
}
