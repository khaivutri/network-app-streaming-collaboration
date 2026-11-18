import test from "node:test";
import assert from "node:assert/strict";
import { createStore, normalizeSession } from "./store.js";

test("normalizeSession converts nullable collections to stable empty values", () => {
  const result = normalizeSession({
    authenticated: true,
    online_peers: null,
    broadcast_messages: null,
    direct_messages: null,
    delivery_status: null,
    file_transfers: null,
  });

  assert.deepEqual(result.online_peers, []);
  assert.deepEqual(result.broadcast_messages, []);
  assert.deepEqual(result.direct_messages, {});
  assert.deepEqual(result.delivery_status, {});
  assert.deepEqual(result.file_transfers, []);
});

test("store snapshots remain renderable after a successful empty-history login", () => {
  const store = createStore();
  store.replaceSession({ authenticated: true, broadcast_messages: null });

  assert.equal(store.snapshot().authenticated, true);
  assert.equal(store.snapshot().broadcast_messages.length, 0);
});

test("store subscribers receive change metadata for chat, file and delivery events", () => {
  const store = createStore();
  store.replaceSession({ authenticated: true, current_user: { username: "alice" } });
  const changes = [];
  store.subscribe((_snapshot, change) => changes.push(change));

  store.addOptimisticMessage("UNICAST", "bob", { client_msg_id: "client-1", sender: "alice", content: "hello" });
  store.applyEvent("chat.message_received", { scope: "UNICAST", sender: "alice", recipient: "bob", client_msg_id: "client-1", content: "hello" });
  store.applyEvent("chat.delivery_status", { client_msg_id: "client-1", status: "FAILED" });
  store.applyEvent("file.progress", { transfer_id: "transfer-1", peer: "bob", state: "TRANSFERRING", percentage: 10 });

  assert.deepEqual(changes.map((change) => change.type), ["chat.optimistic", "chat.message_received", "chat.delivery_status", "file.updated"]);
  assert.deepEqual(changes[0], { type: "chat.optimistic", scope: "UNICAST", peer: "bob", clientMsgId: "client-1" });
  assert.equal(changes[1].peer, "bob");
  assert.equal(changes[2].status, "FAILED");
  assert.equal(changes[3].transferId, "transfer-1");
});

test("temporary upload transfer can be replaced by the server transfer id", () => {
  const store = createStore();
  store.upsertFileTransfer({ transfer_id: "local-one", state: "UPLOADING_LOCAL", peer: "bob", percentage: 70 });
  store.replaceTransferId("local-one", { transfer_id: "server-one", state: "INIT", peer: "bob", percentage: 0 });

  assert.deepEqual(store.snapshot().file_transfers, [{ transfer_id: "server-one", state: "INIT", peer: "bob", percentage: 0 }]);
});
