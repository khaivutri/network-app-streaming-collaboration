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
