package state

import (
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestClientStore_Step5MessageHistoryAndDelivery(t *testing.T) {
	store := NewClientStore()
	store.SetLoginSuccess(&protocol.UserInfoDTO{Username: "alice"}, nil)
	store.AppendBroadcastMessage(protocol.BroadcastMessageServerPayload{MsgID: "b1", Content: "hello"})
	store.AppendDirectMessage(protocol.UnicastMessageServerPayload{MsgID: "d1", Sender: "bob", Recipient: "alice", Content: "secret"})
	if len(store.GetBroadcastMessages()) != 1 || len(store.GetDirectMessages("bob")) != 1 {
		t.Fatal("message history was not partitioned correctly")
	}
	store.MarkDelivered("c1")
	if got := store.DeliveryStatus("c1"); got != "DELIVERED" {
		t.Fatalf("delivery status=%q", got)
	}
}
