package state

import (
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"testing"
)

func TestClientStoreSessionAndPresence(t *testing.T) {
	store := NewClientStore()
	store.SetLoginSuccess(&protocol.UserInfoDTO{Username: "Alice"}, []protocol.PeerDTO{{Username: "Alice", Status: "ONLINE"}})
	store.ApplyDiscoUpdate(protocol.DiscoUpdatePayload{Event: "JOIN", Peer: protocol.PeerDTO{Username: "Bob", Status: "ONLINE"}})
	if !store.IsLoggedIn() || len(store.GetOnlinePeers()) != 2 {
		t.Fatal("login or join state not recorded")
	}
	store.ApplyDiscoUpdate(protocol.DiscoUpdatePayload{Event: "LEAVE", Peer: protocol.PeerDTO{Username: "Bob"}})
	if len(store.GetOnlinePeers()) != 1 {
		t.Fatal("leave state not recorded")
	}
	store.ClearSession()
	if store.IsLoggedIn() || len(store.GetOnlinePeers()) != 0 {
		t.Fatal("session was not cleared")
	}
}
