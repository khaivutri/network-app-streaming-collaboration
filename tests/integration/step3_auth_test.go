package integration

import (
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/handlers"
	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"net"
	"path/filepath"
	"sync"
	"testing"
)

func TestStep3BR01AndBR02(t *testing.T) {
	state.ResetGlobalState()
	defer state.ResetGlobalState()
	store, err := state.LoadCredentialStore(filepath.Join("..", "..", "configs", "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	results := make([]protocol.AuthResponsePayload, 0, 10)
	h := handlers.NewAuthHandler(store, func(raw []byte, _ *net.UDPAddr) error {
		packet, parseErr := protocol.UnmarshalPacket(raw)
		if parseErr != nil {
			return parseErr
		}
		response, decodeErr := protocol.DecodeJSONPayload[protocol.AuthResponsePayload](packet.Payload)
		if decodeErr != nil {
			return decodeErr
		}
		mu.Lock()
		results = append(results, response)
		mu.Unlock()
		return nil
	})
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			payload, _ := protocol.EncodeJSONPayload(protocol.AuthRequestPayload{RequestID: "r", Username: "Alice", Password: "Alice@123"})
			_ = h.HandleAuthRequest(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10000 + i}, uint32(i+1), payload, make(chan []byte, 1))
		}(i)
	}
	wg.Wait()
	ok, duplicate := 0, 0
	for _, result := range results {
		if result.Code == protocol.AuthCodeOK {
			ok++
		}
		if result.Code == protocol.AuthCodeAlreadyOnline {
			duplicate++
		}
	}
	if ok != 1 || duplicate != 9 {
		t.Fatalf("auth race outcomes: ok=%d duplicate=%d results=%d", ok, duplicate, len(results))
	}
}

func TestStep3BR03BR04PresenceAndLogout(t *testing.T) {
	state.ResetGlobalState()
	defer state.ResetGlobalState()
	store, err := state.LoadCredentialStore(filepath.Join("..", "..", "configs", "users.json"))
	if err != nil {
		t.Fatal(err)
	}
	aliceUpdates := make(chan []byte, 2)
	var responses sync.Mutex
	h := handlers.NewAuthHandler(store, func(raw []byte, _ *net.UDPAddr) error { responses.Lock(); defer responses.Unlock(); return nil })
	login := func(name, pass string, id uint32, ch chan []byte) {
		payload, _ := protocol.EncodeJSONPayload(protocol.AuthRequestPayload{RequestID: name, Username: name, Password: pass})
		if err := h.HandleAuthRequest(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: int(10000 + id)}, id, payload, ch); err != nil {
			t.Fatal(err)
		}
	}
	login("Alice", "Alice@123", 1, aliceUpdates)
	login("Bob", "Bob@123", 2, make(chan []byte, 1))
	join := <-aliceUpdates
	joinPacket, err := protocol.UnmarshalPacket(join)
	if err != nil {
		t.Fatal(err)
	}
	update, err := protocol.DecodeJSONPayload[protocol.DiscoUpdatePayload](joinPacket.Payload)
	if err != nil || update.Event != "JOIN" || update.Peer.Username != "Bob" {
		t.Fatalf("bad join update: %#v %v", update, err)
	}
	logout, _ := protocol.EncodeJSONPayload(protocol.LogoutPayload{RequestID: "logout", Username: "Alice"})
	if err := h.HandleLogout(&net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 10001}, 1, logout); err != nil {
		t.Fatal(err)
	}
	if peers := state.GetSnapshotOnlinePeers(); len(peers) != 1 || peers[0].Username != "Bob" {
		t.Fatalf("remaining peers: %#v", peers)
	}
}
