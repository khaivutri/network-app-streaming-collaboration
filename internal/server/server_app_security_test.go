package server

import (
	"bytes"
	"context"
	"net"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// TestBroadcastEncryptsDiscoUpdatesForEachRecipient checks both JOIN and LEAVE wire packets.
func TestBroadcastEncryptsDiscoUpdatesForEachRecipient(t *testing.T) {
	state.ResetGlobalState()
	defer state.ResetGlobalState()

	var clientToServer [32]byte
	var serverToClient [32]byte
	for index := range clientToServer {
		clientToServer[index] = byte(index + 1)
		serverToClient[index] = byte(index + 33)
	}
	clientCipher, err := security.NewSessionCipher(clientToServer, serverToClient, false)
	if err != nil {
		t.Fatal(err)
	}
	serverCipher, err := security.NewSessionCipher(serverToClient, clientToServer, true)
	if err != nil {
		t.Fatal(err)
	}

	aliceSession := rudp.NewSession(101, &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 4101,
	}, 4, context.Background())
	defer aliceSession.Close("test complete")
	aliceSession.SetCipher(serverCipher)
	if _, _, err = state.RegisterReliableUser("Alice", "Alice", aliceSession); err != nil {
		t.Fatal(err)
	}

	bobSession := rudp.NewSession(202, &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 4202,
	}, 4, context.Background())
	defer bobSession.Close("test complete")
	if _, targets, registerErr := state.RegisterReliableUser("Bob", "Bob", bobSession); registerErr != nil {
		t.Fatal(registerErr)
	} else {
		state.SetP2PMetadata("Bob", 5002, "private-ticket")
		if len(targets) != 1 || targets[0].ConnectionID != aliceSession.ID() || targets[0].Cipher != serverCipher {
			t.Fatalf("JOIN targets did not preserve Alice's session context: %#v", targets)
		}
		peer := protocol.PeerDTO{
			Username:    "Bob",
			DisplayName: "Bob",
			IP:          "127.0.0.1",
			Port:        4202,
			P2PPort:     5002,
			PeerTicket:  "private-ticket",
		}
		app := &ServerApp{}
		app.broadcast(targets, protocol.DiscoUpdatePayload{
			Event: "JOIN",
			Peer:  peer,
		})
		assertEncryptedDiscoUpdate(t, aliceSession.OutboundChan(), clientCipher, aliceSession.ID(), "JOIN", "Bob", "private-ticket")
	}

	removed, targets, existed := state.RemoveReliableUserByConnID(bobSession.ID())
	if !existed || removed == nil || removed.Username != "Bob" || removed.Status != "OFFLINE" {
		t.Fatalf("unexpected removed peer: %#v, existed=%v", removed, existed)
	}
	if len(targets) != 1 || targets[0].ConnectionID != aliceSession.ID() || targets[0].Cipher != serverCipher {
		t.Fatalf("LEAVE targets did not preserve Alice's session context: %#v", targets)
	}
	app := &ServerApp{}
	app.broadcast(targets, protocol.DiscoUpdatePayload{
		Event: "LEAVE",
		Peer:  *removed,
	})
	assertEncryptedDiscoUpdate(t, aliceSession.OutboundChan(), clientCipher, aliceSession.ID(), "LEAVE", "Bob", "private-ticket")
}

// assertEncryptedDiscoUpdate checks recipient binding, confidentiality, and payload authenticity.
func assertEncryptedDiscoUpdate(t *testing.T, outbound <-chan []byte, cipher *security.SessionCipher, connID uint32, event, username, ticket string) {
	t.Helper()
	select {
	case raw := <-outbound:
		header, err := protocol.UnmarshalHeader(raw[:protocol.HeaderSize])
		if err != nil {
			t.Fatal(err)
		}
		if !header.HasFlag(protocol.FlagENC) || header.ConnectionID != connID || header.MsgType != protocol.MsgDiscoUpdate {
			t.Fatalf("unexpected wire header: %#v", header)
		}
		for _, secret := range [][]byte{[]byte(username), []byte(ticket), []byte(event), []byte("p2p_port")} {
			if bytes.Contains(raw, secret) {
				t.Fatalf("presence data leaked onto wire: %q", secret)
			}
		}
		payload, err := cipher.DecryptPacket(header, raw[:protocol.HeaderSize], raw[protocol.HeaderSize:])
		if err != nil {
			t.Fatalf("decrypt presence update: %v", err)
		}
		update, err := protocol.DecodeJSONPayload[protocol.DiscoUpdatePayload](payload)
		if err != nil || update.Event != event || update.Peer.Username != username || update.Peer.PeerTicket != ticket {
			t.Fatalf("unexpected decoded update: %#v, err=%v", update, err)
		}
	default:
		t.Fatal("presence update was not queued")
	}
}
