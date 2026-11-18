package integration

import (
	"bytes"
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	clientapp "github.com/khaivutri/network-app-streaming-collaboration/internal/client"
	clientstate "github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

var step8PairCounter atomic.Uint32

func step8CipherPair(t *testing.T) (*security.SessionCipher, *security.SessionCipher) {
	t.Helper()
	var c2s, s2c [32]byte
	seed := byte(step8PairCounter.Add(1))
	for i := range c2s {
		c2s[i] = byte(i+1) ^ seed
		s2c[i] = byte(i+33) ^ seed
	}
	client, err := security.NewSessionCipher(c2s, s2c, false)
	if err != nil {
		t.Fatal(err)
	}
	server, err := security.NewSessionCipher(s2c, c2s, true)
	if err != nil {
		t.Fatal(err)
	}
	return client, server
}

func TestStep8_WireSniffing_Confidentiality_BR06(t *testing.T) {
	client, server := step8CipherPair(t)
	plaintext := []byte("Password@123 BiMatNoiBo_2026_XYZ")
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgMessUnicast, StreamID: protocol.StreamDirectChat, ConnectionID: 9, SeqNum: 1}
	raw, err := client.EncryptPacket(hdr, plaintext)
	if err != nil {
		t.Fatal(err)
	}
	wireHeader, err := protocol.UnmarshalHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if !wireHeader.HasFlag(protocol.FlagENC) {
		t.Fatal("encrypted packet did not set FlagENC")
	}
	if bytes.Contains(raw, plaintext) || bytes.Contains(raw, []byte("Password@123")) {
		t.Fatal("plaintext leaked onto wire")
	}
	decoded, err := server.DecryptRawPacket(raw)
	if err != nil || !bytes.Equal(decoded.Payload, plaintext) {
		t.Fatalf("decrypt failed: %v", err)
	}
}

func TestStep8_HeaderTampering_AAD_Protection(t *testing.T) {
	client, server := step8CipherPair(t)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgMessUnicast, StreamID: protocol.StreamDirectChat, ConnectionID: 7, SeqNum: 100}
	raw, _ := client.EncryptPacket(hdr, []byte("secret"))
	raw[3] ^= 1
	changed, err := protocol.UnmarshalHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.DecryptPacket(changed, raw[:protocol.HeaderSize], raw[protocol.HeaderSize:]); !errors.Is(err, security.ErrAuthTagMismatch) {
		t.Fatalf("tamper error=%v", err)
	}
}

func TestStep8_AntiReplay_Protection(t *testing.T) {
	client, server := step8CipherPair(t)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgMessUnicast, StreamID: protocol.StreamDirectChat, ConnectionID: 7, SeqNum: 1}
	raw, _ := client.EncryptPacket(hdr, []byte("once"))
	if _, err := server.DecryptRawPacket(raw); err != nil {
		t.Fatal(err)
	}
	if _, err := server.DecryptRawPacket(raw); !errors.Is(err, security.ErrReplayDetected) {
		t.Fatalf("replay error=%v", err)
	}
}

func TestStep8_SessionIsolation_BR07(t *testing.T) {
	alice, _ := step8CipherPair(t)
	charlie, bob := step8CipherPair(t)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgMessUnicast, StreamID: protocol.StreamDirectChat, ConnectionID: 11, SeqNum: 1}
	raw, _ := alice.EncryptPacket(hdr, []byte("Alice private message"))
	if _, err := bob.DecryptRawPacket(raw); !errors.Is(err, security.ErrAuthTagMismatch) {
		t.Fatalf("wrong-session decrypt error=%v", err)
	}
	if _, err := charlie.DecryptRawPacket(raw); !errors.Is(err, security.ErrAuthTagMismatch) && !errors.Is(err, security.ErrInvalidDirection) {
		t.Fatalf("wrong-session decrypt error=%v", err)
	}
}

// TestStep8_ClientRejectsUnencryptedDiscoUpdate confirms plaintext presence cannot modify an encrypted session.
func TestStep8_ClientRejectsUnencryptedDiscoUpdate(t *testing.T) {
	clientConn, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP: net.ParseIP("127.0.0.1"),
	})
	if err != nil {
		t.Skipf("loopback UDP unavailable: %v", err)
	}
	defer clientConn.Close()
	attackerConn, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP: net.ParseIP("127.0.0.1"),
	})
	if err != nil {
		t.Skipf("loopback UDP unavailable: %v", err)
	}
	defer attackerConn.Close()

	clientCipher, _ := step8CipherPair(t)
	store := clientstate.NewClientStore()
	store.ReplaceOnlinePeers([]protocol.PeerDTO{
		{
			Username: "Bob",
			Status:   "ONLINE",
		},
	})
	chat := clientapp.NewChatClient(clientConn, attackerConn.LocalAddr().(*net.UDPAddr), 77, store)
	chat.SetCipher(clientCipher)
	ctx, cancel := context.WithCancel(context.Background())
	listenDone := make(chan error, 1)
	go func() {
		listenDone <- chat.Listen(ctx)
	}()

	payload, err := protocol.EncodeJSONPayload(protocol.DiscoUpdatePayload{
		Event: "LEAVE",
		Peer: protocol.PeerDTO{
			Username: "Bob",
			Status:   "OFFLINE",
		},
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	raw, err := protocol.MarshalPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagREL | protocol.FlagORD,
			MsgType:      protocol.MsgDiscoUpdate,
			StreamID:     protocol.StreamControl,
			ConnectionID: 77,
			WindowSize:   protocol.DefaultWindowSize,
		},
		Payload: payload,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	if _, err = attackerConn.WriteToUDP(raw, clientConn.LocalAddr().(*net.UDPAddr)); err != nil {
		cancel()
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	cancel()
	select {
	case <-listenDone:
	case <-time.After(3 * time.Second):
		t.Fatal("ChatClient.Listen did not stop after cancellation")
	}
	if bob, ok := store.GetPeer("Bob"); !ok || bob.Status != "ONLINE" {
		t.Fatalf("plaintext LEAVE modified Bob's presence: %#v, found=%v", bob, ok)
	}
}
