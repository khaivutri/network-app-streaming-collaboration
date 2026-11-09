package rudp

import (
	"context"
	"encoding/base64"
	"net"
	"testing"
	"time"

	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestMuxCryptoHandshakeRejectsMalformedAndSupportsFallback(t *testing.T) {
	mux, err := NewMux(DefaultMuxConfig("127.0.0.1:0"), &muxTestHandler{messages: make(chan *Session, 2)})
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Shutdown(context.Background())
	addr := mux.Addr().(*net.UDPAddr)
	_ = addr
	clientAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 42001}
	malformed, _ := protocol.EncodeJSONPayload(protocol.HandshakeSYNPayload{ClientPubKey: "not-valid!", ClientNonce: base64.StdEncoding.EncodeToString(make([]byte, 16))})
	mux.handleSYN(clientAddr, &protocol.Packet{Payload: malformed})
	mux.sessionsMu.RLock()
	var malformedSession *Session
	for _, session := range mux.sessions {
		malformedSession = session
	}
	mux.sessionsMu.RUnlock()
	if malformedSession == nil || malformedSession.HasCipher() {
		t.Fatal("malformed key unexpectedly negotiated cipher")
	}
	if got := malformedSession.HandshakePayload(); len(got) != 0 {
		t.Fatal("malformed handshake stored crypto payload")
	}

	plainAddr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 42002}
	mux.handleSYN(plainAddr, &protocol.Packet{})
	mux.sessionsMu.RLock()
	count := len(mux.sessions)
	mux.sessionsMu.RUnlock()
	if count != 2 {
		t.Fatalf("fallback session count=%d", count)
	}
}

func TestMuxEncryptedSessionDropsPlaintextDataButAllowsAck(t *testing.T) {
	h := &muxTestHandler{messages: make(chan *Session, 2)}
	mux, err := NewMux(DefaultMuxConfig("127.0.0.1:0"), h)
	if err != nil {
		t.Fatal(err)
	}
	defer mux.Shutdown(context.Background())
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 42003}
	mux.handleSYN(addr)
	mux.sessionsMu.RLock()
	var id uint32
	var session *Session
	for id, session = range mux.sessions {
		break
	}
	mux.sessionsMu.RUnlock()
	var key [32]byte
	cipher, _ := security.NewSessionCipher(key, key, true)
	session.SetCipher(cipher)
	plain := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgPing, ConnectionID: id}}
	mux.dispatch(mustMarshal(plain), addr)
	select {
	case <-h.messages:
		t.Fatal("plaintext data was delivered")
	case <-time.After(30 * time.Millisecond):
	}
	ack := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagACK, MsgType: protocol.MsgAck, ConnectionID: id}}
	mux.dispatch(mustMarshal(ack), addr)
}
