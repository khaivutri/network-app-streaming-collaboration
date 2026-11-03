package rudp

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

type muxTestHandler struct{ messages chan *Session }

func (h *muxTestHandler) OnSessionConnected(*Session)                              {}
func (h *muxTestHandler) OnMessageReceived(s *Session, _ uint8, _ uint8, _ []byte) { h.messages <- s }
func (h *muxTestHandler) OnSessionClosed(*Session, string)                         {}

func TestMuxHandshakeAndDemux(t *testing.T) {
	h := &muxTestHandler{messages: make(chan *Session, 1)}
	m, err := NewMux(DefaultMuxConfig("127.0.0.1:0"), h)
	if err != nil {
		t.Fatal(err)
	}
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 41001}
	m.handleSYN(addr)
	if m.ActiveSessionCount() != 1 {
		t.Fatalf("active sessions=%d", m.ActiveSessionCount())
	}
	m.sessionsMu.RLock()
	var id uint32
	for id = range m.sessions {
		break
	}
	m.sessionsMu.RUnlock()
	pkt := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgPing, StreamID: protocol.StreamControl, ConnectionID: id}}
	m.dispatch(mustMarshal(pkt), addr)
	select {
	case <-h.messages:
	case <-time.After(time.Second):
		t.Fatal("message was not demuxed")
	}
	if err := m.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func mustMarshal(pkt *protocol.Packet) []byte { raw, _ := protocol.MarshalPacket(pkt); return raw }
