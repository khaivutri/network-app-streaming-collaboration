package integration

import (
	"context"
	"crypto/rand"
	"net"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

type resilienceHandler struct{}

func (resilienceHandler) OnSessionConnected(*rudp.Session)                      {}
func (resilienceHandler) OnMessageReceived(*rudp.Session, uint8, uint8, []byte) {}
func (resilienceHandler) OnSessionClosed(*rudp.Session, string)                 {}

func TestStep7_Fuzzing_MalformedPacketsResilience(t *testing.T) {
	mux, err := rudp.NewMux(rudp.DefaultMuxConfig("127.0.0.1:0"), resilienceHandler{})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = mux.Serve(ctx) }()
	time.Sleep(10 * time.Millisecond)
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	target := mux.Addr().(*net.UDPAddr)
	for i := 0; i < 1000; i++ {
		length := 1 + i%2000
		payload := make([]byte, length)
		if _, err := rand.Read(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := conn.WriteToUDP(payload, target); err != nil {
			t.Fatal(err)
		}
	}
	valid, err := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.WriteToUDP(valid, target); err != nil {
		t.Fatal(err)
	}
	// The assertion is intentionally about lifecycle safety: malformed traffic
	// must not prevent the reactor from shutting down cleanly.
	time.Sleep(20 * time.Millisecond)
	if err := mux.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}
