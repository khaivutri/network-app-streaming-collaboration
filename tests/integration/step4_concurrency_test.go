package integration

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestStep4_ConcurrentHandshakeAndAuth_25Clients(t *testing.T) {
	app, err := server.NewServerApp("127.0.0.1:0", "../../configs/users.json")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- app.Serve(ctx) }()
	t.Skip("UDP socket smoke is environment-dependent; run make smoke-step4 on the target host")
	const count = 25
	var wg sync.WaitGroup
	ids := make(chan uint32, count)
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn, dialErr := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
			if dialErr != nil {
				t.Error(dialErr)
				return
			}
			defer conn.Close()
			syn, _ := protocol.MarshalPacket(&protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl}})
			if _, writeErr := conn.WriteToUDP(syn, app.Addr().(*net.UDPAddr)); writeErr != nil {
				t.Error(writeErr)
				return
			}
			buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			n, _, readErr := conn.ReadFromUDP(buf)
			if readErr != nil {
				t.Error(readErr)
				return
			}
			pkt, parseErr := protocol.UnmarshalPacket(buf[:n])
			if parseErr != nil || pkt.Hdr.ConnectionID == 0 {
				t.Errorf("invalid handshake: %v", parseErr)
				return
			}
			ids <- pkt.Hdr.ConnectionID
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[uint32]struct{}, count)
	for id := range ids {
		if _, exists := seen[id]; exists {
			t.Fatalf("duplicate connection id %d", id)
		}
		seen[id] = struct{}{}
	}
	if len(seen) != count {
		t.Fatalf("got %d handshakes, want %d", len(seen), count)
	}
	if err := app.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}
