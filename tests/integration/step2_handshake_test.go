package integration

import (
	"context"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
	"testing"
	"time"
)

func TestTCPHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s := tcp_handshake.NewServer("127.0.0.1:0", nil)
	addr, err := s.Listen()
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx) }()
	result, err := tcp_handshake.ConnectAndHandshake(ctx, addr.String())
	if err != nil || result.ConnectionID == 0 {
		t.Fatalf("%v %#v", err, result)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server did not stop")
	}
}
func TestUDPHandshake(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	s, err := rudp.NewUDPHandshakeServer("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go s.Serve(ctx)
	result, err := rudp.PerformUDPHandshake(ctx, s.Addr().String(), rudp.UDPConfig{HandshakeTimeout: 50 * time.Millisecond})
	if err != nil || result.ConnectionID == 0 {
		t.Fatalf("%v %#v", err, result)
	}
	_ = s.Close()
}
