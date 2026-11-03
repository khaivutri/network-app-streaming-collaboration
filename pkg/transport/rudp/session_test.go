package rudp

import (
	"context"
	"net"
	"sync"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestSessionQueuesAreNonBlocking(t *testing.T) {
	s := NewSession(1, &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 1}, 1, context.Background())
	p := &protocol.Packet{}
	if !s.EnqueueInbound(p) || s.EnqueueInbound(p) {
		t.Fatal("inbound enqueue should drop when full")
	}
	if !s.EnqueueOutbound([]byte{1}) || s.EnqueueOutbound([]byte{2}) {
		t.Fatal("outbound enqueue should drop when full")
	}
}

func TestSessionCloseIsIdempotentAndRaceSafe(t *testing.T) {
	s := NewSession(2, nil, 4, context.Background())
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); s.Close("test"); _ = s.IsClosed(); _ = s.LastActive() }()
	}
	wg.Wait()
	if !s.IsClosed() {
		t.Fatal("session should be closed")
	}
}
