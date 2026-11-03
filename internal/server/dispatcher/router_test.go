package dispatcher

import (
	"context"
	"net"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

func TestRouterDispatchAndPanicRecovery(t *testing.T) {
	r := NewRouter()
	called := make(chan struct{}, 1)
	r.Register(1, func(*rudp.Session, uint8, []byte) error { called <- struct{}{}; return nil })
	s := rudp.NewSession(1, &net.UDPAddr{}, 1, context.Background())
	r.Dispatch(s, 0, 1, nil)
	select {
	case <-called:
	default:
		t.Fatal("handler was not called")
	}
	r.Register(2, func(*rudp.Session, uint8, []byte) error { panic("isolated") })
	r.Dispatch(s, 0, 2, nil)
	r.Dispatch(s, 0, 1, nil)
}
