package rudp

import (
	"errors"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestStreamPoolAcquireAndRelease(t *testing.T) {
	pool := NewStreamPool()
	id, err := pool.Acquire()
	if err != nil || id < protocol.StreamFileMin || id > protocol.StreamFileMax {
		t.Fatalf("invalid allocation %d: %v", id, err)
	}
	if err := pool.Release(id); err != nil {
		t.Fatal(err)
	}
	if pool.InUseCount() != 0 {
		t.Fatal("stream was not released")
	}
}

func TestStreamPoolExhaustionAndRecovery(t *testing.T) {
	pool := NewStreamPool()
	ids := make([]uint8, 0, 252)
	for i := 0; i < 252; i++ {
		id, err := pool.Acquire()
		if err != nil {
			t.Fatal(err)
		}
		ids = append(ids, id)
	}
	if _, err := pool.Acquire(); !errors.Is(err, ErrNoAvailableStreamID) {
		t.Fatal(err)
	}
	if err := pool.Release(ids[0]); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Acquire(); err != nil {
		t.Fatal(err)
	}
}
