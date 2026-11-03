package state

import (
	"net"
	"sync"
	"testing"
)

func TestRegisterPreventsDuplicateAndIsRaceSafe(t *testing.T) {
	ResetGlobalState()
	defer ResetGlobalState()
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 9000}
	const count = 50
	var wg sync.WaitGroup
	var mu sync.Mutex
	okCount := 0
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(id uint32) {
			defer wg.Done()
			if _, _, err := RegisterAuthenticatedUser("Alice", "Alice", id, addr, [32]byte{}, make(chan []byte, 1)); err == nil {
				mu.Lock()
				okCount++
				mu.Unlock()
			}
			_ = GetSnapshotOnlinePeers()
		}(uint32(i + 1))
	}
	wg.Wait()
	if okCount != 1 {
		t.Fatalf("successful registrations = %d, want 1", okCount)
	}
}
