package rudp

import (
	"errors"
	"fmt"
	"sync"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

var (
	ErrNoAvailableStreamID = errors.New("rudp: no available file stream id")
	ErrInvalidStreamID     = errors.New("rudp: stream id outside file range")
)

// StreamPool allocates independent file-transfer stream IDs safely.
type StreamPool struct {
	mu        sync.Mutex
	inUse     map[uint8]bool
	nextIndex uint8
}

// NewStreamPool creates a pool covering stream IDs 0x03 through 0xFE.
func NewStreamPool() *StreamPool {
	return &StreamPool{inUse: make(map[uint8]bool), nextIndex: protocol.StreamFileMin}
}

// Acquire reserves one stream ID using round-robin allocation.
func (pool *StreamPool) Acquire() (uint8, error) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	for count := 0; count <= int(protocol.StreamFileMax-protocol.StreamFileMin); count++ {
		candidate := pool.nextIndex
		if pool.nextIndex == protocol.StreamFileMax {
			pool.nextIndex = protocol.StreamFileMin
		} else {
			pool.nextIndex++
		}
		if !pool.inUse[candidate] {
			pool.inUse[candidate] = true
			return candidate, nil
		}
	}
	return 0, ErrNoAvailableStreamID
}

// Release returns a stream ID to the pool.
func (pool *StreamPool) Release(streamID uint8) error {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	if streamID < protocol.StreamFileMin || streamID > protocol.StreamFileMax {
		return fmt.Errorf("%w: %d", ErrInvalidStreamID, streamID)
	}
	delete(pool.inUse, streamID)
	return nil
}

// InUseCount returns the number of reserved stream IDs.
func (pool *StreamPool) InUseCount() int {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	return len(pool.inUse)
}
