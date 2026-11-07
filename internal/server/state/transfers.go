package state

import (
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// ActiveTransfer stores routing metadata for one in-flight file transfer.
type ActiveTransfer struct {
	TransferID       string
	TransferIDBytes  [16]byte
	SenderUsername   string
	ReceiverUsername string
	SenderConnID     uint32
	ReceiverConnID   uint32
	SenderStreamID   uint8
	ReceiverStreamID uint8
	FileName         string
	FileSize         int64
	TotalChunks      uint32
	ChunkSize        int
	SHA256           string
	StartedAt        time.Time
	ReceiverNextSeq  uint32
}

// NextReceiverSeq reserves the next reliable sequence number for relayed data.
// The transfer registry lock serializes reservations across concurrent server
// packet handlers, preserving receiver stream order without buffering chunks.
func (transfer *ActiveTransfer) NextReceiverSeq() uint32 {
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	if transfer.ReceiverNextSeq == 0 {
		transfer.ReceiverNextSeq = 1
	}
	sequence := transfer.ReceiverNextSeq
	transfer.ReceiverNextSeq++
	return sequence
}

var (
	GlobalTransfersMu     sync.RWMutex
	GlobalActiveTransfers = make(map[string]*ActiveTransfer)
)

// RegisterActiveTransfer adds routing metadata to the global registry.
func RegisterActiveTransfer(transfer *ActiveTransfer) {
	if transfer == nil {
		return
	}
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	copy := *transfer
	GlobalActiveTransfers[transfer.TransferID] = &copy
}

// LookupActiveTransfer returns a detached transfer snapshot.
func LookupActiveTransfer(id string) (ActiveTransfer, bool) {
	GlobalTransfersMu.RLock()
	defer GlobalTransfersMu.RUnlock()
	value, ok := GlobalActiveTransfers[id]
	if !ok || value == nil {
		return ActiveTransfer{}, false
	}
	return *value, true
}

// LookupActiveTransferByBytes resolves the binary transfer identifier in a chunk.
func LookupActiveTransferByBytes(id [16]byte) (ActiveTransfer, bool) {
	return LookupActiveTransfer(protocol.FormatUUID(id))
}

// RemoveActiveTransfer removes and returns one transfer.
func RemoveActiveTransfer(id string) (*ActiveTransfer, bool) {
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	value, ok := GlobalActiveTransfers[id]
	if !ok {
		return nil, false
	}
	delete(GlobalActiveTransfers, id)
	return value, true
}

// CancelTransfersForUser removes all transfers involving a disconnected user.
func CancelTransfersForUser(username string) []*ActiveTransfer {
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	var cancelled []*ActiveTransfer
	for id, value := range GlobalActiveTransfers {
		if value != nil && (value.SenderUsername == username || value.ReceiverUsername == username) {
			copy := *value
			cancelled = append(cancelled, &copy)
			delete(GlobalActiveTransfers, id)
		}
	}
	return cancelled
}

// ResetActiveTransfersState clears the registry for tests and controlled shutdowns.
func ResetActiveTransfersState() {
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	clear(GlobalActiveTransfers)
}
