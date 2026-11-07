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

// ReserveReceiverSeq reserves the next reliable sequence number for relayed data.
// The registry lock serializes reservations across concurrent packet handlers,
// preserving receiver stream order while updating the canonical transfer entry.
func ReserveReceiverSeq(transferID string) (uint32, bool) {
	GlobalTransfersMu.Lock()
	defer GlobalTransfersMu.Unlock()
	transfer, ok := GlobalActiveTransfers[transferID]
	if !ok || transfer == nil {
		return 0, false
	}
	if transfer.ReceiverNextSeq == 0 {
		transfer.ReceiverNextSeq = 1
	}
	sequence := transfer.ReceiverNextSeq
	transfer.ReceiverNextSeq++
	return sequence, true
}

// LookupActiveTransferByReceiverConnID finds the transfer routed to a receiver.
func LookupActiveTransferByReceiverConnID(connectionID uint32) (ActiveTransfer, bool) {
	GlobalTransfersMu.RLock()
	defer GlobalTransfersMu.RUnlock()
	for _, transfer := range GlobalActiveTransfers {
		if transfer != nil && transfer.ReceiverConnID == connectionID {
			return *transfer, true
		}
	}
	return ActiveTransfer{}, false
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
