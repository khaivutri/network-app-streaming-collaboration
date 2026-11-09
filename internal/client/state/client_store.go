package state

import (
	"sync"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// ConnectionState describes the local network connection state.
type ConnectionState string

const (
	ConnStateConnected    ConnectionState = "CONNECTED"
	ConnStateReconnecting ConnectionState = "RECONNECTING"
	ConnStateDisconnected ConnectionState = "DISCONNECTED"
)

// ClientStore is the concurrency-safe local projection of server presence.
type ClientStore struct {
	mu             sync.RWMutex
	isLoggedIn     bool
	currentUser    *protocol.UserInfoDTO
	onlinePeers    map[string]protocol.PeerDTO
	bcastMessages  []protocol.BroadcastMessageServerPayload
	directMessages map[string][]protocol.UnicastMessageServerPayload
	deliveryStatus map[string]string
	fileTransfers  map[string]*transfer.TransferProgress
	completedPaths map[string]string
	connState      ConnectionState
	stateListeners []chan ConnectionState
}

// NewClientStore creates an empty unauthenticated projection.
func NewClientStore() *ClientStore {
	return &ClientStore{
		onlinePeers:    make(map[string]protocol.PeerDTO),
		directMessages: make(map[string][]protocol.UnicastMessageServerPayload),
		deliveryStatus: make(map[string]string),
		fileTransfers:  make(map[string]*transfer.TransferProgress),
		completedPaths: make(map[string]string),
		connState:      ConnStateDisconnected,
	}
}

// UpdateFileProgress stores a detached file-transfer progress snapshot.
func (cs *ClientStore) UpdateFileProgress(progress *transfer.TransferProgress) {
	if progress == nil {
		return
	}
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if progress.TransferID == "" {
		return
	}
	merged := transfer.TransferProgress{}
	if existing := cs.fileTransfers[progress.TransferID]; existing != nil {
		merged = *existing
	}
	merged = mergeTransferProgress(merged, *progress)
	cs.fileTransfers[progress.TransferID] = &merged
	if merged.LocalPath != "" {
		cs.completedPaths[merged.TransferID] = merged.LocalPath
	}
}

// GetFileProgress returns a detached file-transfer progress snapshot.
func (cs *ClientStore) GetFileProgress(id string) (*transfer.TransferProgress, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	progress, ok := cs.fileTransfers[id]
	if !ok || progress == nil {
		return nil, false
	}
	copy := *progress
	return &copy, true
}

// SetLoginSuccess replaces stale presence with the server snapshot.
func (cs *ClientStore) SetLoginSuccess(user *protocol.UserInfoDTO, peers []protocol.PeerDTO) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.isLoggedIn = true
	if user != nil {
		copy := *user
		cs.currentUser = &copy
	} else {
		cs.currentUser = nil
	}
	clear(cs.onlinePeers)
	cs.bcastMessages = nil
	clear(cs.directMessages)
	clear(cs.deliveryStatus)
	clear(cs.fileTransfers)
	clear(cs.completedPaths)
	for _, peer := range peers {
		cs.onlinePeers[peer.Username] = peer
	}
}

// ApplyDiscoUpdate applies one incremental JOIN or LEAVE event.
func (cs *ClientStore) ApplyDiscoUpdate(update protocol.DiscoUpdatePayload) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if update.Event == "JOIN" {
		cs.onlinePeers[update.Peer.Username] = update.Peer
	}
	if update.Event == "LEAVE" {
		delete(cs.onlinePeers, update.Peer.Username)
	}
}

// ReplaceOnlinePeers atomically replaces the presence snapshot from the server.
func (cs *ClientStore) ReplaceOnlinePeers(peers []protocol.PeerDTO) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	clear(cs.onlinePeers)
	for _, peer := range peers {
		cs.onlinePeers[peer.Username] = peer
	}
}

// ClearSession returns the client to the login-screen state.
func (cs *ClientStore) ClearSession() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.isLoggedIn = false
	cs.currentUser = nil
	clear(cs.onlinePeers)
	cs.bcastMessages = nil
	clear(cs.directMessages)
	clear(cs.deliveryStatus)
	clear(cs.fileTransfers)
	clear(cs.completedPaths)
	cs.connState = ConnStateDisconnected
}

// SetConnectionState updates connection state and notifies listeners without blocking.
func (cs *ClientStore) SetConnectionState(next ConnectionState) {
	cs.mu.Lock()
	cs.connState = next
	listeners := append([]chan ConnectionState(nil), cs.stateListeners...)
	cs.mu.Unlock()
	for _, listener := range listeners {
		select {
		case listener <- next:
		default:
		}
	}
}

// ConnectionState returns the current connection state.
func (cs *ClientStore) ConnectionState() ConnectionState {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.connState
}

// SubscribeConnectionState registers a buffered state-event channel.
func (cs *ClientStore) SubscribeConnectionState() <-chan ConnectionState {
	ch := make(chan ConnectionState, 4)
	cs.mu.Lock()
	cs.stateListeners = append(cs.stateListeners, ch)
	cs.mu.Unlock()
	return ch
}

// IsLoggedIn reports the current local session state.
func (cs *ClientStore) IsLoggedIn() bool { cs.mu.RLock(); defer cs.mu.RUnlock(); return cs.isLoggedIn }

// CurrentUser returns a detached value so callers cannot race the store.
func (cs *ClientStore) CurrentUser() *protocol.UserInfoDTO {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	if cs.currentUser == nil {
		return nil
	}
	copy := *cs.currentUser
	return &copy
}

// GetOnlinePeers returns a detached slice for rendering.
func (cs *ClientStore) GetOnlinePeers() []protocol.PeerDTO {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	peers := make([]protocol.PeerDTO, 0, len(cs.onlinePeers))
	for _, peer := range cs.onlinePeers {
		peers = append(peers, peer)
	}
	return peers
}

// AppendBroadcastMessage adds one room message in receive order.
func (cs *ClientStore) AppendBroadcastMessage(message protocol.BroadcastMessageServerPayload) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.bcastMessages = append(cs.bcastMessages, message)
	if message.ClientMsgID != "" && message.Sender != "" {
		cs.deliveryStatus[message.ClientMsgID] = "DELIVERED"
	}
}

// AppendDirectMessage adds one direct message under the other participant's username.
func (cs *ClientStore) AppendDirectMessage(message protocol.UnicastMessageServerPayload) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	peer := message.Sender
	if current := cs.currentUser; current != nil && message.Sender == current.Username {
		peer = message.Recipient
	}
	cs.directMessages[peer] = append(cs.directMessages[peer], message)
	if message.ClientMsgID != "" {
		cs.deliveryStatus[message.ClientMsgID] = "DELIVERED"
	}
}

// GetBroadcastMessages returns a detached room-message snapshot.
func (cs *ClientStore) GetBroadcastMessages() []protocol.BroadcastMessageServerPayload {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return append([]protocol.BroadcastMessageServerPayload(nil), cs.bcastMessages...)
}

// GetDirectMessages returns a detached direct-message snapshot for peer.
func (cs *ClientStore) GetDirectMessages(peer string) []protocol.UnicastMessageServerPayload {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return append([]protocol.UnicastMessageServerPayload(nil), cs.directMessages[peer]...)
}

// GetAllDirectMessages returns a detached map suitable for session bootstrap.
func (cs *ClientStore) GetAllDirectMessages() map[string][]protocol.UnicastMessageServerPayload {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make(map[string][]protocol.UnicastMessageServerPayload, len(cs.directMessages))
	for peer, messages := range cs.directMessages {
		result[peer] = append([]protocol.UnicastMessageServerPayload(nil), messages...)
	}
	return result
}

// GetDeliveryStatuses returns a detached message-status map.
func (cs *ClientStore) GetDeliveryStatuses() map[string]string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make(map[string]string, len(cs.deliveryStatus))
	for id, status := range cs.deliveryStatus {
		result[id] = status
	}
	return result
}

// GetFileProgresses returns detached transfer snapshots for UI bootstrap.
func (cs *ClientStore) GetFileProgresses() []*transfer.TransferProgress {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	result := make([]*transfer.TransferProgress, 0, len(cs.fileTransfers))
	for _, progress := range cs.fileTransfers {
		if progress == nil {
			continue
		}
		copy := *progress
		result = append(result, &copy)
	}
	return result
}

// MarkDelivered updates the local receipt shown for a client-generated message.
func (cs *ClientStore) MarkDelivered(clientMsgID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.deliveryStatus[clientMsgID] = "DELIVERED"
}

// MarkFailed records an asynchronous server rejection for a client message.
func (cs *ClientStore) MarkFailed(clientMsgID, code, message string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	if clientMsgID == "" {
		return
	}
	cs.deliveryStatus[clientMsgID] = "FAILED"
}

// DeliveryStatus returns the receipt state for a client message.
func (cs *ClientStore) DeliveryStatus(clientMsgID string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.deliveryStatus[clientMsgID]
}

// GetCompletedFilePath returns a detached verified download path by transfer ID.
func (cs *ClientStore) GetCompletedFilePath(transferID string) (string, bool) {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	path, ok := cs.completedPaths[transferID]
	return path, ok
}

func mergeTransferProgress(previous, next transfer.TransferProgress) transfer.TransferProgress {
	if next.TransferID != "" {
		previous.TransferID = next.TransferID
	}
	if next.Direction != "" {
		previous.Direction = next.Direction
	}
	if next.Peer != "" {
		previous.Peer = next.Peer
	}
	if next.FileName != "" {
		previous.FileName = next.FileName
	}
	if next.FileSize != 0 {
		previous.FileSize = next.FileSize
	}
	if next.TransferredBytes != 0 || next.State == "INIT" {
		previous.TransferredBytes = next.TransferredBytes
	}
	if next.Percentage != 0 || next.State == "INIT" {
		previous.Percentage = next.Percentage
	}
	if next.State != "" {
		previous.State = next.State
	}
	if next.CreatedAt != 0 && previous.CreatedAt == 0 {
		previous.CreatedAt = next.CreatedAt
	}
	if next.UpdatedAt != 0 {
		previous.UpdatedAt = next.UpdatedAt
	}
	if next.ErrorMessage != "" {
		previous.ErrorMessage = next.ErrorMessage
	}
	if next.LocalPath != "" {
		previous.LocalPath = next.LocalPath
	}
	return previous
}
