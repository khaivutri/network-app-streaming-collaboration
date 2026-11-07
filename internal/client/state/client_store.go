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
	copy := *progress
	cs.fileTransfers[progress.TransferID] = &copy
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

// ClearSession returns the client to the login-screen state.
func (cs *ClientStore) ClearSession() {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.isLoggedIn = false
	cs.currentUser = nil
	clear(cs.onlinePeers)
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

// MarkDelivered updates the local receipt shown for a client-generated message.
func (cs *ClientStore) MarkDelivered(clientMsgID string) {
	cs.mu.Lock()
	defer cs.mu.Unlock()
	cs.deliveryStatus[clientMsgID] = "DELIVERED"
}

// DeliveryStatus returns the receipt state for a client message.
func (cs *ClientStore) DeliveryStatus(clientMsgID string) string {
	cs.mu.RLock()
	defer cs.mu.RUnlock()
	return cs.deliveryStatus[clientMsgID]
}
