package state

import (
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"sync"
)

// ClientStore is the concurrency-safe local projection of server presence.
type ClientStore struct {
	mu          sync.RWMutex
	isLoggedIn  bool
	currentUser *protocol.UserInfoDTO
	onlinePeers map[string]protocol.PeerDTO
}

// NewClientStore creates an empty unauthenticated projection.
func NewClientStore() *ClientStore {
	return &ClientStore{onlinePeers: make(map[string]protocol.PeerDTO)}
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
