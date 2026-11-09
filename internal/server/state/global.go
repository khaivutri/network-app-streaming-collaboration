package state

import (
	"errors"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"net"
	"sync"
	"time"
)

var (
	// ErrUserAlreadyOnline enforces BR02 at the state boundary.
	ErrUserAlreadyOnline = errors.New("state: user already has an active online session")
	// ErrSessionNotFound identifies a missing authenticated session.
	ErrSessionNotFound = errors.New("state: user session not found")
	// ErrConnectionAlreadyInUse protects reverse-index uniqueness.
	ErrConnectionAlreadyInUse = errors.New("state: connection ID is already in use")
	// GlobalUsersMu protects both indexes as one invariant-preserving pair.
	GlobalUsersMu sync.RWMutex
	// GlobalOnlineUsers is the authoritative username-to-session index.
	GlobalOnlineUsers = make(map[string]*UserSession)
	// GlobalConnIndex provides O(1) reverse lookup from wire connection ID.
	GlobalConnIndex = make(map[uint32]string)
)

// UserSession contains server-side network and presence state for one user.
type UserSession struct {
	Username        string
	DisplayName     string
	ConnectionID    uint32
	IP              string
	Port            int
	UDPAddr         *net.UDPAddr
	SessionKey      [32]byte
	LoginAt         time.Time
	LastHeartbeatAt time.Time
	MissedPings     int
	OutboundCh      chan []byte
	Cipher          *security.SessionCipher
}

// sessionPeer converts internal state into a detached wire DTO.
func sessionPeer(session *UserSession, status string) protocol.PeerDTO {
	return protocol.PeerDTO{
		Username:    session.Username,
		DisplayName: session.DisplayName,
		IP:          session.IP,
		Port:        session.Port,
		Status:      status,
		LoginAt:     session.LoginAt.UnixMilli(),
	}
}

// RegisterAuthenticatedUser atomically checks BR02 and records both indexes.
// Returned channels are snapshots; socket I/O must happen after unlocking.
func RegisterAuthenticatedUser(username, displayName string, connID uint32, remoteAddr *net.UDPAddr, sessionKey [32]byte, outboundCh chan []byte) (allPeers []protocol.PeerDTO, otherTargets []chan []byte, err error) {
	return registerAuthenticatedUser(username, displayName, connID, remoteAddr, sessionKey, outboundCh, nil)
}

// RegisterAuthenticatedUserWithCipher records a user and its hop encryption context.
func RegisterAuthenticatedUserWithCipher(username, displayName string, connID uint32, remoteAddr *net.UDPAddr, sessionKey [32]byte, outboundCh chan []byte, cipher *security.SessionCipher) (allPeers []protocol.PeerDTO, otherTargets []chan []byte, err error) {
	return registerAuthenticatedUser(username, displayName, connID, remoteAddr, sessionKey, outboundCh, cipher)
}

func registerAuthenticatedUser(username, displayName string, connID uint32, remoteAddr *net.UDPAddr, sessionKey [32]byte, outboundCh chan []byte, cipher *security.SessionCipher) (allPeers []protocol.PeerDTO, otherTargets []chan []byte, err error) {
	GlobalUsersMu.Lock()
	defer GlobalUsersMu.Unlock()
	if _, exists := GlobalOnlineUsers[username]; exists {
		return nil, nil, ErrUserAlreadyOnline
	}
	if _, exists := GlobalConnIndex[connID]; exists {
		return nil, nil, ErrConnectionAlreadyInUse
	}
	now := time.Now()
	// The maps and their reverse index must change under one lock so a concurrent login cannot pass BR02.
	session := &UserSession{
		Username:        username,
		DisplayName:     displayName,
		ConnectionID:    connID,
		IP:              remoteAddr.IP.String(),
		Port:            remoteAddr.Port,
		UDPAddr:         remoteAddr,
		SessionKey:      sessionKey,
		LoginAt:         now,
		LastHeartbeatAt: now,
		OutboundCh:      outboundCh,
		Cipher:          cipher,
	}
	GlobalOnlineUsers[username] = session
	GlobalConnIndex[connID] = username
	allPeers = make([]protocol.PeerDTO, 0, len(GlobalOnlineUsers))
	otherTargets = make([]chan []byte, 0, len(GlobalOnlineUsers)-1)
	for _, item := range GlobalOnlineUsers {
		allPeers = append(allPeers, sessionPeer(item, "ONLINE"))
		if item.Username != username && item.OutboundCh != nil {
			otherTargets = append(otherTargets, item.OutboundCh)
		}
	}
	return allPeers, otherTargets, nil
}

// RemoveUserByUsername removes a session and snapshots remaining targets.
func RemoveUserByUsername(username string) (removed *protocol.PeerDTO, remainingTargets []chan []byte, existed bool) {
	GlobalUsersMu.Lock()
	defer GlobalUsersMu.Unlock()
	session, ok := GlobalOnlineUsers[username]
	if !ok {
		return nil, nil, false
	}
	peer := sessionPeer(session, "OFFLINE")
	removed = &peer
	delete(GlobalOnlineUsers, username)
	delete(GlobalConnIndex, session.ConnectionID)
	remainingTargets = make([]chan []byte, 0, len(GlobalOnlineUsers))
	for _, item := range GlobalOnlineUsers {
		if item.OutboundCh != nil {
			remainingTargets = append(remainingTargets, item.OutboundCh)
		}
	}
	return removed, remainingTargets, true
}

// RemoveUserByConnID removes the session associated with a wire connection ID.
func RemoveUserByConnID(connID uint32) (removed *protocol.PeerDTO, remainingTargets []chan []byte, existed bool) {
	GlobalUsersMu.RLock()
	username, ok := GlobalConnIndex[connID]
	GlobalUsersMu.RUnlock()
	if !ok {
		return nil, nil, false
	}
	return RemoveUserByUsername(username)
}

// GetSnapshotOnlinePeers returns a detached copy safe after unlocking.
func GetSnapshotOnlinePeers() []protocol.PeerDTO {
	GlobalUsersMu.RLock()
	defer GlobalUsersMu.RUnlock()
	peers := make([]protocol.PeerDTO, 0, len(GlobalOnlineUsers))
	for _, session := range GlobalOnlineUsers {
		peers = append(peers, sessionPeer(session, "ONLINE"))
	}
	return peers
}

// LookupTargetByUsername returns a shallow copy for downstream routing.
func LookupTargetByUsername(username string) (UserSession, bool) {
	GlobalUsersMu.RLock()
	defer GlobalUsersMu.RUnlock()
	session, ok := GlobalOnlineUsers[username]
	if !ok {
		return UserSession{}, false
	}
	copy := *session
	return copy, true
}

// LookupUsernameByConnID resolves the authenticated packet identity.
func LookupUsernameByConnID(connID uint32) (string, bool) {
	GlobalUsersMu.RLock()
	defer GlobalUsersMu.RUnlock()
	username, ok := GlobalConnIndex[connID]
	return username, ok
}

// LookupTargetByConnID returns the session snapshot associated with a connection.
func LookupTargetByConnID(connID uint32) (UserSession, bool) {
	GlobalUsersMu.RLock()
	defer GlobalUsersMu.RUnlock()
	username, ok := GlobalConnIndex[connID]
	if !ok {
		return UserSession{}, false
	}
	session, ok := GlobalOnlineUsers[username]
	if !ok {
		return UserSession{}, false
	}
	return *session, true
}

// TouchHeartbeat updates liveness while preserving both map invariants.
func TouchHeartbeat(connID uint32, latestAddr *net.UDPAddr) bool {
	GlobalUsersMu.Lock()
	defer GlobalUsersMu.Unlock()
	username, ok := GlobalConnIndex[connID]
	if !ok {
		return false
	}
	session, ok := GlobalOnlineUsers[username]
	if !ok {
		return false
	}
	session.LastHeartbeatAt = time.Now()
	session.MissedPings = 0
	if latestAddr != nil {
		session.UDPAddr, session.IP, session.Port = latestAddr, latestAddr.IP.String(), latestAddr.Port
	}
	return true
}

// ResetGlobalState is test-only cleanup; production removes sessions individually.
func ResetGlobalState() {
	GlobalUsersMu.Lock()
	defer GlobalUsersMu.Unlock()
	clear(GlobalOnlineUsers)
	clear(GlobalConnIndex)
}
