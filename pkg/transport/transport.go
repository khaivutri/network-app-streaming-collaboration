// Package transport contains shared connection abstractions and identifiers.
package transport

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"net"
	"sync"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

var generatedIDs = struct {
	sync.Mutex
	values map[uint32]struct{}
}{values: make(map[uint32]struct{})}

// ConnState describes the lifecycle of a client or server session.
type ConnState uint8

const (
	StateClosed ConnState = iota
	StateSynSent
	StateSynReceived
	StateConnected
	StateFinWait
	StateCloseReady
)

func (s ConnState) String() string {
	names := [...]string{"CLOSED", "SYN_SENT", "SYN_RECEIVED", "CONNECTED", "FIN_WAIT", "CLOSE_READY"}
	if int(s) < len(names) {
		return names[s]
	}
	return fmt.Sprintf("UNKNOWN(%d)", s)
}

// CloseReason identifies why a session ended.
type CloseReason uint8

const (
	CloseReasonNormal CloseReason = iota
	CloseReasonHandshakeTimeout
	CloseReasonPeerReset
	CloseReasonHeartbeatTimeout
	CloseReasonCancelled
	CloseReasonServerShutdown
)

func (r CloseReason) String() string {
	names := [...]string{"NORMAL", "HANDSHAKE_TIMEOUT", "PEER_RESET", "HEARTBEAT_TIMEOUT", "CANCELLED", "SERVER_SHUTDOWN"}
	if int(r) < len(names) {
		return names[r]
	}
	return fmt.Sprintf("UNKNOWN(%d)", r)
}

// Session is the common connection contract used by later transport layers.
type Session interface {
	ConnectionID() uint32
	State() ConnState
	LocalAddr() net.Addr
	RemoteAddr() net.Addr
	SendPacket(*protocol.Packet) error
	Close() error
}

// GenerateConnectionID returns a non-zero cryptographically random identifier.
func GenerateConnectionID() (uint32, error) {
	var raw [4]byte
	for attempt := 0; attempt < 32; attempt++ {
		if _, err := rand.Read(raw[:]); err != nil {
			return 0, fmt.Errorf("generate connection id: %w", err)
		}
		if id := binary.BigEndian.Uint32(raw[:]); id != 0 {
			generatedIDs.Lock()
			_, exists := generatedIDs.values[id]
			if !exists {
				generatedIDs.values[id] = struct{}{}
			}
			generatedIDs.Unlock()
			if !exists {
				return id, nil
			}
		}
	}
	return 0, fmt.Errorf("generate connection id: random source returned unusable values repeatedly")
}
