// Package rudp provides the UDP handshake foundation and deterministic state machines.
package rudp

import (
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
)

const (
	HandshakeTimeout      = time.Second
	MaxHandshakeRetries   = 3
	FINRetransmitTimeout  = 800 * time.Millisecond
	MaxFINRetries         = 3
	CloseReadyCleanupWait = 2 * time.Second
	MaxMissedHeartbeats   = 3
)

// Action is a deterministic state-machine output.
type Action struct {
	Packet      *protocol.Packet
	Discard     bool
	CloseReason transport.CloseReason
}

// ClientStateMachine models the five client connection states.
type ClientStateMachine struct {
	mu               sync.Mutex
	state            transport.ConnState
	connectionID     uint32
	retries          int
	finRetries       int
	missedHeartbeats int
	reason           transport.CloseReason
}

// NewClientStateMachine constructs a closed client state machine.
func NewClientStateMachine() *ClientStateMachine {
	return &ClientStateMachine{state: transport.StateClosed}
}

// State returns the current client state.
func (m *ClientStateMachine) State() transport.ConnState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// ConnectionID returns the assigned connection identifier.
func (m *ClientStateMachine) ConnectionID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectionID
}

// Retries returns the handshake retry count.
func (m *ClientStateMachine) Retries() int { m.mu.Lock(); defer m.mu.Unlock(); return m.retries }

// Connect starts a client handshake.
func (m *ClientStateMachine) Connect() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = transport.StateSynSent
	return Action{Packet: controlPacket(protocol.FlagSYN, 0, 0, 0)}
}

// OnPacket consumes a packet for the current client state.
func (m *ClientStateMachine) OnPacket(pkt *protocol.Packet) Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pkt == nil {
		return Action{Discard: true}
	}
	if pkt.Hdr.HasFlag(protocol.FlagRST) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonPeerReset
		return Action{CloseReason: m.reason}
	}
	if m.state == transport.StateSynSent && pkt.Hdr.HasFlag(protocol.FlagSYN) && pkt.Hdr.HasFlag(protocol.FlagACK) && pkt.Hdr.ConnectionID != 0 {
		m.connectionID = pkt.Hdr.ConnectionID
		m.state = transport.StateConnected
		return Action{Packet: controlPacket(protocol.FlagACK, m.connectionID, 1, 1)}
	}
	if m.state == transport.StateFinWait && pkt.Hdr.HasFlag(protocol.FlagFIN) && pkt.Hdr.HasFlag(protocol.FlagACK) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateCloseReady
		return Action{}
	}
	if m.state == transport.StateCloseReady && pkt.Hdr.HasFlag(protocol.FlagFIN) && pkt.Hdr.ConnectionID == m.connectionID {
		return Action{Packet: controlPacket(protocol.FlagFIN|protocol.FlagACK, m.connectionID, 1, pkt.Hdr.SeqNum+1)}
	}
	return Action{Discard: true}
}

// Disconnect starts graceful client teardown.
func (m *ClientStateMachine) Disconnect() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateConnected {
		return Action{Discard: true}
	}
	m.state = transport.StateFinWait
	return Action{Packet: controlPacket(protocol.FlagFIN, m.connectionID, 2, 1)}
}

// OnHandshakeTimeout retransmits SYN or closes after the retry budget.
func (m *ClientStateMachine) OnHandshakeTimeout() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateSynSent {
		return Action{Discard: true}
	}
	if m.retries >= MaxHandshakeRetries {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonHandshakeTimeout
		return Action{CloseReason: m.reason}
	}
	m.retries++
	return Action{Packet: controlPacket(protocol.FlagSYN, 0, 0, 0)}
}

// OnCleanupTimeout ends the TIME_WAIT-like state.
func (m *ClientStateMachine) OnCleanupTimeout() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == transport.StateCloseReady {
		m.state = transport.StateClosed
	}
	return Action{}
}

// Cancel aborts the client session.
func (m *ClientStateMachine) Cancel() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state = transport.StateClosed
	m.reason = transport.CloseReasonCancelled
	return Action{CloseReason: m.reason}
}

// OnHeartbeatMissed closes after the configured missed-heartbeat threshold.
func (m *ClientStateMachine) OnHeartbeatMissed() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.missedHeartbeats++
	if m.missedHeartbeats >= MaxMissedHeartbeats {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonHeartbeatTimeout
		return Action{CloseReason: m.reason}
	}
	return Action{}
}

// ServerStateMachine models the five server connection states.
type ServerStateMachine struct {
	mu               sync.Mutex
	state            transport.ConnState
	connectionID     uint32
	retries          int
	finRetries       int
	missedHeartbeats int
	reason           transport.CloseReason
}

// NewServerStateMachine constructs a server state machine.
func NewServerStateMachine() *ServerStateMachine {
	return &ServerStateMachine{state: transport.StateClosed}
}

// State returns the current server state.
func (m *ServerStateMachine) State() transport.ConnState {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state
}

// ConnectionID returns the assigned identifier.
func (m *ServerStateMachine) ConnectionID() uint32 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.connectionID
}

// AcceptSYN assigns an ID and returns SYN-ACK; duplicate SYN reuses the ID.
func (m *ServerStateMachine) AcceptSYN(id uint32) Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == transport.StateSynReceived {
		return Action{Packet: controlPacket(protocol.FlagSYN|protocol.FlagACK, m.connectionID, 0, 1)}
	}
	m.connectionID = id
	m.state = transport.StateSynReceived
	return Action{Packet: controlPacket(protocol.FlagSYN|protocol.FlagACK, id, 0, 1)}
}

// OnPacket consumes a server packet.
func (m *ServerStateMachine) OnPacket(pkt *protocol.Packet) Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if pkt == nil {
		return Action{Discard: true}
	}
	if pkt.Hdr.HasFlag(protocol.FlagRST) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonPeerReset
		return Action{CloseReason: m.reason}
	}
	if m.state == transport.StateSynReceived && pkt.Hdr.HasFlag(protocol.FlagACK) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateConnected
		return Action{}
	}
	if m.state == transport.StateFinWait && pkt.Hdr.HasFlag(protocol.FlagFIN) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateCloseReady
		return Action{Packet: controlPacket(protocol.FlagFIN|protocol.FlagACK, m.connectionID, 1, pkt.Hdr.SeqNum+1)}
	}
	if m.state == transport.StateFinWait && pkt.Hdr.HasFlag(protocol.FlagACK) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateCloseReady
		return Action{}
	}
	if m.state == transport.StateConnected && pkt.Hdr.HasFlag(protocol.FlagFIN) && pkt.Hdr.ConnectionID == m.connectionID {
		m.state = transport.StateCloseReady
		return Action{Packet: controlPacket(protocol.FlagFIN|protocol.FlagACK, m.connectionID, 1, pkt.Hdr.SeqNum+1)}
	}
	if m.state == transport.StateCloseReady && pkt.Hdr.HasFlag(protocol.FlagFIN) && pkt.Hdr.ConnectionID == m.connectionID {
		return Action{Packet: controlPacket(protocol.FlagFIN|protocol.FlagACK, m.connectionID, 1, pkt.Hdr.SeqNum+1)}
	}
	return Action{Discard: true}
}

// OnHandshakeTimeout retransmits SYN-ACK while the handshake retry budget remains.
func (m *ServerStateMachine) OnHandshakeTimeout() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateSynReceived {
		return Action{Discard: true}
	}
	if m.retries >= MaxHandshakeRetries {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonHandshakeTimeout
		return Action{CloseReason: m.reason}
	}
	m.retries++
	return Action{Packet: controlPacket(protocol.FlagSYN|protocol.FlagACK, m.connectionID, 0, 1)}
}

// OnFINTimeout retransmits FIN and eventually force-closes into CLOSE_READY.
func (m *ServerStateMachine) OnFINTimeout() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateFinWait {
		return Action{Discard: true}
	}
	if m.finRetries >= MaxFINRetries {
		m.state = transport.StateCloseReady
		return Action{}
	}
	m.finRetries++
	return Action{Packet: controlPacket(protocol.FlagFIN, m.connectionID, 1, 1)}
}

// OnHeartbeatMissed aborts a connected server session after repeated heartbeat loss.
func (m *ServerStateMachine) OnHeartbeatMissed() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateConnected {
		return Action{Discard: true}
	}
	m.missedHeartbeats++
	if m.missedHeartbeats >= MaxMissedHeartbeats {
		m.state = transport.StateClosed
		m.reason = transport.CloseReasonHeartbeatTimeout
		return Action{CloseReason: m.reason}
	}
	return Action{}
}

// Disconnect starts server-initiated teardown.
func (m *ServerStateMachine) Disconnect() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state != transport.StateConnected {
		return Action{Discard: true}
	}
	m.state = transport.StateFinWait
	return Action{Packet: controlPacket(protocol.FlagFIN, m.connectionID, 1, 1)}
}

// OnCleanupTimeout closes a server in CLOSE_READY.
func (m *ServerStateMachine) OnCleanupTimeout() Action {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.state == transport.StateCloseReady {
		m.state = transport.StateClosed
	}
	return Action{}
}
func controlPacket(flags uint8, id, seq, ack uint32) *protocol.Packet {
	return &protocol.Packet{
		Hdr: protocol.Header{
			Version: protocol.ProtocolVersion, 
			Flags: flags, 
			MsgType: protocol.MsgCtrlHandshake, 
			StreamID: protocol.StreamControl, 
			ConnectionID: id, 
			SeqNum: seq, 
			AckNum: ack, 
			WindowSize: protocol.DefaultWindowSize,
		}}
}
