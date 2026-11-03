package rudp

import (
	"context"
	"net"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// Session is the per-client actor context owned by a Mux.
type Session struct {
	id         uint32
	remoteAddr *net.UDPAddr
	inboundCh  chan *protocol.Packet
	outboundCh chan []byte
	closeCh    chan struct{}
	closeOnce  sync.Once
	mu         sync.RWMutex
	lastActive time.Time
	isClosed   bool
	closeErr   string
	ctx        context.Context
	cancel     context.CancelFunc
}

// NewSession creates a session with bounded inbound and outbound queues.
func NewSession(id uint32, remoteAddr *net.UDPAddr, queueSize int, parentCtx context.Context) *Session {
	if queueSize < 1 {
		queueSize = 1
	}
	sessCtx, cancel := context.WithCancel(parentCtx)
	return &Session{
		id:         id,
		remoteAddr: cloneUDPAddr(remoteAddr),
		inboundCh:  make(chan *protocol.Packet, queueSize),
		outboundCh: make(chan []byte, queueSize),
		closeCh:    make(chan struct{}),
		lastActive: time.Now(),
		ctx:        sessCtx,
		cancel:     cancel,
	}
}

// ID returns the wire connection identifier.
func (s *Session) ID() uint32 { return s.id }

// RemoteAddr returns a detached copy of the current peer address.
func (s *Session) RemoteAddr() *net.UDPAddr {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return cloneUDPAddr(s.remoteAddr)
}

// UpdateRemoteAddr records the latest peer address and activity timestamp.
func (s *Session) UpdateRemoteAddr(addr *net.UDPAddr) {
	s.mu.Lock()
	s.remoteAddr = cloneUDPAddr(addr)
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// Touch refreshes the session activity timestamp.
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// LastActive returns the last activity time.
func (s *Session) LastActive() time.Time {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastActive
}

// Context returns the session lifetime context.
func (s *Session) Context() context.Context { return s.ctx }

// InboundChan exposes the receive-only actor queue.
func (s *Session) InboundChan() <-chan *protocol.Packet { return s.inboundCh }

// OutboundChan exposes the receive-only egress queue.
func (s *Session) OutboundChan() <-chan []byte { return s.outboundCh }

// OutboundQueue exposes the writable queue to trusted server integrations.
func (s *Session) OutboundQueue() chan []byte { return s.outboundCh }

// CloseChan signals that the session has been closed.
func (s *Session) CloseChan() <-chan struct{} { return s.closeCh }

// EnqueueInbound adds a packet without ever blocking the ingress reader.
func (s *Session) EnqueueInbound(pkt *protocol.Packet) bool {
	s.mu.RLock()
	closed := s.isClosed
	s.mu.RUnlock()
	if closed {
		return false
	}
	select {
	case s.inboundCh <- pkt:
		s.Touch()
		return true
	default:
		return false
	}
}

// EnqueueOutbound adds raw data without allowing a slow consumer to block peers.
func (s *Session) EnqueueOutbound(rawPacket []byte) bool {
	s.mu.RLock()
	closed := s.isClosed
	s.mu.RUnlock()
	if closed {
		return false
	}
	select {
	case s.outboundCh <- rawPacket:
		return true
	default:
		return false
	}
}

// IsClosed reports whether Close has completed its state transition.
func (s *Session) IsClosed() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.isClosed
}

// Close marks the session closed and cancels all actor work exactly once.
func (s *Session) Close(reason string) {
	s.closeOnce.Do(func() {
		s.mu.Lock()
		s.isClosed = true
		s.closeErr = reason
		s.mu.Unlock()
		s.cancel()
		close(s.closeCh)
	})
}

func cloneUDPAddr(addr *net.UDPAddr) *net.UDPAddr {
	if addr == nil {
		return nil
	}
	copyAddr := *addr
	copyAddr.IP = append(net.IP(nil), addr.IP...)
	return &copyAddr
}
