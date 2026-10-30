package rudp

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/tcp_handshake"
)

// UDPConfig controls UDP handshake timing and packet filtering.
type UDPConfig struct {
	HandshakeTimeout time.Duration
	PacketDropHook   func(string, *protocol.Packet) bool
}

// UDPHandshakeServer serves application handshakes over UDP.
type UDPHandshakeServer struct {
	conn     *net.UDPConn
	addr     *net.UDPAddr
	dropHook func(string, *protocol.Packet) bool
	sessions map[string]uint32
}

// NewUDPHandshakeServer binds a UDP handshake server.
func NewUDPHandshakeServer(listenAddr string, cfg ...UDPConfig) (*UDPHandshakeServer, error) {
	addr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	s := &UDPHandshakeServer{conn: conn, addr: conn.LocalAddr().(*net.UDPAddr), sessions: make(map[string]uint32)}
	if len(cfg) > 0 {
		s.dropHook = cfg[0].PacketDropHook
	}
	return s, configureUDPSocket(conn)
}

// Addr returns the actual bound address.
func (s *UDPHandshakeServer) Addr() net.Addr { return s.addr }

// Close stops the server.
func (s *UDPHandshakeServer) Close() error { return s.conn.Close() }

// Serve handles SYN, ACK, and FIN packets until context cancellation.
func (s *UDPHandshakeServer) Serve(ctx context.Context) error {
	go func() { <-ctx.Done(); _ = s.conn.Close() }()
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	for {
		n, addr, err := s.conn.ReadFromUDP(buf)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		pkt, err := protocol.UnmarshalPacket(buf[:n])
		if err != nil {
			continue
		}
		key := addr.String()
		if pkt.Hdr.HasFlag(protocol.FlagSYN) {
			id, ok := s.sessions[key]
			if !ok {
				id, err = transport.GenerateConnectionID()
				if err != nil {
					continue
				}
				s.sessions[key] = id
			}
			response := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN | protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: id, AckNum: 1, WindowSize: protocol.DefaultWindowSize}}
			if s.dropHook != nil && s.dropHook("out", response) {
				continue
			}
			raw, _ := protocol.MarshalPacket(response)
			_, _ = s.conn.WriteToUDP(raw, addr)
			continue
		}
		id, ok := s.sessions[key]
		if !ok || id != pkt.Hdr.ConnectionID {
			if pkt.Hdr.HasFlag(protocol.FlagRST) {
				continue
			}
			reset := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagRST, MsgType: protocol.MsgCtrlHandshake, ConnectionID: pkt.Hdr.ConnectionID}}
			raw, _ := protocol.MarshalPacket(reset)
			_, _ = s.conn.WriteToUDP(raw, addr)
			continue
		}
		if pkt.Hdr.HasFlag(protocol.FlagFIN) {
			response := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagFIN | protocol.FlagACK, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, ConnectionID: id, SeqNum: 1, AckNum: pkt.Hdr.SeqNum + 1, WindowSize: protocol.DefaultWindowSize}}
			raw, _ := protocol.MarshalPacket(response)
			_, _ = s.conn.WriteToUDP(raw, addr)
			delete(s.sessions, key)
		}
	}
}

// PerformUDPHandshake executes a retransmitting client handshake and graceful close.
func PerformUDPHandshake(ctx context.Context, serverAddr string, cfg ...UDPConfig) (*tcp_handshake.HandshakeResult, error) {
	timeout := HandshakeTimeout
	var hook func(string, *protocol.Packet) bool
	if len(cfg) > 0 {
		if cfg[0].HandshakeTimeout > 0 {
			timeout = cfg[0].HandshakeTimeout
		}
		hook = cfg[0].PacketDropHook
	}
	addr, err := net.ResolveUDPAddr("udp", serverAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", nil)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err = configureUDPSocket(conn); err != nil {
		return nil, err
	}
	started := time.Now()
	syn := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN, MsgType: protocol.MsgCtrlHandshake, StreamID: protocol.StreamControl, WindowSize: protocol.DefaultWindowSize}}
	raw, _ := protocol.MarshalPacket(syn)
	var welcome *protocol.Packet
	var n int
	for attempt := 0; attempt <= MaxHandshakeRetries; attempt++ {
		if hook == nil || !hook("out", syn) {
			if _, err = conn.WriteToUDP(raw, addr); err != nil {
				return nil, err
			}
		}
		_ = conn.SetReadDeadline(time.Now().Add(timeout))
		buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
		n, _, readErr := conn.ReadFromUDP(buf)
		if readErr == nil {
			welcome, err = protocol.UnmarshalPacket(buf[:n])
			if err == nil && welcome.Hdr.HasFlag(protocol.FlagSYN) && welcome.Hdr.HasFlag(protocol.FlagACK) {
				break
			}
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	if welcome == nil || welcome.Hdr.ConnectionID == 0 {
		return nil, fmt.Errorf("UDP handshake timed out")
	}
	ack := controlPacket(protocol.FlagACK, welcome.Hdr.ConnectionID, 1, 1)
	ackRaw, _ := protocol.MarshalPacket(ack)
	if _, err = conn.WriteToUDP(ackRaw, addr); err != nil {
		return nil, err
	}
	fin := controlPacket(protocol.FlagFIN, welcome.Hdr.ConnectionID, 2, 1)
	finRaw, _ := protocol.MarshalPacket(fin)
	if _, err = conn.WriteToUDP(finRaw, addr); err != nil {
		return nil, err
	}
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	buf := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	n, _, err = conn.ReadFromUDP(buf)
	if err != nil {
		return nil, err
	}
	finAck, err := protocol.UnmarshalPacket(buf[:n])
	if err != nil || !finAck.Hdr.HasFlag(protocol.FlagFIN) {
		return nil, fmt.Errorf("invalid UDP FIN-ACK")
	}
	return &tcp_handshake.HandshakeResult{ConnectionID: welcome.Hdr.ConnectionID, ServerAddr: serverAddr, LocalAddr: conn.LocalAddr().String(), RTTMillis: time.Since(started).Milliseconds(), RTTMicros: time.Since(started).Microseconds(), WelcomeMsg: protocol.DefaultWelcomeMessage}, nil
}
