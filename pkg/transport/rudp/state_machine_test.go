package rudp

import (
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport"
)

func TestClientStateMachineHappyPathAndTeardown(t *testing.T) {
	client := NewClientStateMachine()
	if client.State() != transport.StateClosed {
		t.Fatalf("initial state must be CLOSED, got %s", client.State())
	}
	action := client.Connect()
	if client.State() != transport.StateSynSent || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagSYN) {
		t.Fatalf("Connect must enter SYN_SENT and emit SYN")
	}
	const assignedID uint32 = 0x12345678
	synAck := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagSYN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: assignedID,
			AckNum:       1,
		},
	}
	action = client.OnPacket(synAck)
	if client.State() != transport.StateConnected || client.ConnectionID() != assignedID || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagACK) {
		t.Fatalf("SYN-ACK must connect client and emit ACK")
	}
	action = client.Disconnect()
	if client.State() != transport.StateFinWait || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagFIN) {
		t.Fatalf("Disconnect must enter FIN_WAIT and emit FIN")
	}
	finAck := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagFIN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: assignedID,
			SeqNum:       1,
			AckNum:       3,
		},
	}
	client.OnPacket(finAck)
	if client.State() != transport.StateCloseReady {
		t.Fatalf("FIN-ACK must enter CLOSE_READY, got %s", client.State())
	}
	client.OnCleanupTimeout()
	if client.State() != transport.StateClosed {
		t.Fatalf("cleanup must enter CLOSED, got %s", client.State())
	}
}

func TestClientStateMachineHandshakeTimeoutRetry(t *testing.T) {
	client := NewClientStateMachine()
	client.Connect()
	for i := 1; i <= MaxHandshakeRetries; i++ {
		action := client.OnHandshakeTimeout()
		if client.State() != transport.StateSynSent || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagSYN) {
			t.Fatalf("retry %d must retransmit SYN in SYN_SENT", i)
		}
	}
	action := client.OnHandshakeTimeout()
	if client.State() != transport.StateClosed || action.CloseReason != transport.CloseReasonHandshakeTimeout {
		t.Fatalf("retry exhaustion must close with handshake timeout")
	}
}

func TestServerStateMachineDuplicateSYN(t *testing.T) {
	server := NewServerStateMachine()
	const sessionID uint32 = 0xABCDEF01
	first := server.AcceptSYN(sessionID)
	if server.State() != transport.StateSynReceived || first.Packet.Hdr.ConnectionID != sessionID {
		t.Fatalf("first SYN must establish SYN_RECEIVED with assigned ID")
	}
	duplicate := server.AcceptSYN(0x99999999)
	if server.State() != transport.StateSynReceived || duplicate.Packet.Hdr.ConnectionID != sessionID {
		t.Fatalf("duplicate SYN must reuse the original ID")
	}
	ack := &protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: sessionID,
			SeqNum:       1,
			AckNum:       1,
		},
	}
	server.OnPacket(ack)
	if server.State() != transport.StateConnected {
		t.Fatalf("final ACK must enter CONNECTED, got %s", server.State())
	}
}

func TestServerStateMachineTimeoutHeartbeatAndFINWait(t *testing.T) {
	server := NewServerStateMachine()
	server.AcceptSYN(0x11112222)
	for i := 1; i <= MaxHandshakeRetries; i++ {
		action := server.OnHandshakeTimeout()
		if server.State() != transport.StateSynReceived || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagSYN) || !action.Packet.Hdr.HasFlag(protocol.FlagACK) {
			t.Fatalf("handshake retry %d must emit SYN-ACK in SYN_RECEIVED", i)
		}
	}
	action := server.OnHandshakeTimeout()
	if server.State() != transport.StateClosed || action.CloseReason != transport.CloseReasonHandshakeTimeout {
		t.Fatalf("handshake retry exhaustion must close with timeout")
	}

	heartbeatServer := NewServerStateMachine()
	heartbeatServer.AcceptSYN(0x33334444)
	heartbeatServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x33334444,
			SeqNum:       1,
			AckNum:       1,
		},
	})
	for i := 0; i < MaxMissedHeartbeats-1; i++ {
		if action := heartbeatServer.OnHeartbeatMissed(); action.CloseReason != transport.CloseReasonNormal {
			t.Fatalf("heartbeat miss %d must not close early", i+1)
		}
	}
	action = heartbeatServer.OnHeartbeatMissed()
	if heartbeatServer.State() != transport.StateClosed || action.CloseReason != transport.CloseReasonHeartbeatTimeout {
		t.Fatalf("heartbeat exhaustion must close with timeout")
	}

	finServer := NewServerStateMachine()
	finServer.AcceptSYN(0x55556666)
	finServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x55556666,
			SeqNum:       1,
			AckNum:       1,
		},
	})
	finAction := finServer.Disconnect()
	if finServer.State() != transport.StateFinWait || finAction.Packet == nil || !finAction.Packet.Hdr.HasFlag(protocol.FlagFIN) {
		t.Fatalf("server Disconnect must enter FIN_WAIT and emit FIN")
	}
	for i := 1; i <= MaxFINRetries; i++ {
		action = finServer.OnFINTimeout()
		if finServer.State() != transport.StateFinWait || action.Packet == nil || !action.Packet.Hdr.HasFlag(protocol.FlagFIN) {
			t.Fatalf("FIN retry %d must retransmit FIN", i)
		}
	}
	finServer.OnFINTimeout()
	if finServer.State() != transport.StateCloseReady {
		t.Fatalf("FIN retry exhaustion must force CLOSE_READY, got %s", finServer.State())
	}

	ackServer := NewServerStateMachine()
	ackServer.AcceptSYN(0x77778888)
	ackServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x77778888,
			SeqNum:       1,
			AckNum:       1,
		},
	})
	ackServer.Disconnect()
	ackServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x77778888,
			SeqNum:       2,
			AckNum:       2,
		},
	})
	if ackServer.State() != transport.StateCloseReady {
		t.Fatalf("valid FIN-ACK must move FIN_WAIT to CLOSE_READY")
	}

	finPacketServer := NewServerStateMachine()
	finPacketServer.AcceptSYN(0x9999AAAA)
	finPacketServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x9999AAAA,
			SeqNum:       1,
			AckNum:       1,
		},
	})
	finPacketServer.Disconnect()
	finAckAction := finPacketServer.OnPacket(&protocol.Packet{
		Hdr: protocol.Header{
			Version:      protocol.ProtocolVersion,
			Flags:        protocol.FlagFIN | protocol.FlagACK,
			MsgType:      protocol.MsgCtrlHandshake,
			ConnectionID: 0x9999AAAA,
			SeqNum:       2,
			AckNum:       2,
		},
	})
	if finAckAction.Packet == nil || !finAckAction.Packet.Hdr.HasFlag(protocol.FlagFIN) || finPacketServer.State() != transport.StateCloseReady {
		t.Fatalf("valid FIN in FIN_WAIT must emit FIN-ACK and enter CLOSE_READY")
	}
}
