package integration

import (
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

func TestStep5_BroadcastChat_EndToEnd(t *testing.T) {
	payload := protocol.BroadcastMessageServerPayload{Sender: "Alice", Content: "Xin chào cả phòng"}
	raw, err := protocol.EncodeJSONPayload(payload)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.DecodeJSONPayload[protocol.BroadcastMessageServerPayload](raw)
	if err != nil || decoded.Sender != "Alice" || decoded.Content != payload.Content {
		t.Fatalf("broadcast payload mismatch: %+v", decoded)
	}
}

func TestStep5_DirectChat_Isolation_BR07(t *testing.T) {
	packet := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, StreamID: protocol.StreamDirectChat, ConnectionID: 42}}
	raw, err := protocol.MarshalPacket(packet)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := protocol.UnmarshalPacket(raw)
	if err != nil || decoded.Hdr.ConnectionID != 42 {
		t.Fatalf("direct packet target was not preserved")
	}
}

func TestStep5_DirectChat_ValidationErrors(t *testing.T) {
	if protocol.ErrCodeMsgEmptyContent == protocol.ErrCodeMsgSelfRecipient || protocol.ErrCodeMsgSelfRecipient == protocol.ErrCodeMsgRecipientOffline {
		t.Fatal("validation error codes must be distinct")
	}
}

func TestStep5_ZeroHeadOfLineBlocking_MultiStream(t *testing.T) {
	fileStream := rudp.NewStream(protocol.StreamFileMin, 4)
	chatStream := rudp.NewStream(protocol.StreamBroadcastChat, 4)
	_, _ = fileStream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 2}})
	packets, _ := chatStream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 1}})
	if len(packets) != 1 {
		t.Fatal("chat stream was blocked by file stream gap")
	}
}

func TestStep5_SACK16Bit_PacketLossRecovery(t *testing.T) {
	stream := rudp.NewStream(protocol.StreamBroadcastChat, 8)
	for seq := uint32(1); seq <= 5; seq++ {
		packet := &protocol.Packet{Hdr: protocol.Header{SeqNum: seq}}
		stream.RecordSent(packet, []byte{byte(seq)})
	}
	acked, lost := stream.ProcessAck(1, 0b1110, 8)
	if acked != 3 || len(lost) != 1 || lost[0].SeqNum != 1 {
		t.Fatalf("SACK acked=%d lost=%v", acked, lost)
	}
}
