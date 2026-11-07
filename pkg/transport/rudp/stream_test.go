package rudp

import (
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestStream_InOrderPacketsDelivery(t *testing.T) {
	stream := NewStream(protocol.StreamBroadcastChat, 8)
	for seq := uint32(1); seq <= 3; seq++ {
		packets, _ := stream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: seq}})
		if len(packets) != 1 || packets[0].Hdr.SeqNum != seq {
			t.Fatalf("seq %d was not delivered", seq)
		}
	}
	ack, bitmap, _ := stream.ComputeAckState()
	if ack != 4 || bitmap != 0 {
		t.Fatalf("ack=(%d,%016b), want (4,0)", ack, bitmap)
	}
}

func TestStream_OutOfOrderAndSACKBitmapCalculation(t *testing.T) {
	stream := NewStream(protocol.StreamBroadcastChat, 8)
	_, _ = stream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 1}})
	_, _ = stream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 4}})
	ack, bitmap, _ := stream.ComputeAckState()
	if ack != 2 || bitmap != 2 {
		t.Fatalf("ack=(%d,%d), want (2,2)", ack, bitmap)
	}
	_, _ = stream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 2}})
	packets, _ := stream.ProcessIncomingPacket(&protocol.Packet{Hdr: protocol.Header{SeqNum: 3}})
	if len(packets) != 2 || packets[0].Hdr.SeqNum != 3 || packets[1].Hdr.SeqNum != 4 {
		t.Fatalf("reorder result=%v", packets)
	}
}

func TestStream_ProcessAckAndFastRetransmit(t *testing.T) {
	stream := NewStream(protocol.StreamBroadcastChat, 8)
	for seq := uint32(1); seq <= 4; seq++ {
		packet := &protocol.Packet{
			Hdr: protocol.Header{
				SeqNum: seq,
			},
		}
		stream.RecordSent(packet, []byte{byte(seq)})
	}
	acked, lost := stream.ProcessAck(1, 0b111, 8)
	if acked != 3 || len(lost) != 1 || lost[0].SeqNum != 1 {
		t.Fatalf("acked=%d lost=%v", acked, lost)
	}
	// Immediate duplicate SACK within cooldown should not spam another retransmit.
	_, duplicateLost := stream.ProcessAck(1, 0b111, 8)
	if len(duplicateLost) != 0 {
		t.Fatalf("expected cooldown to suppress duplicate fast retransmit, got %d", len(duplicateLost))
	}
}

func TestStream_GetExpiredSegmentsSortedAndResetsTimer(t *testing.T) {
	stream := NewStream(protocol.StreamFileMin, 8)
	for _, seq := range []uint32{3, 1, 2} {
		packet := &protocol.Packet{
			Hdr: protocol.Header{
				SeqNum: seq,
			},
		}
		stream.RecordSent(packet, []byte{byte(seq)})
	}
	expired := stream.GetExpiredSegments(-1)
	if len(expired) != 3 || expired[0].SeqNum != 1 || expired[1].SeqNum != 2 || expired[2].SeqNum != 3 {
		t.Fatalf("expected sorted expired segments [1,2,3], got %+v", expired)
	}
}
