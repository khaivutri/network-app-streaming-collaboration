package handlers

import (
	"net"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/server/state"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// TestHandleFileChunkDeterministicSeqOnRetransmit verifies that relayed file chunks
// always receive SeqNum = ChunkIndex + 1 so retransmissions fill receiver gaps
// instead of advancing to a new out-of-window sequence number.
func TestHandleFileChunkDeterministicSeqOnRetransmit(t *testing.T) {
	state.ResetGlobalState()
	defer state.ResetGlobalState()

	transferID := "b3e0db4e-8f3a-4001-98d7-80195619d9d3"
	idBytes, err := protocol.ParseUUID(transferID)
	if err != nil {
		t.Fatalf("parse transfer id: %v", err)
	}

	senderQueue := make(chan []byte, 16)
	receiverQueue := make(chan []byte, 16)
	peerAddr := &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 9999,
	}

	if _, _, err := state.RegisterAuthenticatedUser("hung.vo", "Hung Vo", 101, peerAddr, [32]byte{}, senderQueue); err != nil {
		t.Fatalf("register sender: %v", err)
	}
	if _, _, err := state.RegisterAuthenticatedUser("Bob", "Bob", 202, peerAddr, [32]byte{}, receiverQueue); err != nil {
		t.Fatalf("register receiver: %v", err)
	}

	state.RegisterActiveTransfer(&state.ActiveTransfer{
		TransferID:       transferID,
		TransferIDBytes:  idBytes,
		SenderUsername:   "hung.vo",
		ReceiverUsername: "Bob",
		SenderConnID:     101,
		ReceiverConnID:   202,
		SenderStreamID:   protocol.StreamFileMin,
		ReceiverStreamID: protocol.StreamFileMin,
		FileName:         "test.pdf",
		FileSize:         3600,
		TotalChunks:      3,
		ChunkSize:        1200,
		StartedAt:        time.Now(),
	})

	handler := NewFileRelayHandler()

	// Relay chunk 0, chunk 1, chunk 2, then retransmit chunk 0.
	indices := []uint32{0, 1, 2, 0}
	expectedSeqs := []uint32{1, 2, 3, 1}

	for step, chunkIdx := range indices {
		rawChunk, marshalErr := protocol.MarshalFileChunk(&protocol.FileChunkPacket{
			TransferID: idBytes,
			ChunkIndex: chunkIdx,
			Data:       []byte("payload-bytes"),
		})
		if marshalErr != nil {
			t.Fatalf("step %d marshal chunk: %v", step, marshalErr)
		}
		if relayErr := handler.HandleFileChunk(nil, protocol.StreamFileMin, rawChunk); relayErr != nil {
			t.Fatalf("step %d handle chunk: %v", step, relayErr)
		}
		select {
		case forwarded := <-receiverQueue:
			pkt, unmarshalErr := protocol.UnmarshalPacket(forwarded)
			if unmarshalErr != nil {
				t.Fatalf("step %d unmarshal packet: %v", step, unmarshalErr)
			}
			if pkt.Hdr.SeqNum != expectedSeqs[step] {
				t.Fatalf("step %d (chunk=%d): got SeqNum=%d, want %d", step, chunkIdx, pkt.Hdr.SeqNum, expectedSeqs[step])
			}
		case <-time.After(time.Second):
			t.Fatalf("step %d timed out waiting for relayed chunk", step)
		}
	}
}
