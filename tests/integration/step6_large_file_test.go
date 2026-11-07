package integration

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

const largeFileSize = 10093111

// TestStep6_LargeFileUDP_SACKRecovery exercises real UDP sockets while
// deterministically dropping selected datagrams on their first arrival.
func TestStep6_LargeFileUDP_SACKRecovery(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "large-source.bin")
	data := make([]byte, largeFileSize)
	for index := range data {
		data[index] = byte((index*31 + index/257) % 251)
	}
	if err := os.WriteFile(sourcePath, data, 0644); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(data)
	expectedSHA := hex.EncodeToString(hash[:])

	transferID := "123e4567-e89b-42d3-a456-426614174000"
	uploader, initPayload, err := transfer.NewUploader(transferID, sourcePath, protocol.MaxFileChunkDataSize)
	if err != nil {
		t.Fatal(err)
	}
	defer uploader.Close()
	downloader, err := transfer.NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = downloader.Cleanup() })

	senderConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer senderConn.Close()
	receiverConn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("127.0.0.1")})
	if err != nil {
		t.Fatal(err)
	}
	defer receiverConn.Close()
	_ = senderConn.SetWriteBuffer(4 * 1024 * 1024)
	_ = receiverConn.SetReadBuffer(4 * 1024 * 1024)

	largeTestWindow := uint16(65535)
	senderStream := rudp.NewStream(protocol.StreamFileMin, largeTestWindow)
	receiverStream := rudp.NewStream(protocol.StreamFileMin, largeTestWindow)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	dropped := make(map[uint32]bool)
	var droppedMu sync.Mutex
	var receivedPackets atomic.Int64
	var sentAcks atomic.Int64
	receivedDone := make(chan error, 1)
	go func() {
		receivedDone <- receiveLargeFile(ctx, receiverConn, senderConn.LocalAddr().(*net.UDPAddr), receiverStream, downloader, dropped, &droppedMu, initPayload.TotalChunks, &receivedPackets, &sentAcks)
	}()

	if err := sendLargeFileWindowed(ctx, senderConn, receiverConn.LocalAddr().(*net.UDPAddr), senderStream, uploader, initPayload); err != nil {
		if receivedPackets.Load() == 0 {
			t.Skipf("loopback UDP unavailable in this environment: %v", err)
		}
		select {
		case receiveErr := <-receivedDone:
			t.Fatalf("sender failed: %v; receiver failed: %v", err, receiveErr)
		default:
		}
		t.Fatalf("%v (received=%d acks=%d)", err, receivedPackets.Load(), sentAcks.Load())
	}
	if err := <-receivedDone; err != nil {
		t.Fatal(err)
	}

	droppedMu.Lock()
	droppedCount := len(dropped)
	droppedMu.Unlock()
	if droppedCount == 0 {
		t.Fatal("test did not drop any datagrams")
	}

	actualSHA, err := downloader.FinalizeAndVerify()
	if err != nil {
		t.Fatal(err)
	}
	if actualSHA != expectedSHA {
		t.Fatalf("SHA-256 mismatch: got %s, want %s", actualSHA, expectedSHA)
	}
	downloadedPath := filepath.Join(root, "downloads", transferID+"_large-source.bin")
	got, err := os.ReadFile(downloadedPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, data) {
		t.Fatal("downloaded bytes differ from source")
	}
}

func sendLargeFileWindowed(ctx context.Context, conn *net.UDPConn, destination *net.UDPAddr, stream *rudp.Stream, uploader *transfer.Uploader, initPayload *protocol.FileInitPayload) error {
	transferID, err := protocol.ParseUUID(initPayload.TransferID)
	if err != nil {
		return err
	}
	packets := make([][]byte, 0, initPayload.TotalChunks)
	for index := uint32(0); index < initPayload.TotalChunks; index++ {
		data, readErr := uploader.ReadChunk(index)
		if readErr != nil {
			return readErr
		}
		packet := &protocol.Packet{
			Hdr: protocol.Header{
				Version:    protocol.ProtocolVersion,
				Flags:      protocol.FlagREL | protocol.FlagORD | protocol.FlagACK,
				MsgType:    protocol.MsgFileChunk,
				StreamID:   stream.ID,
				SeqNum:     stream.NextSeqNum(),
				WindowSize: protocol.DefaultWindowSize,
			},
			Payload: mustMarshalChunk(transferID, index, data),
		}
		raw, marshalErr := protocol.MarshalPacket(packet)
		if marshalErr != nil {
			return marshalErr
		}
		packets = append(packets, raw)
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	const windowPackets = 16
	for start := 0; start < len(packets); start += windowPackets {
		end := start + windowPackets
		if end > len(packets) {
			end = len(packets)
		}
		for _, raw := range packets[start:end] {
			packet, parseErr := protocol.UnmarshalPacket(raw)
			if parseErr != nil {
				return parseErr
			}
			stream.RecordSent(packet, raw)
			if _, writeErr := conn.WriteToUDP(raw, destination); writeErr != nil {
				return writeErr
			}
		}
		deadline := time.Now().Add(2 * time.Second)
		for stream.InFlightCount() > 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
			if time.Now().After(deadline) {
				return fmt.Errorf("window recovery timeout with %d in-flight segments", stream.InFlightCount())
			}
			_ = conn.SetReadDeadline(time.Now().Add(25 * time.Millisecond))
			n, _, readErr := conn.ReadFromUDP(buffer)
			if readErr != nil {
				if netErr, ok := readErr.(net.Error); ok && netErr.Timeout() {
					for _, segment := range stream.GetExpiredSegments(150 * time.Millisecond) {
						if _, writeErr := conn.WriteToUDP(segment.Raw, destination); writeErr != nil {
							return writeErr
						}
					}
					continue
				}
				return readErr
			}
			ack, parseErr := protocol.UnmarshalPacket(buffer[:n])
			if parseErr != nil || ack.Hdr.MsgType != protocol.MsgAck {
				continue
			}
			_, fastRetransmit := stream.ProcessAck(ack.Hdr.AckNum, ack.Hdr.AckBitmap, ack.Hdr.WindowSize)
			for _, segment := range fastRetransmit {
				if _, writeErr := conn.WriteToUDP(segment.Raw, destination); writeErr != nil {
					return writeErr
				}
			}
		}
	}
	return nil
}

func sendInitialLargeFile(conn *net.UDPConn, destination *net.UDPAddr, stream *rudp.Stream, uploader *transfer.Uploader, initPayload *protocol.FileInitPayload) error {
	transferID, err := protocol.ParseUUID(initPayload.TransferID)
	if err != nil {
		return err
	}
	for index := uint32(0); index < initPayload.TotalChunks; index++ {
		data, readErr := uploader.ReadChunk(index)
		if readErr != nil {
			return readErr
		}
		packet := &protocol.Packet{
			Hdr: protocol.Header{
				Version:    protocol.ProtocolVersion,
				Flags:      protocol.FlagREL | protocol.FlagORD | protocol.FlagACK,
				MsgType:    protocol.MsgFileChunk,
				StreamID:   stream.ID,
				SeqNum:     stream.NextSeqNum(),
				WindowSize: protocol.DefaultWindowSize,
			},
			Payload: mustMarshalChunk(transferID, index, data),
		}
		raw, marshalErr := protocol.MarshalPacket(packet)
		if marshalErr != nil {
			return marshalErr
		}
		stream.RecordSent(packet, raw)
		if _, writeErr := conn.WriteToUDP(raw, destination); writeErr != nil {
			return writeErr
		}
		time.Sleep(100 * time.Microsecond)
	}
	return nil
}

func receiveLargeFile(ctx context.Context, conn *net.UDPConn, sender *net.UDPAddr, stream *rudp.Stream, downloader *transfer.Downloader, dropped map[uint32]bool, droppedMu *sync.Mutex, totalChunks uint32, receivedPackets, sentAcks *atomic.Int64) error {
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	packetCount := 0
	for {
		if err := conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			return err
		}
		n, remote, err := conn.ReadFromUDP(buffer)
		if err != nil {
			if ctx.Err() != nil {
				return fmt.Errorf("receive timeout after %d packets: %w", packetCount, ctx.Err())
			}
			continue
		}
		packet, err := protocol.UnmarshalPacket(buffer[:n])
		if err != nil || packet.Hdr.MsgType != protocol.MsgFileChunk {
			continue
		}
		packetCount++
		receivedPackets.Add(1)
		if packet.Hdr.SeqNum%17 == 0 {
			droppedMu.Lock()
			firstArrival := !dropped[packet.Hdr.SeqNum]
			if firstArrival {
				dropped[packet.Hdr.SeqNum] = true
			}
			droppedMu.Unlock()
			if firstArrival {
				continue
			}
		}
		packets, _ := stream.ProcessIncomingPacket(packet)
		ackNum, ackBitmap, window := stream.ComputeAckState()
		ack := &protocol.Packet{
			Hdr: protocol.Header{
				Version:    protocol.ProtocolVersion,
				Flags:      protocol.FlagACK,
				MsgType:    protocol.MsgAck,
				StreamID:   stream.ID,
				AckNum:     ackNum,
				AckBitmap:  ackBitmap,
				WindowSize: window,
			},
		}
		rawAck, marshalErr := protocol.MarshalPacket(ack)
		if marshalErr != nil {
			return marshalErr
		}
		if _, writeErr := conn.WriteToUDP(rawAck, remote); writeErr != nil {
			return writeErr
		}
		sentAcks.Add(1)
		for _, delivered := range packets {
			chunk, chunkErr := protocol.UnmarshalFileChunk(delivered.Payload)
			if chunkErr != nil {
				return chunkErr
			}
			complete, writeErr := downloader.WriteChunk(chunk.ChunkIndex, chunk.Data)
			if writeErr != nil {
				return writeErr
			}
			if complete && uint32(len(packets)) > 0 && downloader.Progress() == 100 {
				return nil
			}
		}
		if streamExpected(stream) > totalChunks {
			return nil
		}
	}
}

func recoverLargeFile(ctx context.Context, conn *net.UDPConn, destination *net.UDPAddr, stream *rudp.Stream) error {
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("sender recovery timeout with %d in-flight segments: %w", stream.InFlightCount(), ctx.Err())
		case <-ticker.C:
			for _, segment := range stream.GetExpiredSegments(150 * time.Millisecond) {
				if _, err := conn.WriteToUDP(segment.Raw, destination); err != nil {
					return err
				}
			}
		default:
			_ = conn.SetReadDeadline(time.Now().Add(5 * time.Millisecond))
			n, _, err := conn.ReadFromUDP(buffer)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return err
			}
			packet, err := protocol.UnmarshalPacket(buffer[:n])
			if err != nil || packet.Hdr.MsgType != protocol.MsgAck {
				continue
			}
			_, fastRetransmit := stream.ProcessAck(packet.Hdr.AckNum, packet.Hdr.AckBitmap, packet.Hdr.WindowSize)
			for _, segment := range fastRetransmit {
				if _, err := conn.WriteToUDP(segment.Raw, destination); err != nil {
					return err
				}
			}
			if stream.InFlightCount() == 0 {
				return nil
			}
		}
	}
}

func mustMarshalChunk(id [16]byte, index uint32, data []byte) []byte {
	payload, err := protocol.MarshalFileChunk(&protocol.FileChunkPacket{TransferID: id, ChunkIndex: index, Data: data})
	if err != nil {
		panic(err)
	}
	return payload
}

func streamExpected(stream *rudp.Stream) uint32 {
	ack, _, _ := stream.ComputeAckState()
	return ack
}
