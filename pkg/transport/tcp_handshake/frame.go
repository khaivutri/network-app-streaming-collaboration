package tcp_handshake

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// WriteTCPFrame writes a length-prefixed application packet without partial-write loss.
func WriteTCPFrame(conn io.Writer, pkt *protocol.Packet) error {
	raw, err := protocol.MarshalPacket(pkt)
	if err != nil {
		return err
	}
	if len(raw) < protocol.HeaderSize || len(raw) > protocol.HeaderSize+protocol.MaxUDPPayload {
		return protocol.ErrInvalidTCPFrame
	}
	frame := make([]byte, 4+len(raw))
	binary.BigEndian.PutUint32(frame[:4], uint32(len(raw)))
	copy(frame[4:], raw)
	for len(frame) > 0 {
		n, writeErr := conn.Write(frame)
		if writeErr != nil {
			return fmt.Errorf("write TCP frame: %w", writeErr)
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		frame = frame[n:]
	}
	return nil
}

// ReadTCPFrame reads and validates one length-prefixed application packet.
func ReadTCPFrame(conn io.Reader) (*protocol.Packet, error) {
	var lenBuf [4]byte
	if _, err := io.ReadFull(conn, lenBuf[:]); err != nil {
		return nil, fmt.Errorf("read TCP frame length: %w", err)
	}
	total := binary.BigEndian.Uint32(lenBuf[:])
	max := uint32(protocol.HeaderSize + protocol.MaxUDPPayload)
	if total < uint32(protocol.HeaderSize) {
		return nil, protocol.ErrInvalidTCPFrame
	}
	if total > max {
		return nil, protocol.ErrPayloadTooLarge
	}
	raw := make([]byte, int(total))
	if _, err := io.ReadFull(conn, raw); err != nil {
		return nil, fmt.Errorf("read TCP frame body: %w", err)
	}
	return protocol.UnmarshalPacket(raw)
}
