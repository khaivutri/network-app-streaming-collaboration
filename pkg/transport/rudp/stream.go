package rudp

import (
	"sort"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

const fastRetransmitCooldown = 25 * time.Millisecond

// SentSegment is a reliable packet retained until cumulative or selective ACK.
type SentSegment struct {
	SeqNum   uint32
	Packet   *protocol.Packet
	Raw      []byte
	SentTime time.Time
	Retries  int
	Acked    bool
}

// Stream owns ordering and reliability state for one logical stream.
type Stream struct {
	ID              uint8
	mu              sync.RWMutex
	nextSendSeq     uint32
	expectedRecvSeq uint32
	reorderBuf      map[uint32]*protocol.Packet
	sendBuffer      map[uint32]*SentSegment
	windowSize      uint16
	peerWindowSize  uint16
}

// NewStream creates an independent reliable stream.
func NewStream(streamID uint8, windowSize uint16) *Stream {
	if windowSize == 0 {
		windowSize = protocol.DefaultWindowSize
	}
	return &Stream{
		ID:              streamID,
		nextSendSeq:     1,
		expectedRecvSeq: 1,
		reorderBuf:      make(map[uint32]*protocol.Packet),
		sendBuffer:      make(map[uint32]*SentSegment),
		windowSize:      windowSize,
		peerWindowSize:  protocol.DefaultWindowSize,
	}
}

// NextSeqNum reserves the next sequence number atomically.
func (s *Stream) NextSeqNum() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := s.nextSendSeq
	s.nextSendSeq++
	return seq
}

// RecordSent retains a reliable packet for retransmission.
func (s *Stream) RecordSent(pkt *protocol.Packet, raw []byte) {
	if pkt == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sendBuffer[pkt.Hdr.SeqNum] = &SentSegment{
		SeqNum:   pkt.Hdr.SeqNum,
		Packet:   pkt,
		Raw:      append([]byte(nil), raw...),
		SentTime: time.Now(),
	}
}

// InFlightCount returns the number of reliable segments awaiting ACKs.
func (s *Stream) InFlightCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.sendBuffer)
}

// PeerWindowSize returns the latest receive-window advertisement from the peer.
func (s *Stream) PeerWindowSize() uint16 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.peerWindowSize == 0 {
		return protocol.DefaultWindowSize
	}
	return s.peerWindowSize
}

// ComputeAckState returns cumulative ACK, 16-bit SACK bitmap, and receive window.
func (s *Stream) ComputeAckState() (uint32, uint16, uint16) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ack := s.expectedRecvSeq
	var bitmap uint16
	for k := uint32(0); k < 16; k++ {
		if _, ok := s.reorderBuf[ack+1+k]; ok {
			bitmap |= 1 << k
		}
	}
	return ack, bitmap, s.windowSize
}

// ProcessIncomingPacket orders a packet and returns newly contiguous packets.
func (s *Stream) ProcessIncomingPacket(pkt *protocol.Packet) ([]*protocol.Packet, bool) {
	if pkt == nil {
		return nil, false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	seq := pkt.Hdr.SeqNum
	if seq < s.expectedRecvSeq {
		return nil, true
	}
	if seq == s.expectedRecvSeq {
		out := []*protocol.Packet{pkt}
		s.expectedRecvSeq++
		for {
			next, ok := s.reorderBuf[s.expectedRecvSeq]
			if !ok {
				break
			}
			delete(s.reorderBuf, s.expectedRecvSeq)
			out = append(out, next)
			s.expectedRecvSeq++
		}
		return out, len(s.reorderBuf) > 0
	}
	if seq < s.expectedRecvSeq+uint32(s.windowSize) {
		s.reorderBuf[seq] = pkt
	}
	return nil, true
}

// ProcessAck applies cumulative and selective acknowledgements and reports fast-retransmit candidates.
func (s *Stream) ProcessAck(ackNum uint32, ackBitmap uint16, peerWin uint16) (int, []*SentSegment) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.peerWindowSize = peerWin
	acked := 0
	for seq, segment := range s.sendBuffer {
		if seq < ackNum {
			segment.Acked = true
			delete(s.sendBuffer, seq)
			acked++
		}
	}
	for k := uint32(0); k < 16; k++ {
		if ackBitmap&(1<<k) == 0 {
			continue
		}
		seq := ackNum + 1 + k
		if segment, ok := s.sendBuffer[seq]; ok {
			segment.Acked = true
			delete(s.sendBuffer, seq)
			acked++
		}
	}
	if bits16(ackBitmap) < 3 {
		return acked, nil
	}
	if segment, ok := s.sendBuffer[ackNum]; ok && !segment.Acked {
		now := time.Now()
		if segment.Retries == 0 || now.Sub(segment.SentTime) >= fastRetransmitCooldown {
			segment.Retries++
			segment.SentTime = now
			return acked, []*SentSegment{segment}
		}
	}
	return acked, nil
}

// GetExpiredSegments returns unacknowledged segments older than rto sorted by
// ascending sequence number, resetting each returned segment's retransmit timer.
func (s *Stream) GetExpiredSegments(rto time.Duration) []*SentSegment {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	out := make([]*SentSegment, 0)
	for _, segment := range s.sendBuffer {
		if !segment.Acked && now.Sub(segment.SentTime) > rto {
			segment.Retries++
			segment.SentTime = now
			out = append(out, segment)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].SeqNum < out[j].SeqNum
	})
	return out
}

func bits16(value uint16) int {
	count := 0
	for value != 0 {
		value &= value - 1
		count++
	}
	return count
}
