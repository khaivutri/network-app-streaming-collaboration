package protocol

import "encoding/binary"

// Header is the fixed 20-byte network-order application header.
type Header struct {
	Version      uint8
	Flags        uint8
	MsgType      uint8
	StreamID     uint8
	ConnectionID uint32
	SeqNum       uint32
	AckNum       uint32
	AckBitmap    uint16
	WindowSize   uint16
}

// Packet combines a header with a bounded application payload.
type Packet struct {
	Hdr     Header
	Payload []byte
}

// MarshalHeader encodes a Header using BigEndian offsets defined by the wire protocol.
func MarshalHeader(h Header) [HeaderSize]byte {
	var out [HeaderSize]byte
	out[0], out[1], out[2], out[3] = h.Version, h.Flags, h.MsgType, h.StreamID
	binary.BigEndian.PutUint32(out[4:8], h.ConnectionID)
	binary.BigEndian.PutUint32(out[8:12], h.SeqNum)
	binary.BigEndian.PutUint32(out[12:16], h.AckNum)
	binary.BigEndian.PutUint16(out[16:18], h.AckBitmap)
	binary.BigEndian.PutUint16(out[18:20], h.WindowSize)
	return out
}

// UnmarshalHeader validates and decodes the first 20 bytes of a wire buffer.
func UnmarshalHeader(buf []byte) (Header, error) {
	if len(buf) < HeaderSize {
		return Header{}, ErrBufferTooShort
	}
	if buf[0] != ProtocolVersion {
		return Header{}, ErrUnsupportedVersion
	}
	return Header{
		Version: buf[0], 
		Flags: buf[1], 
		MsgType: buf[2], 
		StreamID: buf[3], 
		ConnectionID: binary.BigEndian.Uint32(buf[4:8]), 
		SeqNum: binary.BigEndian.Uint32(buf[8:12]), 
		AckNum: binary.BigEndian.Uint32(buf[12:16]), 
		AckBitmap: binary.BigEndian.Uint16(buf[16:18]), 
		WindowSize: binary.BigEndian.Uint16(buf[18:20]),
	}, nil
}

// HasFlag reports whether a control bit is set.
func (h Header) HasFlag(flag uint8) bool { return h.Flags&flag != 0 }

// MarshalPacket validates and encodes a complete packet.
func MarshalPacket(p *Packet) ([]byte, error) {
	if p == nil {
		return nil, ErrNilPacket
	}
	if p.Hdr.Version != ProtocolVersion {
		return nil, ErrUnsupportedVersion
	}
	if len(p.Payload) > MaxUDPPayload {
		return nil, ErrPayloadTooLarge
	}
	result := make([]byte, HeaderSize+len(p.Payload))
	header := MarshalHeader(p.Hdr)
	copy(result, header[:])
	copy(result[HeaderSize:], p.Payload)
	return result, nil
}

// UnmarshalPacket decodes a complete packet and defensively copies its payload.
func UnmarshalPacket(raw []byte) (*Packet, error) {
	h, err := UnmarshalHeader(raw)
	if err != nil {
		return nil, err
	}
	if len(raw)-HeaderSize > MaxUDPPayload {
		return nil, ErrPayloadTooLarge
	}
	p := &Packet{Hdr: h}
	if len(raw) > HeaderSize {
		p.Payload = append([]byte(nil), raw[HeaderSize:]...)
	}
	return p, nil
}
