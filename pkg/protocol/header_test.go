package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestHeaderRoundTrip(t *testing.T) {
	h := Header{ProtocolVersion, FlagSYN | FlagACK | FlagREL | FlagENC, MsgMessUnicast, 2, 0x8F4A12C1, 0x01020304, 0x05060708, 0xBEEF, 0x0040}
	raw := MarshalHeader(h)
	if len(raw) != HeaderSize || !bytes.Equal(raw[:4], []byte{1, 0x93, 0x31, 2}) || !bytes.Equal(raw[4:8], []byte{0x8F, 0x4A, 0x12, 0xC1}) {
		t.Fatalf("unexpected header: %x", raw)
	}
	got, err := UnmarshalHeader(raw[:])
	if err != nil || got != h {
		t.Fatalf("round trip: %#v %v", got, err)
	}
}
func TestFlags(t *testing.T) {
	for _, f := range []uint8{FlagSYN, FlagACK, FlagFIN, FlagRST, FlagREL, FlagORD, FlagFRAG, FlagENC} {
		if !(Header{Flags: f}).HasFlag(f) {
			t.Errorf("flag %x", f)
		}
	}
}
func TestHeaderErrors(t *testing.T) {
	if _, err := UnmarshalHeader(nil); !errors.Is(err, ErrBufferTooShort) {
		t.Fatal(err)
	}
	raw := make([]byte, HeaderSize)
	raw[0] = 2
	if _, err := UnmarshalHeader(raw); !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatal(err)
	}
}
func TestPacketBoundsAndCopy(t *testing.T) {
	p := &Packet{Hdr: Header{Version: ProtocolVersion}, Payload: make([]byte, MaxUDPPayload)}
	raw, err := MarshalPacket(p)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := UnmarshalPacket(raw)
	if err != nil {
		t.Fatal(err)
	}
	raw[HeaderSize] = 9
	if decoded.Payload[0] == 9 {
		t.Fatal("payload was not copied")
	}
	p.Payload = make([]byte, MaxUDPPayload+1)
	if !errors.Is(mustMarshal(p), ErrPayloadTooLarge) {
		t.Fatal("oversize accepted")
	}
}
func mustMarshal(p *Packet) error { _, err := MarshalPacket(p); return err }
