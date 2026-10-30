package tcp_handshake

import (
	"bytes"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	p := &protocol.Packet{Hdr: protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagSYN}, Payload: []byte("hello")}
	var b bytes.Buffer
	if err := WriteTCPFrame(&b, p); err != nil {
		t.Fatal(err)
	}
	got, err := ReadTCPFrame(&b)
	if err != nil || !bytes.Equal(got.Payload, p.Payload) {
		t.Fatalf("%v %#v", err, got)
	}
}
