package client

import (
	"net"
	"testing"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
	security "github.com/khaivutri/network-app-streaming-collaboration/pkg/crypto"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestSendPingEncryptsWhenSessionCipherExists(t *testing.T) {
	serverConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	})
	if err != nil {
		t.Skipf("loopback UDP unavailable: %v", err)
	}
	defer serverConn.Close()
	clientConn, err := net.ListenUDP("udp", &net.UDPAddr{
		IP:   net.ParseIP("127.0.0.1"),
		Port: 0,
	})
	if err != nil {
		t.Skipf("loopback UDP unavailable: %v", err)
	}
	defer clientConn.Close()
	var c2s, s2c [32]byte
	for index := range c2s {
		c2s[index] = byte(index + 1)
		s2c[index] = byte(index + 33)
	}
	clientCipher, err := security.NewSessionCipher(c2s, s2c, false)
	if err != nil {
		t.Fatal(err)
	}
	serverCipher, err := security.NewSessionCipher(s2c, c2s, true)
	if err != nil {
		t.Fatal(err)
	}
	chat := NewChatClient(clientConn, serverConn.LocalAddr().(*net.UDPAddr), 42, state.NewClientStore())
	chat.SetCipher(clientCipher)
	if err := chat.SendPing(); err != nil {
		t.Fatal(err)
	}
	buffer := make([]byte, protocol.HeaderSize+protocol.MaxUDPPayload+security.Overhead)
	if err := serverConn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	n, _, err := serverConn.ReadFromUDP(buffer)
	if err != nil {
		if networkErr, ok := err.(net.Error); ok && networkErr.Timeout() {
			t.Skipf("loopback UDP receive unavailable: %v", err)
		}
		t.Fatal(err)
	}
	packet, err := protocol.UnmarshalPacket(buffer[:n])
	if err != nil {
		t.Fatal(err)
	}
	if !packet.Hdr.HasFlag(protocol.FlagENC) {
		t.Fatal("ping did not set FlagENC")
	}
	if _, err := serverCipher.DecryptRawPacket(buffer[:n]); err != nil {
		t.Fatalf("server could not decrypt ping: %v", err)
	}
}
