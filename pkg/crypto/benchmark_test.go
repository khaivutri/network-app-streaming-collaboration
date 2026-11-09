package crypto

import (
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func BenchmarkX25519DeriveSessionKeys(b *testing.B) {
	client, _ := GenerateX25519KeyPair()
	server, _ := GenerateX25519KeyPair()
	var clientNonce, serverNonce [16]byte
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, err := DeriveSessionKeys(client.PrivateKey, server.PublicKey, clientNonce, serverNonce, uint32(i), false); err != nil { b.Fatal(err) }
	}
}

func BenchmarkAESGCMEncryptPacket1KB(b *testing.B) {
	var key [32]byte
	cipher, _ := NewSessionCipher(key, key, false)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgMessUnicast}
	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := cipher.EncryptPacket(hdr, payload); err != nil { b.Fatal(err) }
	}
}

func BenchmarkAESGCMDecryptPacket1KB(b *testing.B) {
	var key [32]byte
	sender, _ := NewSessionCipher(key, key, false)
	receiver, _ := NewSessionCipher(key, key, true)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgMessUnicast}
	payload := make([]byte, 1024)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw, err := sender.EncryptPacket(hdr, payload)
		if err != nil { b.Fatal(err) }
		if _, err = receiver.DecryptRawPacket(raw); err != nil { b.Fatal(err) }
	}
}
