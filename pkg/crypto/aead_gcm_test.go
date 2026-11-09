package crypto

import (
	"encoding/binary"
	"errors"
	"sync"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestSessionCipherRoundTripAADAndReplay(t *testing.T) {
	var c2s, s2c [32]byte
	for i := range c2s {
		c2s[i] = byte(i + 1)
		s2c[i] = byte(i + 33)
	}
	client, err := NewSessionCipher(c2s, s2c, false)
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewSessionCipher(s2c, c2s, true)
	if err != nil {
		t.Fatal(err)
	}
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgPing, StreamID: protocol.StreamControl, ConnectionID: 9, SeqNum: 1}
	raw, err := client.EncryptPacket(hdr, []byte("secret payload"))
	if err != nil {
		t.Fatal(err)
	}
	pkt, err := server.DecryptRawPacket(raw)
	if err != nil || string(pkt.Payload) != "secret payload" {
		t.Fatalf("roundtrip failed: %v", err)
	}
	if _, err = server.DecryptRawPacket(raw); !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("replay error=%v", err)
	}
}

func TestSessionCipherRejectsTamperedHeaderAndCiphertext(t *testing.T) {
	var key [32]byte
	for i := range key {
		key[i] = byte(i + 1)
	}
	client, _ := NewSessionCipher(key, key, false)
	server, _ := NewSessionCipher(key, key, true)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagREL, MsgType: protocol.MsgPing, StreamID: protocol.StreamControl, ConnectionID: 9}
	raw, _ := client.EncryptPacket(hdr, []byte("secret"))
	raw[8]++
	changed, err := protocol.UnmarshalHeader(raw)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = server.DecryptPacket(changed, raw[:protocol.HeaderSize], raw[protocol.HeaderSize:]); !errors.Is(err, ErrAuthTagMismatch) {
		t.Fatalf("header tamper error=%v", err)
	}
}

func TestSessionCipherBoundaries(t *testing.T) {
	var key [32]byte
	client, _ := NewSessionCipher(key, key, false)
	server, _ := NewSessionCipher(key, key, true)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgPing}
	raw, err := client.EncryptPacket(hdr, nil)
	if err != nil || len(raw) != protocol.HeaderSize+Overhead {
		t.Fatalf("empty payload raw length=%d err=%v", len(raw), err)
	}
	pkt, err := server.DecryptRawPacket(raw)
	if err != nil || len(pkt.Payload) != 0 {
		t.Fatalf("empty payload decrypt failed: %v", err)
	}
	for _, size := range []int{0, 11, 12, 27} {
		if _, err := server.DecryptPacket(protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagENC}, make([]byte, protocol.HeaderSize), make([]byte, size)); !errors.Is(err, ErrInvalidPayloadLength) {
			t.Fatalf("size %d error=%v", size, err)
		}
	}
	if _, err := client.EncryptPacket(hdr, make([]byte, protocol.MaxUDPPayload-Overhead+1)); !errors.Is(err, protocol.ErrPayloadTooLarge) {
		t.Fatalf("oversize error=%v", err)
	}
}

func TestSessionCipherConcurrentNonceUniqueness(t *testing.T) {
	var key [32]byte
	client, _ := NewSessionCipher(key, key, false)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, MsgType: protocol.MsgPing}
	const total = 500
	nonces := make(chan string, total)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < total/10; j++ {
				raw, err := client.EncryptPacket(hdr, []byte("x"))
				if err != nil {
					t.Errorf("encrypt: %v", err)
					return
				}
				nonces <- string(raw[protocol.HeaderSize : protocol.HeaderSize+NonceSize])
			}
		}()
	}
	wg.Wait()
	close(nonces)
	seen := make(map[string]bool, total)
	for nonce := range nonces {
		if seen[nonce] {
			t.Fatal("nonce reused")
		}
		seen[nonce] = true
	}
	if len(seen) != total || client.sendCounter.Load() != total {
		t.Fatalf("nonces=%d counter=%d", len(seen), client.sendCounter.Load())
	}
}

func TestSessionCipherSlidingWindow(t *testing.T) {
	var key [32]byte
	sender, _ := NewSessionCipher(key, key, false)
	receiver, _ := NewSessionCipher(key, key, true)
	hdr := protocol.Header{Version: protocol.ProtocolVersion, Flags: protocol.FlagENC, MsgType: protocol.MsgPing}
	makeRaw := func(counter uint64) []byte {
		aad := protocol.MarshalHeader(hdr)
		nonce := make([]byte, NonceSize)
		copy(nonce, sender.sendPrefix[:])
		binary.BigEndian.PutUint64(nonce[4:], counter)
		out := append(append([]byte{}, aad[:]...), nonce...)
		return sender.sendAEAD.Seal(out, nonce, []byte("x"), aad[:])
	}
	if _, err := receiver.DecryptRawPacket(makeRaw(20)); err != nil {
		t.Fatal(err)
	}
	for _, counter := range []uint64{15, 18, 12, 19, 14} {
		if _, err := receiver.DecryptRawPacket(makeRaw(counter)); err != nil {
			t.Fatalf("counter %d: %v", counter, err)
		}
	}
	if _, err := receiver.DecryptRawPacket(makeRaw(15)); !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("duplicate error=%v", err)
	}
	if _, err := receiver.DecryptRawPacket(makeRaw(500)); err != nil {
		t.Fatal(err)
	}
	if _, err := receiver.DecryptRawPacket(makeRaw(5)); !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("stale error=%v", err)
	}
}
