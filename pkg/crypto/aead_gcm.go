package crypto

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

const (
	NonceSize  = 12
	GCMTagSize = 16
	Overhead   = NonceSize + GCMTagSize
)

var (
	PrefixClientToServer = [4]byte{'C', '2', 'S', 0x00}
	PrefixServerToClient = [4]byte{'S', '2', 'C', 0x00}
)

var (
	ErrAuthTagMismatch      = errors.New("crypto: AES-GCM authentication failed or header tampered")
	ErrReplayDetected       = errors.New("crypto: duplicate or stale packet nonce rejected")
	ErrMissingEncFlag       = errors.New("crypto: expected FlagENC (0x80) on encrypted session")
	ErrInvalidPayloadLength = errors.New("crypto: encrypted payload shorter than overhead (28 bytes)")
	ErrInvalidDirection     = errors.New("crypto: unexpected nonce direction prefix")
)

// SessionCipher provides thread-safe directional AES-256-GCM encryption.
type SessionCipher struct {
	sendAEAD       cipher.AEAD
	recvAEAD       cipher.AEAD
	sendPrefix     [4]byte
	recvPrefix     [4]byte
	sendCounter    atomic.Uint64
	replayMu       sync.Mutex
	maxSeenCounter uint64
	replayWindow   uint64
}

// NewSessionCipher creates a cipher with separate keys and nonce directions.
func NewSessionCipher(writeKey, readKey [32]byte, isServer bool) (*SessionCipher, error) {
	sendBlock, err := aes.NewCipher(writeKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: create send AES cipher: %w", err)
	}
	sendAEAD, err := cipher.NewGCM(sendBlock)
	if err != nil {
		return nil, fmt.Errorf("crypto: create send GCM: %w", err)
	}
	recvBlock, err := aes.NewCipher(readKey[:])
	if err != nil {
		return nil, fmt.Errorf("crypto: create receive AES cipher: %w", err)
	}
	recvAEAD, err := cipher.NewGCM(recvBlock)
	if err != nil {
		return nil, fmt.Errorf("crypto: create receive GCM: %w", err)
	}
	sc := &SessionCipher{
		sendAEAD: sendAEAD,
		recvAEAD: recvAEAD,
	}
	if isServer {
		sc.sendPrefix = PrefixServerToClient
		sc.recvPrefix = PrefixClientToServer
	} else {
		sc.sendPrefix = PrefixClientToServer
		sc.recvPrefix = PrefixServerToClient
	}
	return sc, nil
}

// EncryptPacket authenticates the complete wire header and encrypts plaintext.
func (sc *SessionCipher) EncryptPacket(hdr protocol.Header, plaintext []byte) ([]byte, error) {
	if len(plaintext) > protocol.MaxUDPPayload-Overhead {
		return nil, protocol.ErrPayloadTooLarge
	}
	hdr.Flags |= protocol.FlagENC
	aad := protocol.MarshalHeader(hdr)
	counter := sc.sendCounter.Add(1)
	nonce := make([]byte, NonceSize)
	copy(nonce[:4], sc.sendPrefix[:])
	binary.BigEndian.PutUint64(nonce[4:], counter)
	out := make([]byte, 0, protocol.HeaderSize+NonceSize+len(plaintext)+GCMTagSize)
	out = append(out, aad[:]...)
	out = append(out, nonce...)
	out = sc.sendAEAD.Seal(out, nonce, plaintext, aad[:])
	return out, nil
}

// DecryptPacket authenticates rawHeader20B and decrypts one encrypted payload.
func (sc *SessionCipher) DecryptPacket(hdr protocol.Header, rawHeader20B []byte, encryptedPayload []byte) ([]byte, error) {
	if !hdr.HasFlag(protocol.FlagENC) {
		return nil, ErrMissingEncFlag
	}
	if len(encryptedPayload) < Overhead {
		return nil, ErrInvalidPayloadLength
	}
	if len(rawHeader20B) != protocol.HeaderSize {
		return nil, ErrAuthTagMismatch
	}
	nonce := encryptedPayload[:NonceSize]
	if !bytes.Equal(nonce[:4], sc.recvPrefix[:]) {
		return nil, ErrInvalidDirection
	}
	counter := binary.BigEndian.Uint64(nonce[4:])
	if counter == 0 {
		return nil, ErrReplayDetected
	}

	sc.replayMu.Lock()
	defer sc.replayMu.Unlock()
	if !sc.counterAvailable(counter) {
		return nil, ErrReplayDetected
	}
	plaintext, err := sc.recvAEAD.Open(nil, nonce, encryptedPayload[NonceSize:], rawHeader20B)
	if err != nil {
		return nil, ErrAuthTagMismatch
	}
	sc.markCounter(counter)
	return plaintext, nil
}

// DecryptRawPacket parses and decrypts a complete wire packet.
func (sc *SessionCipher) DecryptRawPacket(rawPacket []byte) (*protocol.Packet, error) {
	if len(rawPacket) < protocol.HeaderSize {
		return nil, ErrInvalidPayloadLength
	}
	hdr, err := protocol.UnmarshalHeader(rawPacket[:protocol.HeaderSize])
	if err != nil {
		return nil, fmt.Errorf("crypto: parse encrypted header: %w", err)
	}
	payload, err := sc.DecryptPacket(hdr, rawPacket[:protocol.HeaderSize], rawPacket[protocol.HeaderSize:])
	if err != nil {
		return nil, err
	}
	return &protocol.Packet{
		Hdr:     hdr,
		Payload: payload,
	}, nil
}

func (sc *SessionCipher) counterAvailable(counter uint64) bool {
	if sc.maxSeenCounter == 0 || counter > sc.maxSeenCounter {
		return true
	}
	distance := sc.maxSeenCounter - counter
	return distance < 64 && sc.replayWindow&(uint64(1)<<distance) == 0
}

func (sc *SessionCipher) markCounter(counter uint64) {
	if sc.maxSeenCounter == 0 || counter > sc.maxSeenCounter {
		delta := counter - sc.maxSeenCounter
		if delta >= 64 {
			sc.replayWindow = 1
		} else {
			sc.replayWindow = (sc.replayWindow << delta) | 1
		}
		sc.maxSeenCounter = counter
		return
	}
	distance := sc.maxSeenCounter - counter
	sc.replayWindow |= uint64(1) << distance
}
