// Package crypto contains the protocol's authenticated encryption primitives.
package crypto

import (
	"crypto/ecdh"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

// KeyPair contains an ephemeral X25519 private key and its 32-byte public key.
type KeyPair struct {
	PrivateKey *ecdh.PrivateKey
	PublicKey  [32]byte
}

// GenerateX25519KeyPair creates a fresh ephemeral X25519 key pair.
func GenerateX25519KeyPair() (*KeyPair, error) {
	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("crypto: generate x25519 key: %w", err)
	}
	var publicKey [32]byte
	copy(publicKey[:], privateKey.PublicKey().Bytes())
	return &KeyPair{
		PrivateKey: privateKey,
		PublicKey:  publicKey,
	}, nil
}

// DeriveSessionKeys derives independent client-to-server and server-to-client AES keys.
func DeriveSessionKeys(localPriv *ecdh.PrivateKey, peerPub [32]byte, clientNonce, serverNonce [16]byte, connID uint32, isServer bool) (writeKey [32]byte, readKey [32]byte, err error) {
	if localPriv == nil {
		return writeKey, readKey, fmt.Errorf("crypto: local x25519 private key is nil")
	}
	peerKey, err := ecdh.X25519().NewPublicKey(peerPub[:])
	if err != nil {
		return writeKey, readKey, fmt.Errorf("crypto: parse peer x25519 public key: %w", err)
	}
	sharedSecret, err := localPriv.ECDH(peerKey)
	if err != nil {
		return writeKey, readKey, fmt.Errorf("crypto: compute x25519 shared secret: %w", err)
	}
	salt := make([]byte, 0, len(clientNonce)+len(serverNonce))
	salt = append(salt, clientNonce[:]...)
	salt = append(salt, serverNonce[:]...)
	prk := hkdfExtract(salt, sharedSecret)

	var connBytes [4]byte
	binary.BigEndian.PutUint32(connBytes[:], connID)
	c2sInfo := append([]byte("collab-v1-c2s"), connBytes[:]...)
	s2cInfo := append([]byte("collab-v1-s2c"), connBytes[:]...)
	c2sKey := hkdfExpand(prk, c2sInfo, 32)
	s2cKey := hkdfExpand(prk, s2cInfo, 32)
	if isServer {
		copy(writeKey[:], s2cKey)
		copy(readKey[:], c2sKey)
	} else {
		copy(writeKey[:], c2sKey)
		copy(readKey[:], s2cKey)
	}
	return writeKey, readKey, nil
}

func hkdfExtract(salt, inputKeyMaterial []byte) []byte {
	mac := hmac.New(sha256.New, salt)
	_, _ = mac.Write(inputKeyMaterial)
	return mac.Sum(nil)
}

func hkdfExpand(prk, info []byte, length ...int) []byte {
	outputLength := 32
	if len(length) > 0 {
		outputLength = length[0]
	}
	if outputLength <= 0 || outputLength > 255*sha256.Size {
		return nil
	}
	result := make([]byte, 0, outputLength)
	var previous []byte
	for counter := byte(1); len(result) < outputLength; counter++ {
		mac := hmac.New(sha256.New, prk)
		_, _ = mac.Write(previous)
		_, _ = mac.Write(info)
		_, _ = mac.Write([]byte{counter})
		previous = mac.Sum(nil)
		result = append(result, previous...)
	}
	return result[:outputLength]
}
