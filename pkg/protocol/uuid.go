package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

var ErrInvalidUUIDString = errors.New("protocol: invalid UUID string format")

// GenerateUUIDv4 creates an RFC 4122 version-four UUID.
func GenerateUUIDv4() (string, [16]byte, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", value, fmt.Errorf("protocol: generate uuid: %w", err)
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return FormatUUID(value), value, nil
}

// FormatUUID formats a UUID byte array using the canonical representation.
func FormatUUID(value [16]byte) string {
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(value[0:4]), hex.EncodeToString(value[4:6]), hex.EncodeToString(value[6:8]), hex.EncodeToString(value[8:10]), hex.EncodeToString(value[10:16]))
}

// ParseUUID parses a canonical or hyphenless UUID string.
func ParseUUID(input string) ([16]byte, error) {
	var value [16]byte
	clean := strings.ReplaceAll(input, "-", "")
	if len(clean) != 32 {
		return value, ErrInvalidUUIDString
	}
	decoded, err := hex.DecodeString(clean)
	if err != nil {
		return value, fmt.Errorf("protocol: parse uuid: %w", err)
	}
	copy(value[:], decoded)
	return value, nil
}
