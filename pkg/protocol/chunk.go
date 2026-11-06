package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	FileChunkSubHeaderSize = 24
	MaxFileChunkDataSize   = 1200
)

var (
	ErrChunkBufferTooShort = errors.New("protocol: chunk buffer shorter than 24-byte sub-header")
	ErrChunkLengthMismatch = errors.New("protocol: chunk payload length does not match data length")
	ErrChunkDataTooLarge   = errors.New("protocol: chunk data exceeds max 1200-byte limit")
	ErrNilChunkPacket      = errors.New("protocol: file chunk packet cannot be nil")
)

// FileChunkPacket is the binary FILE_CHUNK payload.
type FileChunkPacket struct {
	TransferID [16]byte
	ChunkIndex uint32
	DataLen    uint32
	Data       []byte
}

// MarshalFileChunk serializes a file chunk without allocating server-side file buffers.
func MarshalFileChunk(chunk *FileChunkPacket) ([]byte, error) {
	if chunk == nil {
		return nil, ErrNilChunkPacket
	}
	if len(chunk.Data) > MaxFileChunkDataSize {
		return nil, ErrChunkDataTooLarge
	}
	raw := make([]byte, FileChunkSubHeaderSize+len(chunk.Data))
	copy(raw[:16], chunk.TransferID[:])
	binary.BigEndian.PutUint32(raw[16:20], chunk.ChunkIndex)
	binary.BigEndian.PutUint32(raw[20:24], uint32(len(chunk.Data)))
	copy(raw[24:], chunk.Data)
	return raw, nil
}

// UnmarshalFileChunk validates and decodes a binary file chunk.
func UnmarshalFileChunk(raw []byte) (*FileChunkPacket, error) {
	if len(raw) < FileChunkSubHeaderSize {
		return nil, ErrChunkBufferTooShort
	}
	dataLen := binary.BigEndian.Uint32(raw[20:24])
	if dataLen > MaxFileChunkDataSize {
		return nil, ErrChunkDataTooLarge
	}
	actual := len(raw) - FileChunkSubHeaderSize
	if int(dataLen) != actual {
		return nil, fmt.Errorf("%w: header=%d actual=%d", ErrChunkLengthMismatch, dataLen, actual)
	}
	var id [16]byte
	copy(id[:], raw[:16])
	data := append([]byte(nil), raw[24:]...)
	return &FileChunkPacket{TransferID: id, ChunkIndex: binary.BigEndian.Uint32(raw[16:20]), DataLen: dataLen, Data: data}, nil
}
