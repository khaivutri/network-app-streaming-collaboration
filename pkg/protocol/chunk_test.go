package protocol

import (
	"bytes"
	"errors"
	"testing"
)

func TestFileChunkMarshalUnmarshalRoundtrip(t *testing.T) {
	var id [16]byte
	for i := range id {
		id[i] = byte(i)
	}
	data := bytes.Repeat([]byte{0xa5}, MaxFileChunkDataSize)
	raw, err := MarshalFileChunk(&FileChunkPacket{TransferID: id, ChunkIndex: 9, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	got, err := UnmarshalFileChunk(raw)
	if err != nil || got.ChunkIndex != 9 || got.DataLen != 1200 || !bytes.Equal(got.Data, data) {
		t.Fatalf("chunk mismatch: %#v %v", got, err)
	}
}

func TestFileChunkValidation(t *testing.T) {
	if _, err := UnmarshalFileChunk(make([]byte, 23)); !errors.Is(err, ErrChunkBufferTooShort) {
		t.Fatal(err)
	}
	if _, err := MarshalFileChunk(&FileChunkPacket{Data: make([]byte, 1201)}); !errors.Is(err, ErrChunkDataTooLarge) {
		t.Fatal(err)
	}
	raw := make([]byte, 24)
	raw[23] = 1
	if _, err := UnmarshalFileChunk(raw); !errors.Is(err, ErrChunkLengthMismatch) {
		t.Fatal(err)
	}
}
