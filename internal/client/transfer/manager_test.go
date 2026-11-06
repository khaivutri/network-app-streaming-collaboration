package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

func TestUploaderDownloaderIntegrityAndCleanup(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "sample.bin")
	data := make([]byte, 5000)
	for i := range data {
		data[i] = byte(i % 251)
	}
	if err := os.WriteFile(source, data, 0644); err != nil {
		t.Fatal(err)
	}
	id := "123e4567-e89b-42d3-a456-426614174000"
	uploader, initPayload, err := NewUploader(id, source, 1200)
	if err != nil {
		t.Fatal(err)
	}
	defer uploader.Close()
	if initPayload.TotalChunks != 5 {
		t.Fatalf("chunks=%d", initPayload.TotalChunks)
	}
	downloader, err := NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < initPayload.TotalChunks; index++ {
		chunk, readErr := uploader.ReadChunk(index)
		if readErr != nil {
			t.Fatal(readErr)
		}
		complete, writeErr := downloader.WriteChunk(index, chunk)
		if writeErr != nil {
			t.Fatal(writeErr)
		}
		if index < 4 && complete {
			t.Fatal("completed too early")
		}
	}
	actual, err := downloader.FinalizeAndVerify()
	if err != nil {
		t.Fatal(err)
	}
	wantBytes := sha256.Sum256(data)
	if actual != hex.EncodeToString(wantBytes[:]) {
		t.Fatalf("checksum mismatch")
	}
	if _, err := os.Stat(filepath.Join(root, "downloads", id+"_sample.bin")); err != nil {
		t.Fatal(err)
	}
	_ = protocol.FileCodeReady
}
