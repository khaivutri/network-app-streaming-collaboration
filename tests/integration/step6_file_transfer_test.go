package integration

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/transfer"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

func TestStep6_FileTransfer_EndToEnd_Integrity_BR08(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.bin")
	content := bytes.Repeat([]byte("reliable-file-transfer\x00"), 10000)
	if err := os.WriteFile(source, content, 0644); err != nil {
		t.Fatal(err)
	}
	uploader, initPayload, err := transfer.NewUploader("123e4567-e89b-42d3-a456-426614174000", source, protocol.MaxFileChunkDataSize)
	if err != nil {
		t.Fatal(err)
	}
	defer uploader.Close()
	downloader, err := transfer.NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	for index := uint32(0); index < initPayload.TotalChunks; index++ {
		data, readErr := uploader.ReadChunk(index)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if _, writeErr := downloader.WriteChunk(index, data); writeErr != nil {
			t.Fatal(writeErr)
		}
	}
	if _, err := downloader.FinalizeAndVerify(); err != nil {
		t.Fatal(err)
	}
}

func TestStep6_FileTransfer_ZeroHeadOfLineBlocking_ChatDuringTransfer(t *testing.T) {
	pool := rudp.NewStreamPool()
	fileStream, err := pool.Acquire()
	if err != nil || fileStream < protocol.StreamFileMin {
		t.Fatalf("file stream allocation failed: %v", err)
	}
	chat := rudp.NewStream(protocol.StreamBroadcastChat, protocol.DefaultWindowSize)
	if chat.NextSeqNum() != 1 {
		t.Fatal("chat stream sequence was coupled to file stream")
	}
}

func TestStep6_FileTransfer_SecuritySanitization_PathTraversal(t *testing.T) {
	if got := transfer.SanitizeFileName("../../../etc/passwd"); got != "passwd" {
		t.Fatalf("got %q", got)
	}
	if got := transfer.SanitizeFileName("../../evil.exe"); got != "evil.exe" {
		t.Fatalf("got %q", got)
	}
}

func TestStep6_FileTransfer_Cancellation_Cleanup_UC05(t *testing.T) {
	root := t.TempDir()
	initPayload := &protocol.FileInitPayload{TransferID: "123e4567-e89b-42d3-a456-426614174000", FileName: "partial.bin", FileSize: 1200, ChunkSize: 1200, TotalChunks: 1, SHA256: "bad"}
	d, err := transfer.NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Cleanup() })
	if _, err := d.WriteChunk(0, []byte("partial")); err != nil {
		t.Fatal(err)
	}
	if err := d.Cleanup(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, "staging", initPayload.TransferID+".part")); !os.IsNotExist(err) {
		t.Fatalf("partial file remains: %v", err)
	}
}

func TestStep6_FileTransfer_CorruptionDetection_ChecksumMismatch(t *testing.T) {
	root := t.TempDir()
	initPayload := &protocol.FileInitPayload{TransferID: "123e4567-e89b-42d3-a456-426614174000", FileName: "corrupt.bin", FileSize: 4, ChunkSize: 4, TotalChunks: 1, SHA256: "0000000000000000000000000000000000000000000000000000000000000000"}
	d, err := transfer.NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Cleanup() })
	if _, err := d.WriteChunk(0, []byte("bad!")); err != nil {
		t.Fatal(err)
	}
	if _, err := d.FinalizeAndVerify(); err == nil {
		t.Fatal("expected checksum failure")
	}
	if _, err := os.Stat(filepath.Join(root, "staging", initPayload.TransferID+".part")); !os.IsNotExist(err) {
		t.Fatalf("corrupt partial remains: %v", err)
	}
}

func TestStep6_FileTransfer_ProgressReporting_BR09(t *testing.T) {
	root := t.TempDir()
	content := bytes.Repeat([]byte("x"), 2400)
	source := filepath.Join(root, "progress.bin")
	if err := os.WriteFile(source, content, 0644); err != nil {
		t.Fatal(err)
	}
	uploader, initPayload, err := transfer.NewUploader("123e4567-e89b-42d3-a456-426614174000", source, 1200)
	if err != nil {
		t.Fatal(err)
	}
	defer uploader.Close()
	d, err := transfer.NewDownloader(filepath.Join(root, "staging"), filepath.Join(root, "downloads"), initPayload)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Cleanup() })
	previous := 0.0
	for index := uint32(0); index < initPayload.TotalChunks; index++ {
		data, _ := uploader.ReadChunk(index)
		_, _ = d.WriteChunk(index, data)
		current := d.Progress()
		if current < previous {
			t.Fatalf("progress regressed")
		}
		previous = current
	}
	if previous != 100 {
		t.Fatalf("progress=%v", previous)
	}
}
