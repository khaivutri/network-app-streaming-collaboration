package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

// Uploader hashes a source file and provides deterministic chunks by index.
type Uploader struct {
	mu          sync.Mutex
	transferID  string
	filePath    string
	fileName    string
	fileSize    int64
	chunkSize   int
	totalChunks uint32
	sha256Hex   string
	sourceFile  *os.File
}

// NewUploader opens a source file and prepares its transfer metadata.
func NewUploader(transferID, filePath string, chunkSize int) (*Uploader, *protocol.FileInitPayload, error) {
	if chunkSize <= 0 || chunkSize > protocol.MaxFileChunkDataSize {
		chunkSize = protocol.MaxFileChunkDataSize
	}
	file, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("transfer: open source: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("transfer: stat source: %w; close source: %v", err, closeErr)
		}
		return nil, nil, fmt.Errorf("transfer: stat source: %w", err)
	}
	hash := sha256.New()
	if _, err = io.Copy(hash, file); err != nil {
		closeErr := file.Close()
		if closeErr != nil {
			return nil, nil, fmt.Errorf("transfer: hash source: %w; close source: %v", err, closeErr)
		}
		return nil, nil, fmt.Errorf("transfer: hash source: %w", err)
	}
	if err = file.Close(); err != nil {
		return nil, nil, fmt.Errorf("transfer: close hash source: %w", err)
	}
	size := stat.Size()
	total := uint32((size + int64(chunkSize) - 1) / int64(chunkSize))
	if total == 0 {
		total = 1
	}
	source, err := os.Open(filePath)
	if err != nil {
		return nil, nil, fmt.Errorf("transfer: reopen source: %w", err)
	}
	checksum := hex.EncodeToString(hash.Sum(nil))
	name := filepath.Base(filePath)
	return &Uploader{
		transferID: transferID, 
		filePath: filePath, 
		fileName: name, 
		fileSize: size, 
		chunkSize: chunkSize, 
		totalChunks: total, 
		sha256Hex: checksum, 
		sourceFile: source,
		}, 
		&protocol.FileInitPayload{
			TransferID: transferID, 
			FileName: name, MimeType: "application/octet-stream", 
			FileSize: size, ChunkSize: chunkSize, 
			TotalChunks: total, 
			SHA256: checksum, 
			CreatedAt: time.Now().UnixMilli(),
		}, nil
}

// ReadChunk reads one bounded chunk at its deterministic file offset.
func (u *Uploader) ReadChunk(index uint32) ([]byte, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.sourceFile == nil {
		return nil, fmt.Errorf("transfer: source is closed")
	}
	data := make([]byte, u.chunkSize)
	n, err := u.sourceFile.ReadAt(data, int64(index)*int64(u.chunkSize))
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("transfer: read chunk %d: %w", index, err)
	}
	return data[:n], nil
}

// Close releases the source file.
func (u *Uploader) Close() error {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.sourceFile == nil {
		return nil
	}
	err := u.sourceFile.Close()
	u.sourceFile = nil
	if err != nil {
		return fmt.Errorf("transfer: close source: %w", err)
	}
	return nil
}
