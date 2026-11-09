package transfer

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/protocol"
)

var ErrChecksumMismatch = errors.New("transfer: SHA-256 checksum mismatch")

// Downloader writes chunks to a staging file and atomically finalizes verified data.
type Downloader struct {
	mu           sync.Mutex
	transferID   string
	fileSize     int64
	chunkSize    int
	totalChunks  uint32
	expectedSHA  string
	stagingPath  string
	downloadPath string
	stagingFile  *os.File
	receivedMask map[uint32]bool
	bytesWritten int64
	finished     bool
}

// NewDownloader creates a bounded staging file for a transfer.
func NewDownloader(stagingDir, downloadDir string, init *protocol.FileInitPayload) (*Downloader, error) {
	if init == nil || init.TransferID == "" {
		return nil, fmt.Errorf("transfer: file init is required")
	}
	if err := os.MkdirAll(stagingDir, 0755); err != nil {
		return nil, fmt.Errorf("transfer: create staging: %w", err)
	}
	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		return nil, fmt.Errorf("transfer: create downloads: %w", err)
	}
	part := filepath.Join(stagingDir, init.TransferID+".part")
	file, err := os.OpenFile(part, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return nil, fmt.Errorf("transfer: open staging: %w", err)
	}
	return &Downloader{transferID: init.TransferID, fileSize: init.FileSize, chunkSize: init.ChunkSize, totalChunks: init.TotalChunks, expectedSHA: init.SHA256, stagingPath: part, downloadPath: filepath.Join(downloadDir, init.TransferID+"_"+SanitizeFileName(init.FileName)), stagingFile: file, receivedMask: make(map[uint32]bool)}, nil
}

// WriteChunk writes one chunk at its file offset and reports completion readiness.
func (d *Downloader) WriteChunk(index uint32, data []byte) (bool, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return false, nil
	}
	if index >= d.totalChunks || len(data) > d.chunkSize || d.stagingFile == nil {
		return false, fmt.Errorf("transfer: invalid chunk index or length")
	}
	if _, err := d.stagingFile.WriteAt(data, int64(index)*int64(d.chunkSize)); err != nil {
		return false, fmt.Errorf("transfer: write chunk: %w", err)
	}
	if !d.receivedMask[index] {
		d.receivedMask[index] = true
		d.bytesWritten += int64(len(data))
	}
	return uint32(len(d.receivedMask)) == d.totalChunks, nil
}

// FinalizeAndVerify fsyncs, hashes, removes corrupt data, or atomically renames it.
func (d *Downloader) FinalizeAndVerify() (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.finished {
		return "", fmt.Errorf("transfer: downloader already finalized")
	}
	d.finished = true
	if d.stagingFile == nil {
		return "", fmt.Errorf("transfer: staging file is closed")
	}
	if err := d.stagingFile.Sync(); err != nil {
		return "", fmt.Errorf("transfer: sync staging: %w", err)
	}
	if err := d.stagingFile.Close(); err != nil {
		return "", fmt.Errorf("transfer: close staging: %w", err)
	}
	d.stagingFile = nil
	file, err := os.Open(d.stagingPath)
	if err != nil {
		return "", fmt.Errorf("transfer: reopen staging: %w", err)
	}
	hash := sha256.New()
	_, copyErr := io.Copy(hash, file)
	closeErr := file.Close()
	if copyErr != nil {
		return "", fmt.Errorf("transfer: hash staging: %w", copyErr)
	}
	if closeErr != nil {
		return "", fmt.Errorf("transfer: close hashed staging: %w", closeErr)
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if actual != d.expectedSHA {
		if removeErr := os.Remove(d.stagingPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return actual, fmt.Errorf("%w: remove corrupt staging: %v", ErrChecksumMismatch, removeErr)
		}
		return actual, fmt.Errorf("%w: expected %s got %s", ErrChecksumMismatch, d.expectedSHA, actual)
	}
	if err := os.Rename(d.stagingPath, d.downloadPath); err != nil {
		// Windows can reject a cross-directory rename while its directory handle is
		// being scanned. Preserve the verified bytes with an explicit sync/copy
		// fallback, then remove the staging path only after the destination closes.
		if copyErr := copyVerifiedFile(d.stagingPath, d.downloadPath); copyErr != nil {
			return actual, fmt.Errorf("transfer: finalize rename: %w; copy fallback: %v", err, copyErr)
		}
		for attempt := 0; attempt < 5; attempt++ {
			if removeErr := os.Remove(d.stagingPath); removeErr == nil || errors.Is(removeErr, os.ErrNotExist) {
				break
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	return actual, nil
}

func copyVerifiedFile(sourcePath, destinationPath string) error {
	source, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("open source: %w", err)
	}
	destination, err := os.OpenFile(destinationPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		closeErr := source.Close()
		if closeErr != nil {
			return fmt.Errorf("open destination: %w; close source: %v", err, closeErr)
		}
		return fmt.Errorf("open destination: %w", err)
	}
	_, copyErr := io.Copy(destination, source)
	syncErr := destination.Sync()
	destinationCloseErr := destination.Close()
	sourceCloseErr := source.Close()
	if copyErr != nil {
		return fmt.Errorf("copy bytes: %w", copyErr)
	}
	if syncErr != nil {
		return fmt.Errorf("sync destination: %w", syncErr)
	}
	if destinationCloseErr != nil {
		return fmt.Errorf("close destination: %w", destinationCloseErr)
	}
	if sourceCloseErr != nil {
		return fmt.Errorf("close source: %w", sourceCloseErr)
	}
	return nil
}

// Cleanup closes and removes a partial transfer.
func (d *Downloader) Cleanup() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.finished = true
	if d.stagingFile != nil {
		if err := d.stagingFile.Close(); err != nil {
			return fmt.Errorf("transfer: close partial staging: %w", err)
		}
		d.stagingFile = nil
	}
	if err := os.Remove(d.stagingPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("transfer: remove partial staging: %w", err)
	}
	return nil
}

// Progress returns the received percentage.
func (d *Downloader) Progress() float64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fileSize <= 0 {
		return 0
	}
	return minFloat(100, float64(d.bytesWritten)*100/float64(d.fileSize))
}

// BytesWritten returns the number of unique payload bytes written to staging.
func (d *Downloader) BytesWritten() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.bytesWritten
}

// DownloadPath returns the verified destination path owned by this downloader.
func (d *Downloader) DownloadPath() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.downloadPath
}

func minFloat(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}
