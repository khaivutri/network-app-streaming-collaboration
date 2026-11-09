package transfer

import "sync"

// TransferProgress is a thread-safe-store-friendly progress snapshot.
type TransferProgress struct {
	CreatedAt        int64   `json:"created_at"`
	UpdatedAt        int64   `json:"updated_at"`
	TransferID       string  `json:"transfer_id"`
	Direction        string  `json:"direction"`
	Peer             string  `json:"peer"`
	FileName         string  `json:"file_name"`
	FileSize         int64   `json:"file_size"`
	TransferredBytes int64   `json:"transferred_bytes"`
	Percentage       float64 `json:"percentage"`
	State            string  `json:"state"`
	ErrorMessage     string  `json:"error_message,omitempty"`
	LocalPath        string  `json:"-"`
}

// Manager owns active uploaders, downloaders, and progress snapshots.
type Manager struct {
	mu          sync.RWMutex
	stagingDir  string
	downloadDir string
	uploaders   map[string]*Uploader
	downloaders map[string]*Downloader
	progress    map[string]*TransferProgress
}

// NewManager creates a local transfer manager.
func NewManager(stagingDir, downloadDir string) *Manager {
	return &Manager{
		stagingDir:  stagingDir,
		downloadDir: downloadDir,
		uploaders:   make(map[string]*Uploader),
		downloaders: make(map[string]*Downloader),
		progress:    make(map[string]*TransferProgress),
	}
}

// SetProgress stores a detached progress snapshot.
func (m *Manager) SetProgress(progress *TransferProgress) {
	if progress == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	copy := *progress
	m.progress[progress.TransferID] = &copy
}

// GetProgress returns a detached progress snapshot.
func (m *Manager) GetProgress(id string) (*TransferProgress, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.progress[id]
	if !ok || value == nil {
		return nil, false
	}
	copy := *value
	return &copy, true
}

// RegisterUploader tracks an outgoing transfer.
func (m *Manager) RegisterUploader(id string, uploader *Uploader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.uploaders[id] = uploader
}

// RegisterDownloader tracks an incoming transfer.
func (m *Manager) RegisterDownloader(id string, downloader *Downloader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.downloaders[id] = downloader
}

// Uploader returns an outgoing transfer by ID.
func (m *Manager) Uploader(id string) (*Uploader, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.uploaders[id]
	return value, ok
}

// Downloader returns an incoming transfer by ID.
func (m *Manager) Downloader(id string) (*Downloader, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	value, ok := m.downloaders[id]
	return value, ok
}

// Remove deletes transfer bookkeeping and returns its endpoints.
func (m *Manager) Remove(id string) (*Uploader, *Downloader) {
	m.mu.Lock()
	defer m.mu.Unlock()
	uploader := m.uploaders[id]
	downloader := m.downloaders[id]
	delete(m.uploaders, id)
	delete(m.downloaders, id)
	return uploader, downloader
}
