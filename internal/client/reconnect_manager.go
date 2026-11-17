package client

import (
	"context"
	"time"

	"github.com/khaivutri/network-app-streaming-collaboration/internal/client/state"
)

// ReconnectConfig controls retry timing and attempt limits.
type ReconnectConfig struct {
	InitialInterval          time.Duration
	MaxInterval              time.Duration
	Multiplier               float64
	MaxAttempts              int
	PreserveSessionOnFailure bool
}

// ReconnectManager performs cancellable exponential-backoff recovery.
type ReconnectManager struct {
	store *state.ClientStore
	cfg   ReconnectConfig
}

// NewReconnectManager creates a manager with production defaults.
func NewReconnectManager(store *state.ClientStore, cfg ReconnectConfig) *ReconnectManager {
	if cfg.InitialInterval <= 0 {
		cfg.InitialInterval = time.Second
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = 16 * time.Second
	}
	if cfg.Multiplier <= 1 {
		cfg.Multiplier = 2
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = 5
	}
	return &ReconnectManager{store: store, cfg: cfg}
}

// Start retries reconnectFn until success, cancellation, or the attempt limit.
func (m *ReconnectManager) Start(ctx context.Context, reconnectFn func() error) error {
	if !m.cfg.PreserveSessionOnFailure {
		m.store.SetConnectionState(state.ConnStateReconnecting)
	}
	interval := m.cfg.InitialInterval
	var lastErr error
	for attempt := 0; m.cfg.PreserveSessionOnFailure || attempt < m.cfg.MaxAttempts; attempt++ {
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			if !m.cfg.PreserveSessionOnFailure {
				m.store.ClearSession()
			}
			return ctx.Err()
		case <-timer.C:
		}
		if err := reconnectFn(); err == nil {
			m.store.SetConnectionState(state.ConnStateConnected)
			return nil
		} else {
			lastErr = err
		}
		interval = time.Duration(float64(interval) * m.cfg.Multiplier)
		if interval > m.cfg.MaxInterval {
			interval = m.cfg.MaxInterval
		}
	}
	m.store.ClearSession()
	if lastErr != nil {
		return lastErr
	}
	return context.Canceled
}
