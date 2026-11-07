package rudp

import (
	"sync"
	"time"
)

const MaxSegmentRetries = 6

// RTOConfig defines bounds and smoothing factors for retransmission timeout estimation.
type RTOConfig struct {
	MinRTO  time.Duration
	InitRTO time.Duration
	MaxRTO  time.Duration
	Alpha   float64
	Beta    float64
}

// AdaptiveRTO is a thread-safe Jacobson/Karels RTT estimator.
type AdaptiveRTO struct {
	mu         sync.RWMutex
	cfg        RTOConfig
	srtt       time.Duration
	rttvar     time.Duration
	currentRTO time.Duration
	hasSamples bool
}

// DefaultRTOConfig returns the Step 7 RFC 6298-inspired defaults.
func DefaultRTOConfig() RTOConfig {
	return RTOConfig{MinRTO: 100 * time.Millisecond, InitRTO: 300 * time.Millisecond, MaxRTO: 3 * time.Second, Alpha: 0.125, Beta: 0.25}
}

// NewAdaptiveRTO creates an estimator with validated defaults.
func NewAdaptiveRTO(cfg RTOConfig) *AdaptiveRTO {
	defaults := DefaultRTOConfig()
	if cfg.MinRTO <= 0 {
		cfg.MinRTO = defaults.MinRTO
	}
	if cfg.InitRTO <= 0 {
		cfg.InitRTO = defaults.InitRTO
	}
	if cfg.MaxRTO <= 0 {
		cfg.MaxRTO = defaults.MaxRTO
	}
	if cfg.Alpha <= 0 || cfg.Alpha >= 1 {
		cfg.Alpha = defaults.Alpha
	}
	if cfg.Beta <= 0 || cfg.Beta >= 1 {
		cfg.Beta = defaults.Beta
	}
	if cfg.InitRTO < cfg.MinRTO {
		cfg.InitRTO = cfg.MinRTO
	}
	return &AdaptiveRTO{cfg: cfg, currentRTO: cfg.InitRTO}
}

// RTO returns the current clamped retransmission timeout.
func (r *AdaptiveRTO) RTO() time.Duration { r.mu.RLock(); defer r.mu.RUnlock(); return r.currentRTO }

// SRTT returns the smoothed round-trip estimate.
func (r *AdaptiveRTO) SRTT() time.Duration { r.mu.RLock(); defer r.mu.RUnlock(); return r.srtt }

// RTTVAR returns the smoothed RTT variance.
func (r *AdaptiveRTO) RTTVAR() time.Duration { r.mu.RLock(); defer r.mu.RUnlock(); return r.rttvar }

// Update incorporates a non-negative RTT sample.
func (r *AdaptiveRTO) Update(sample time.Duration) {
	if sample <= 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if !r.hasSamples {
		r.srtt, r.rttvar, r.hasSamples = sample, sample/2, true
	} else {
		err := sample - r.srtt
		absErr := err
		if absErr < 0 {
			absErr = -absErr
		}
		r.rttvar = time.Duration(float64(r.rttvar) + r.cfg.Beta*(float64(absErr)-float64(r.rttvar)))
		r.srtt = time.Duration(float64(r.srtt) + r.cfg.Alpha*float64(err))
	}
	r.currentRTO = clampRTO(r.srtt+4*r.rttvar, r.cfg)
}

// Backoff doubles the timeout while respecting the configured maximum.
func (r *AdaptiveRTO) Backoff() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.currentRTO = clampRTO(r.currentRTO*2, r.cfg)
	return r.currentRTO
}

func clampRTO(value time.Duration, cfg RTOConfig) time.Duration {
	if value < cfg.MinRTO {
		return cfg.MinRTO
	}
	if value > cfg.MaxRTO {
		return cfg.MaxRTO
	}
	return value
}
