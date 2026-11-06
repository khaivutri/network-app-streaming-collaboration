package rudp

import (
	"sync"
	"time"
)

// CongestionController provides a small thread-safe congestion window controller.
type CongestionController struct {
	mu           sync.RWMutex
	cwnd         int
	ssthresh     int
	rttMin       time.Duration
	lastLossTime time.Time
}

// NewCongestionController creates a conservative pacing controller.
func NewCongestionController() *CongestionController {
	return &CongestionController{cwnd: 10, ssthresh: 64, rttMin: 5 * time.Millisecond}
}

// OnPacketAcked increases the congestion window after successful delivery.
func (c *CongestionController) OnPacketAcked() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cwnd < c.ssthresh {
		c.cwnd++
	}
}

// OnPacketLoss reduces the congestion window after loss.
func (c *CongestionController) OnPacketLoss() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ssthresh = c.cwnd / 2
	if c.ssthresh < 4 {
		c.ssthresh = 4
	}
	c.cwnd = c.ssthresh
	c.lastLossTime = time.Now()
}
