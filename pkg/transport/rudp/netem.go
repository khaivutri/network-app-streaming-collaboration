package rudp

import (
	"errors"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"time"
)

var ErrInvalidNetEmConfig = errors.New("rudp: invalid network emulation config")

// NetEmConfig describes packet impairment options for local network demos.
type NetEmConfig struct {
	Enabled       bool
	DropRate      float64
	ReorderRate   float64
	DuplicateRate float64
	CorruptRate   float64
	BaseLatency   time.Duration
	MaxJitter     time.Duration
	Seed          int64
}

// NetEmStats counts outbound datagrams affected by the emulator.
type NetEmStats struct {
	TotalSent      uint64
	DroppedPackets uint64
	ReorderedCount uint64
	Duplicated     uint64
	Corrupted      uint64
}

// Validate rejects impairment rates outside [0,1] and negative delays.
func (c NetEmConfig) Validate() error {
	rates := []float64{c.DropRate, c.ReorderRate, c.DuplicateRate, c.CorruptRate}
	for _, rate := range rates {
		if rate < 0 || rate > 1 {
			return fmt.Errorf("%w: rates must be between 0 and 1", ErrInvalidNetEmConfig)
		}
	}
	if c.BaseLatency < 0 || c.MaxJitter < 0 {
		return fmt.Errorf("%w: latency cannot be negative", ErrInvalidNetEmConfig)
	}
	return nil
}

// PacketConn is the UDP socket surface needed by the reliable transport.
type PacketConn interface {
	ReadFromUDP([]byte) (int, *net.UDPAddr, error)
	WriteToUDP([]byte, *net.UDPAddr) (int, error)
	Close() error
	LocalAddr() net.Addr
	SetReadDeadline(time.Time) error
	SetWriteDeadline(time.Time) error
	SetReadBuffer(int) error
	SetWriteBuffer(int) error
}

// NetEmConn wraps a UDP socket and applies configured outbound impairments.
type NetEmConn struct {
	inner PacketConn
	mu    sync.Mutex
	rng   *rand.Rand
	cfg   NetEmConfig
	stats NetEmStats
}

// NewNetEmConn validates configuration and wraps the supplied socket.
func NewNetEmConn(inner PacketConn, config NetEmConfig) (*NetEmConn, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	if inner == nil {
		return nil, fmt.Errorf("%w: socket is required", ErrInvalidNetEmConfig)
	}
	return &NetEmConn{
		inner: inner,
		cfg:   config,
		rng:   rand.New(rand.NewSource(config.Seed)),
	}, nil
}

// ReadFromUDP forwards one datagram from the underlying socket.
func (c *NetEmConn) ReadFromUDP(buffer []byte) (int, *net.UDPAddr, error) {
	return c.inner.ReadFromUDP(buffer)
}

// WriteToUDP forwards one datagram to the underlying socket.
func (c *NetEmConn) WriteToUDP(buffer []byte, addr *net.UDPAddr) (int, error) {
	c.mu.Lock()
	cfg := c.cfg
	if !cfg.Enabled && cfg.DropRate == 0 && cfg.BaseLatency == 0 && cfg.MaxJitter == 0 && cfg.ReorderRate == 0 && cfg.DuplicateRate == 0 && cfg.CorruptRate == 0 {
		c.mu.Unlock()
		return c.inner.WriteToUDP(buffer, addr)
	}
	c.stats.TotalSent++
	if c.rng.Float64() < cfg.DropRate {
		c.stats.DroppedPackets++
		c.mu.Unlock()
		return len(buffer), nil
	}
	corrupt := c.rng.Float64() < cfg.CorruptRate
	duplicate := c.rng.Float64() < cfg.DuplicateRate
	reorder := c.rng.Float64() < cfg.ReorderRate
	delay := cfg.BaseLatency
	if cfg.MaxJitter > 0 {
		delay += time.Duration(c.rng.Int63n(int64(cfg.MaxJitter) + 1))
	}
	if reorder {
		delay += cfg.BaseLatency + cfg.MaxJitter
		c.stats.ReorderedCount++
	}
	if duplicate {
		c.stats.Duplicated++
	}
	if corrupt {
		c.stats.Corrupted++
	}
	c.mu.Unlock()
	if delay > 0 {
		time.Sleep(delay)
	}
	packet := append([]byte(nil), buffer...)
	if corrupt && len(packet) > 0 {
		packet[len(packet)/2] ^= 1
	}
	_, err := c.inner.WriteToUDP(packet, addr)
	if err != nil || !duplicate {
		if err != nil {
			return 0, err
		}
		return len(buffer), nil
	}
	if _, err = c.inner.WriteToUDP(packet, addr); err != nil {
		return 0, err
	}
	// A short gap keeps the two datagrams observable on platforms that coalesce
	// immediate localhost sends while preserving duplicate packet semantics.
	time.Sleep(time.Millisecond)
	return len(buffer), nil
}

// UpdateConfig atomically replaces impairment settings after validation.
func (c *NetEmConn) UpdateConfig(config NetEmConfig) error {
	if err := config.Validate(); err != nil {
		return err
	}
	c.mu.Lock()
	c.cfg = config
	c.mu.Unlock()
	return nil
}

// Stats returns a consistent snapshot of outbound impairment counters.
func (c *NetEmConn) Stats() NetEmStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stats
}

// ResetStats clears outbound impairment counters.
func (c *NetEmConn) ResetStats() {
	c.mu.Lock()
	c.stats = NetEmStats{}
	c.mu.Unlock()
}

// Close closes the owned socket.
func (c *NetEmConn) Close() error { return c.inner.Close() }

// LocalAddr returns the underlying socket's local address.
func (c *NetEmConn) LocalAddr() net.Addr { return c.inner.LocalAddr() }

// SetReadDeadline sets the underlying socket's read deadline.
func (c *NetEmConn) SetReadDeadline(deadline time.Time) error {
	return c.inner.SetReadDeadline(deadline)
}

// SetWriteDeadline sets the underlying socket's write deadline.
func (c *NetEmConn) SetWriteDeadline(deadline time.Time) error {
	return c.inner.SetWriteDeadline(deadline)
}

// SetReadBuffer forwards the requested socket receive buffer size.
func (c *NetEmConn) SetReadBuffer(size int) error { return c.inner.SetReadBuffer(size) }

// SetWriteBuffer forwards the requested socket send buffer size.
func (c *NetEmConn) SetWriteBuffer(size int) error { return c.inner.SetWriteBuffer(size) }
