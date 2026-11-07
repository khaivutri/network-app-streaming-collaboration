package transfer

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

const (
	progressBarWidth  = 30
	bytesPerKilobyte  = 1024
	bytesPerMegabyte  = bytesPerKilobyte * bytesPerKilobyte
	minRenderInterval = 100 * time.Millisecond
)

// ProgressBar renders one transfer's live progress without taking ownership
// of terminal output, allowing callers to choose when to print the returned line.
type ProgressBar struct {
	mu          sync.Mutex
	label       string
	totalBytes  int64
	startedAt   time.Time
	lastRender  time.Time
	transferred int64
	finished    bool
}

// NewProgressBar creates a progress renderer for a bounded transfer.
func NewProgressBar(label string, totalBytes int64) *ProgressBar {
	return &ProgressBar{
		label:      label,
		totalBytes: totalBytes,
		startedAt:  time.Now(),
	}
}

// Update records transferred bytes and returns the current terminal line when
// the render throttle interval has elapsed or the transfer reaches completion.
func (bar *ProgressBar) Update(transferred int64) string {
	bar.mu.Lock()
	defer bar.mu.Unlock()
	bar.transferred = clampProgressBytes(transferred, bar.totalBytes)
	now := time.Now()
	if !bar.lastRender.IsZero() && bar.transferred < bar.totalBytes && now.Sub(bar.lastRender) < minRenderInterval {
		return ""
	}
	bar.lastRender = now
	return bar.renderLocked()
}

// Complete marks the transfer complete and returns a final 100 percent line.
func (bar *ProgressBar) Complete() string {
	bar.mu.Lock()
	defer bar.mu.Unlock()
	bar.transferred = bar.totalBytes
	bar.finished = true
	return bar.renderLocked()
}

func (bar *ProgressBar) renderLocked() string {
	percentage := float64(100)
	if bar.totalBytes > 0 {
		percentage = float64(bar.transferred) * 100 / float64(bar.totalBytes)
	}
	filled := int(percentage * progressBarWidth / 100)
	if filled > progressBarWidth {
		filled = progressBarWidth
	}
	arrow := ">"
	if filled == progressBarWidth {
		arrow = "="
	}
	barPart := strings.Repeat("=", filled)
	if filled < progressBarWidth {
		barPart += arrow
		barPart += strings.Repeat(" ", progressBarWidth-filled-1)
	}
	rate := float64(bar.transferred) / maxFloat(time.Since(bar.startedAt).Seconds(), 0.001)
	return fmt.Sprintf("[%s] %s: [%s] %5.1f%% (%s / %s) %s/s", transferStatus(bar.finished), bar.label, barPart, percentage, formatBytes(bar.transferred), formatBytes(bar.totalBytes), formatBytes(int64(rate)))
}

func clampProgressBytes(value, total int64) int64 {
	if value < 0 {
		return 0
	}
	if total > 0 && value > total {
		return total
	}
	return value
}

func formatBytes(value int64) string {
	if value >= bytesPerMegabyte {
		return fmt.Sprintf("%.1f MB", float64(value)/bytesPerMegabyte)
	}
	if value >= bytesPerKilobyte {
		return fmt.Sprintf("%.1f KB", float64(value)/bytesPerKilobyte)
	}
	return fmt.Sprintf("%d B", value)
}

func maxFloat(value, minimum float64) float64 {
	if value < minimum {
		return minimum
	}
	return value
}

func transferStatus(finished bool) string {
	if finished {
		return "Đã tải tệp"
	}
	return "Tải tệp"
}
