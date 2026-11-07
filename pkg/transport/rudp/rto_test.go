package rudp

import (
	"testing"
	"time"
)

func TestAdaptiveRTOUpdateAndBackoff(t *testing.T) {
	rto := NewAdaptiveRTO(DefaultRTOConfig())
	rto.Update(50 * time.Millisecond)
	if rto.RTO() < 100*time.Millisecond || rto.RTO() > 3*time.Second {
		t.Fatalf("RTO out of bounds: %v", rto.RTO())
	}
	for i := 0; i < 8; i++ {
		rto.Backoff()
	}
	if rto.RTO() != 3*time.Second {
		t.Fatalf("backoff did not clamp: %v", rto.RTO())
	}
	rto.Update(100 * time.Millisecond)
	if rto.SRTT() <= 0 || rto.RTTVAR() <= 0 {
		t.Fatalf("missing estimator samples")
	}
}
