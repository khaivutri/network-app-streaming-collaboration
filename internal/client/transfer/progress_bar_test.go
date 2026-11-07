package transfer

import (
	"strings"
	"testing"
)

func TestProgressBarRendersTransferMetrics(t *testing.T) {
	bar := NewProgressBar("Doc.pdf", 10*bytesPerMegabyte)
	line := bar.Update(5 * bytesPerMegabyte)
	if !strings.Contains(line, "50.0%") || !strings.Contains(line, "5.0 MB / 10.0 MB") {
		t.Fatalf("progress line %q does not contain expected metrics", line)
	}
	if !strings.Contains(bar.Complete(), "100.0%") {
		t.Fatal("complete progress line did not reach 100 percent")
	}
}
